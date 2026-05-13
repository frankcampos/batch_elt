import asyncio
from pathlib import Path
from typing import Literal

import dagster as dg
from dagster_openai import OpenAIResource
from fastmcp import Client
from pydantic import BaseModel


def _default_server_path() -> Path:
    # components/mcp_query_component.py -> components -> batch_elt_components -> src -> <project root>
    return Path(__file__).resolve().parents[3] / "my_mcp.py"


class ColumnDoc(BaseModel):
    name: str
    data_type: str
    description: str
    is_nullable: bool
    is_pii: bool
    used_for_aggregations: bool
    example_values: list[str]
    gotchas: str


class TableDoc(BaseModel):
    table_name: str
    table_type: Literal["fact", "dimension", "staging", "aggregate", "reference"]
    description: str
    primary_key: str
    grain: str
    source_system: str
    columns: list[ColumnDoc]


def _to_markdown(doc: TableDoc) -> str:
    lines = [
        f"# Table: {doc.table_name}",
        f"**Type:** {doc.table_type}",
        f"**Description:** {doc.description}",
        f"**Primary Key:** {doc.primary_key}",
        f"**Grain:** {doc.grain}",
        f"**Source System:** {doc.source_system}",
        "",
        "## Columns",
    ]
    for col in doc.columns:
        lines += [
            f"### {col.name}",
            f"- **Type:** {col.data_type}",
            f"- **Description:** {col.description}",
            f"- **Nullable:** {col.is_nullable}",
            f"- **PII:** {col.is_pii}",
            f"- **Used for aggregations:** {col.used_for_aggregations}",
            f"- **Example values:** {', '.join(col.example_values) or 'N/A'}",
            f"- **Gotchas:** {col.gotchas}",
            "",
        ]
    return "\n".join(lines)


class MCPQueryComponent(dg.Component, dg.Model, dg.Resolvable):
    """Generates an AI-written data dictionary for a table.

    The schema is fetched through the duckdb-inspector MCP server (so the LLM never
    touches the database directly — it only sees the query output), then OpenAI turns
    that schema into a structured TableDoc that gets written out as Markdown.
    """

    # YAML fields.
    spec: dg.ResolvedAssetSpec
    sql: str                          # query the MCP server runs to fetch the table's schema
    table: str                        # table name (used in the prompt + output filename)
    ai_model: str
    output_dir: str
    mcp_server_path: str | None = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Capture config as locals so the asset closes over plain values.
        sql = self.sql
        table = self.table
        ai_model = self.ai_model
        output_dir = self.output_dir
        server_path = Path(self.mcp_server_path) if self.mcp_server_path else _default_server_path()

        async def _fetch_schema_via_mcp() -> str:
            client = Client(str(server_path))
            async with client:
                result = await client.call_tool("query_duckdb", arguments={"sql": sql})
                return "\n".join(c.text for c in result.content)

        @dg.multi_asset(specs=[self.spec])
        def _mcp_doc_asset(
            context: dg.AssetExecutionContext, openai: OpenAIResource
        ) -> dg.MaterializeResult:
            # 1. MCP fetches the schema — the only thing the LLM will ever see.
            schema_text = asyncio.run(_fetch_schema_via_mcp())
            context.log.info(f"Schema fetched via MCP:\n{schema_text}")

            # 2. The LLM turns the raw schema into a structured TableDoc.
            prompt = (
                f"Create documentation for the table `{table}` "
                f"with the following columns:\n{schema_text}"
            )
            with openai.get_client(context) as client:
                response = client.beta.chat.completions.parse(
                    model=ai_model,
                    messages=[{"role": "user", "content": prompt}],
                    response_format=TableDoc,
                )
            doc = response.choices[0].message.parsed

            # 3. Write the Markdown data dictionary.
            output_path = Path(output_dir) / f"{table}_mcp_version.md"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(_to_markdown(doc))

            return dg.MaterializeResult(
                metadata={
                    "schema_query": dg.MetadataValue.md(f"```sql\n{sql.strip()}\n```"),
                    "schema": dg.MetadataValue.text(schema_text),
                    "doc_path": dg.MetadataValue.path(str(output_path)),
                    "table_type": dg.MetadataValue.text(doc.table_type),
                }
            )

        return dg.Definitions(assets=[_mcp_doc_asset])
