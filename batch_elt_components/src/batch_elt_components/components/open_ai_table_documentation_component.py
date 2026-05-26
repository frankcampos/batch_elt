import dagster as dg
from pydantic import BaseModel
from dagster_openai import OpenAIResource
from dagster_duckdb import DuckDBResource
from typing import Literal
from pathlib import Path


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


class OpenAITableDocumentationComponent(dg.Component, dg.Model, dg.Resolvable):
    """This component uses the OpenAI API to create documentation for tables and columns."""
    ai_model: str
    table: str
    output_dir: str
    database: str | None = None
    database_path: str | None = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        @dg.asset(name=f"{self.table}_documentation", group_name="documentation")
        def _asset(context: dg.AssetExecutionContext, openai: OpenAIResource, database: DuckDBResource) -> str:
            with database.get_connection() as conn:
                if self.database and self.database_path:
                    # ducklake tables
                    conn.execute(f"ATTACH IF NOT EXISTS '{self.database_path}' AS {self.database} (READ_ONLY)")
                    schema = conn.execute(f"""
                        SELECT c.column_name, c.column_type
                        FROM {self.database}.main.ducklake_column c
                        JOIN {self.database}.main.ducklake_table t ON c.table_id = t.table_id
                        WHERE t.table_name = '{self.table}'
                          AND t.end_snapshot IS NULL
                          AND c.end_snapshot IS NULL
                        ORDER BY c.column_order
                    """).fetchall()
                else:
                    # duckdb tables
                    schema = conn.execute(f"DESCRIBE {self.table}").fetchall()

            schema_text = "\n".join(f"{col[0]} ({col[1]})" for col in schema)
            prompt = f"Create documentation for the table `{self.table}` with the following columns:\n{schema_text}"

            with openai.get_client(context) as client:
                response = client.beta.chat.completions.parse(
                    model=self.ai_model,
                    messages=[{"role": "user", "content": prompt}],
                    response_format=TableDoc,
                )

            doc = response.choices[0].message.parsed
            output_path = Path(self.output_dir) / f"{self.table}.md"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(_to_markdown(doc))

            return str(output_path)

        return dg.Definitions(assets=[_asset])
