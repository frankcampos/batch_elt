from fastmcp import FastMCP
import duckdb
from pathlib import Path

mcp = FastMCP('Database inspector')

DB_PATH = Path(__file__).parent.parent / 'duckdb' / 'data.duckdb'
DUCKLAKE_PATH = Path(__file__).parent.parent / 'duckdb' / 'my_ducklake.ddb'


def _connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    conn.execute(f"ATTACH IF NOT EXISTS 'ducklake:{DUCKLAKE_PATH}' AS ducklake (READ_ONLY)")
    return conn


@mcp.tool()
def query_duckdb(sql: str) -> str:
    """
    Query the DuckDB database.

    Args:
        sql: The SQL query to execute.

    Returns:
        A string with the query output.
    """
    with _connect() as conn:
        res = conn.execute(sql).fetchall()
    return "\n".join(str(row) for row in res)


@mcp.tool()
def list_tables() -> str:
    """
    List all available tables and which database they belong to.

    Returns:
        A string listing each table and its source database.
    """
    with _connect() as conn:
        rows = conn.execute("""
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_catalog, table_schema, table_name
        """).fetchall()
    return "\n".join(f"{cat}.{sch}.{tbl}" for cat, sch, tbl in rows)


if __name__ == "__main__":
    mcp.run()