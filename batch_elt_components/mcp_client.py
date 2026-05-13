import asyncio
from pathlib import Path

from fastmcp import Client

my_mcp_server_path = Path(__file__).parent / "my_mcp.py"


async def main() -> None:
    client = Client(str(my_mcp_server_path))

    async with client:
        # Entering the context connects AND initializes the session.
        print(f"Connected: {client.is_connected()}")
        print(f"Initialized: {client.initialize_result is not None}")
        print(f"Server: {client.initialize_result.serverInfo.name}")

        # List the tools the server provides.
        tools = await client.list_tools()
        for tool in tools:
            print(f"Tool: {tool.name}")

        print("\n--- list_tables ---")
        result = await client.call_tool("list_tables", arguments={})
        for content in result.content:
            print(content.text)

        print("\n--- query_duckdb ---")
        result = await client.call_tool(
            "query_duckdb",
            arguments={"sql": "SELECT * from ducklake.main.agregations limit 10"},
        )
        for content in result.content:
            print(content.text)


if __name__ == "__main__":
    asyncio.run(main())
