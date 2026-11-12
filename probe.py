import argparse
import asyncio
import json
import os

from fastmcp import Client
from fastmcp.exceptions import ToolError
from fastmcp_tasks import call_tool_task


async def probe_catalog(client):
    tools = {tool.name: tool for tool in await client.list_tools()}
    assert tools["stock"].input_schema["properties"]["quantity"]["type"] == "integer"
    assert (await client.call_tool("stock", {"item": "fern", "quantity": 2})).data == 5
    try:
        await client.call_tool("stock", {"item": "fern", "quantity": 0})
    except ToolError as error:
        assert "quantity must be positive" in str(error)
    else:
        raise AssertionError("stock accepted an invalid quantity")

    resources = await client.read_resource("catalog://items")
    assert json.loads(resources[0].text) == {"fern": 7, "ivy": 3}
    prompt = await client.get_prompt("restock", {"item": "ivy"})
    assert prompt.messages[0].content.text == "Restock ivy."


async def probe_composition(client, *, gateway):
    arguments = {"boxes": 3, "items_per_box": 7}
    result = await client.call_tool("inventory_count", {})
    assert result.data == 10
    assert result.meta["canary_middleware"] is True
    extensions = client.server_capabilities.extensions or {}
    if gateway and "io.modelcontextprotocol/tasks" not in extensions:
        assert (await client.call_tool("shipment_size", arguments)).data == 21
        return "not_advertised_inline_verified"

    task = await call_tool_task(client, "shipment_size", arguments)
    assert (await task.result()).data == 21
    return "background_verified"


async def main(fixture, expected_version, url, *, token=None, gateway=False):
    if url is None:
        if fixture == "catalog":
            from fixtures.catalog.server import mcp
        else:
            from fixtures.composition.server import mcp
        client = Client(mcp)
    else:
        client = Client(
            url, auth=token,
        )

    async with client:
        actual = (await client.call_tool("installed_version", {})).data
        assert actual == expected_version, f"expected {expected_version}, got {actual}"
        checks = {}
        if fixture == "catalog":
            await probe_catalog(client)
        else:
            checks["shipment_execution"] = await probe_composition(client, gateway=gateway)

    print(f"{fixture}: verified FastMCP {actual}")
    return checks


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("fixture", choices=("catalog", "composition"))
    parser.add_argument("version")
    parser.add_argument("--url")
    args = parser.parse_args()
    token = os.environ["CANARY_GATEWAY_PROBE_TOKEN"] if args.url else None
    asyncio.run(main(args.fixture, args.version, args.url, token=token))
