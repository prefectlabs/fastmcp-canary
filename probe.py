import argparse
import asyncio
import json
import os

from fastmcp import Client
from fastmcp.exceptions import ToolError
from fastmcp_tasks import call_tool_task
from packaging.version import Version


async def probe_catalog(client, version):
    tools = {tool.name: tool for tool in await client.list_tools()}
    assert "stock" in tools, "stock tool is missing"
    quantity_type = tools["stock"].input_schema.get("properties", {}).get("quantity", {}).get("type")
    assert quantity_type == "integer", f"stock quantity type should be integer, got {quantity_type}"
    result = await client.call_tool("stock", {"item": "fern", "quantity": 2})
    assert result.data == 5, f"stock should return 5, got {result.data}"
    try:
        await client.call_tool("stock", {"item": "fern", "quantity": 0})
    except ToolError as error:
        assert "quantity must be positive" in str(error), f"unexpected stock validation error: {error}"
    else:
        raise AssertionError("stock accepted an invalid quantity")

    resources = await client.read_resource("catalog://items")
    assert resources, "catalog resource returned no content"
    items = json.loads(resources[0].text)
    assert items == {"fern": 7, "ivy": 3}, f"unexpected catalog contents: {items}"
    prompt = await client.get_prompt("restock", {"item": "ivy"})
    assert prompt.messages, "restock prompt returned no messages"
    text = prompt.messages[0].content.text
    assert text == "Restock ivy.", f"unexpected restock prompt: {text}"
    if Version(version) >= Version("4.2.0a4"):
        assert "inventory_count" in tools, "the Annotated dependency tool is missing"
        properties = tools["inventory_count"].input_schema["properties"]
        assert set(properties) == {"item"}, f"dependency leaked into the tool schema: {properties}"
        result = await client.call_tool("inventory_count", {"item": "ivy"})
        assert result.data == 3, f"expected injected inventory count 3, got {result.data}"
        return {"annotated_dependency": "verified"}
    return {}


async def probe_composition(client, *, gateway):
    arguments = {"boxes": 3, "items_per_box": 7}
    result = await client.call_tool("inventory_count", {})
    assert result.data == 10, f"inventory count should be 10, got {result.data}"
    assert (result.meta or {}).get("canary_middleware") is True, "inventory middleware metadata is missing"
    extensions = client.server_capabilities.extensions or {}
    if gateway and "io.modelcontextprotocol/tasks" not in extensions:
        result = await client.call_tool("shipment_size", arguments)
        assert result.data == 21, f"inline shipment size should be 21, got {result.data}"
        return "not_advertised_inline_verified"

    task = await call_tool_task(client, "shipment_size", arguments)
    result = await task.result()
    assert result.data == 21, f"background shipment size should be 21, got {result.data}"
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
        if actual != expected_version:
            raise RuntimeError(f"Fixture reported {actual} while probing {expected_version}")
        checks = {}
        if fixture == "catalog":
            checks = await probe_catalog(client, actual)
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
