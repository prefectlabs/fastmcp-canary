from importlib.metadata import version

from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware
from fastmcp_tasks import TasksExtension

mcp = FastMCP("Canary Composition")
child = FastMCP("Inventory")
mcp.add_extension(TasksExtension())


class CountMiddleware(Middleware):
    async def on_call_tool(self, context, call_next):
        result = await call_next(context)
        if context.message.name == "inventory_count":
            result.meta = {**(result.meta or {}), "canary_middleware": True}
        return result


@mcp.tool
def installed_version() -> str:
    return version("fastmcp")


@mcp.tool(task=True)
async def shipment_size(boxes: int, items_per_box: int) -> int:
    return boxes * items_per_box


@child.tool
def count() -> int:
    return 10


mcp.add_middleware(CountMiddleware())
mcp.mount(child, namespace="inventory")
