from importlib.metadata import version
from typing import Annotated

from fastmcp import FastMCP
from packaging.version import Version

mcp = FastMCP("Canary Catalog")
CATALOG = {"fern": 7, "ivy": 3}


@mcp.tool
def installed_version() -> str:
    return version("fastmcp")


@mcp.tool
def stock(item: str, quantity: int) -> int:
    if quantity < 1:
        raise ValueError("quantity must be positive")
    return CATALOG[item] - quantity


@mcp.resource("catalog://items")
def items() -> dict[str, int]:
    return CATALOG


@mcp.prompt
def restock(item: str) -> str:
    return f"Restock {item}."


if Version(version("fastmcp")) >= Version("4.2.0a4"):
    from fastmcp.dependencies import Depends

    def inventory() -> dict[str, int]:
        return CATALOG

    @mcp.tool
    def inventory_count(item: str, counts: Annotated[dict[str, int], Depends(inventory)]) -> int:
        return counts[item]
