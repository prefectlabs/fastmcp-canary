from importlib.metadata import version

from fastmcp import FastMCP

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
