# fastmcp-canary

Downstream compatibility checks for [FastMCP](https://github.com/PrefectHQ/fastmcp) on [FastMCP Gateway](https://www.prefect.io/fastmcp/hosting). The fixtures use synthetic data.

## local checks

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and run:

```sh
uv run --no-project --with 'fastmcp[tasks]==4.1.8' python probe.py catalog 4.1.8
uv run --no-project --with 'fastmcp[tasks]==4.1.8' python probe.py composition 4.1.8
```

Pass `--url` to probe a deployed fixture using `CANARY_GATEWAY_PROBE_TOKEN`.
