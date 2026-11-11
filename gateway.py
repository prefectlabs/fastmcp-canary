import argparse
import asyncio
import gzip
import hashlib
import io
import json
import os
import tarfile
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
from packaging.version import Version

from probe import main as probe

API = "https://horizon.prefect.io/api/v0"
ORG_SLUG = "fastmcp-canary"
FIXTURES = {"catalog": "fastmcp", "composition": "fastmcp[tasks]"}


def fixture_archive(fixture, version):
    requirements = f"{FIXTURES[fixture]}=={Version(version)}\n".encode()
    server = Path(__file__).parent / "fixtures" / fixture / "server.py"
    compressed = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed, mode="wb", mtime=0) as stream:
        with tarfile.open(fileobj=stream, mode="w") as archive:
            for name, data in (("server.py", server.read_bytes()), ("requirements.txt", requirements)):
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                entry.mode = 0o644
                archive.addfile(entry, io.BytesIO(data))
    return compressed.getvalue()


def request(client, method, path, **kwargs):
    response = client.request(method, path, **kwargs)
    if response.is_error:
        raise RuntimeError(f"Gateway returned HTTP {response.status_code} for {method} {path}")
    return response.json()


def items(client, path):
    cursor = None
    while True:
        params = {"cursor": cursor} if cursor else {}
        result = request(client, "GET", path, params=params)
        yield from result["items"]
        cursor = result.get("meta", {}).get("nextCursor")
        if cursor is None:
            return


def wait_for(client, path, pending, expected):
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        result = request(client, "GET", path)
        status = result["status"]
        if status == expected:
            return result
        if status not in pending:
            raise RuntimeError(f"{path} reached {status}")
        time.sleep(4)
    raise TimeoutError(f"{path} did not reach {expected}")


def deploy_fixture(fixture, version, organization_id):
    archive = fixture_archive(fixture, version)
    checksum = hashlib.sha256(archive).hexdigest()
    with httpx.Client(
        base_url=API, headers={"Authorization": f"Bearer {os.environ['CANARY_GATEWAY_DEPLOY_TOKEN']}"},
        timeout=60,
    ) as client:
        orgs = {org["slug"]: org["id"] for org in items(client, "/me/organizations")}
        if orgs.get(ORG_SLUG) != organization_id or set(orgs.values()) != {organization_id}:
            raise RuntimeError("Gateway deploy identity must belong only to the canary organization")

        projects = f"/organizations/{organization_id}/projects"
        slug = f"canary-{fixture}"
        project = next((item for item in items(client, projects) if item["slug"] == slug), None)
        if project is None:
            raise RuntimeError(f"Provision the hosted {slug} project in the canary organization")
        if (project["organizationId"] != organization_id or project["kind"] != "hosted"
                or project["authMode"] != "fastmcp-cloud"):
            raise RuntimeError(f"{slug} must be hosted and protected in the canary organization")

        base = f"{projects}/{project['id']}"
        target = request(client, "GET", f"{base}/targets/by-slug/production")
        serving = urlparse(target["servingUrl"])
        if (serving.scheme != "https" or serving.username or not serving.hostname
                or not serving.hostname.endswith(".fastmcp.app")):
            raise RuntimeError("Gateway returned an unexpected serving origin")

        upload = request(client, "POST", f"{base}/uploads/url", json={
            "sizeBytes": len(archive), "checksumSha256": checksum,
        })["upload"]
        if urlparse(upload["url"]).scheme != "https":
            raise RuntimeError("Gateway upload requires HTTPS")
        try:
            uploaded = httpx.put(upload["url"], headers=upload["headers"], content=archive, timeout=60)
        except httpx.RequestError:
            raise RuntimeError("Gateway archive upload could not reach the storage service") from None
        if not uploaded.is_success:
            raise RuntimeError(f"Gateway archive upload returned HTTP {uploaded.status_code}")

        build = request(client, "POST", f"{base}/versions", json={
            "source": {"kind": "archive", "checksumSha256": checksum},
            "build": {"entrypoint": "server.py:mcp", "dependencyPath": "requirements.txt"},
        })
        wait_for(client, f"{base}/versions/{build['id']}", {"building"}, "ready")
        deployment = request(client, "POST", f"{base}/deployments", json={
            "targetId": target["id"], "versionId": build["id"],
        })
        wait_for(client, f"{base}/deployments/{deployment['id']}", {"queued", "deploying"}, "succeeded")
        current = request(client, "GET", f"{base}/targets/by-slug/production")
        if current["currentDeploymentId"] != deployment["id"]:
            raise RuntimeError(f"{slug} is serving a different deployment")

    url = target["servingUrl"].rstrip("/")
    if not url.endswith("/mcp"):
        url += "/mcp"
    asyncio.run(probe(fixture, version, url))
    return {"organization_id": organization_id, "project_id": project["id"],
            "deployment_id": deployment["id"], "version": version, "url": url}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("fixture", choices=FIXTURES)
    parser.add_argument("version", type=lambda value: str(Version(value)))
    parser.add_argument("--organization-id", required=True)
    args = parser.parse_args()
    print(json.dumps(deploy_fixture(args.fixture, args.version, args.organization_id)))
