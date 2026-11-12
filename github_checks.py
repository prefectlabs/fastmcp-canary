from time import time

import httpx
import jwt
from prefect import runtime
from prefect.blocks.system import Secret

REPO = "prefectlabs/fastmcp-canary"
REPO_ID = 1454624259
APP_SLUG = "fastmcp-canary"


def publish_check(app_id: int, evidence: dict, passed: bool) -> str:
    head_sha = runtime.deployment.version
    run_id = runtime.flow_run.id
    if not head_sha or not run_id:
        raise RuntimeError("Publishing requires a release run from a pinned deployment")

    now = int(time())
    assertion = jwt.encode(
        {"iat": now - 60, "exp": now + 300, "iss": str(app_id)},
        Secret.load("fastmcp-canary-github-app-key").get(),
        algorithm="RS256",
    )
    headers = {
        "Authorization": f"Bearer {assertion}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    with httpx.Client(base_url="https://api.github.com", headers=headers, timeout=30) as client:
        app = client.get("/app").raise_for_status().json()
        if (app["id"], app["slug"], app["owner"]["login"].lower()) != (app_id, APP_SLUG, "prefectlabs"):
            raise RuntimeError("Unexpected GitHub App identity")

        install = client.get(f"/repos/{REPO}/installation").raise_for_status().json()
        if install["app_id"] != app_id or install["suspended_at"]:
            raise RuntimeError("The canary GitHub App is unavailable")

        token = client.post(f"/app/installations/{install['id']}/access_tokens", json={
            "repository_ids": [REPO_ID], "permissions": {"checks": "write"},
        }).raise_for_status().json()["token"]
        client.headers["Authorization"] = f"Bearer {token}"
        try:
            return write_check(client, head_sha, str(run_id), evidence, passed)
        finally:
            client.delete("/installation/token").raise_for_status()


def write_check(client, head_sha, run_id, evidence, passed):
    release = evidence["release"]["tag"]
    name = f"FastMCP Gateway canary {release}"
    checks = f"/repos/{REPO}/check-runs"
    existing = client.get(f"/repos/{REPO}/commits/{head_sha}/check-runs", params={
        "check_name": name, "filter": "all", "per_page": 100,
    }).raise_for_status().json()["check_runs"]
    match = next((check for check in existing if check["external_id"] == run_id
                  and check["app"]["slug"] == APP_SLUG), None)
    payload = {
        "name": name,
        "details_url": evidence["run_url"],
        "external_id": run_id,
        "status": "completed",
        "conclusion": "success" if passed else "failure",
        "output": {
            "title": f"{release} compatibility check",
            "summary": "Live fixture checks passed." if passed else "A live fixture check failed.",
        },
    }
    if match:
        response = client.patch(f"{checks}/{match['id']}", json=payload)
    else:
        response = client.post(checks, json={**payload, "head_sha": head_sha})
    return response.raise_for_status().json()["html_url"]
