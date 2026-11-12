from time import monotonic, sleep
from uuid import UUID

import httpx
from prefect import runtime
from prefect.artifacts import create_markdown_artifact
from prefect.client.orchestration import get_client


def verified_assessment(result: dict, evidence: dict) -> dict:
    if result["decision"] != "draft_regression":
        return result
    if any(fixture.get("confirmed_repeat") for fixture in evidence["fixtures"]):
        return result
    return {
        "decision": "needs_evidence",
        "title": "Regression could not be confirmed",
        "body": "A report requires the same candidate failure twice and a passing baseline.",
    }


def assess_release(flow_id: UUID, version_id: UUID, evidence: dict) -> dict:
    with get_client(sync_client=True) as client:
        plan = f"/flows/{flow_id}/execution-plan"
        active = client.request("GET", plan).json()["active_version"]
        if not active or active["id"] != str(version_id):
            raise RuntimeError("The configured canary assessment plan is no longer active")

        run = client.request("POST", f"{plan}/runs", json={
            "name": f"assess-{evidence['release']['tag']}",
            "parameters": {"evidence": evidence},
            "idempotency_key": f"canary-assess-{runtime.flow_run.id}",
        }).json()
        run_id = run["id"]
        path = f"/flow_runs/{run_id}/execution-plan"
        deadline = monotonic() + 240
        while monotonic() < deadline:
            state = client.request("GET", path).json()
            if state["snapshot"]["execution_plan_version_id"] != str(version_id):
                raise RuntimeError(f"Assessment used a different plan version in run {run_id}")
            if state["status"] in {"failed", "cancelled", "blocked"}:
                raise RuntimeError(f"Assessment ended {state['status']} in run {run_id}")
            if state["status"] == "completed":
                try:
                    output = client.request("GET", f"{path}/outputs/result").json()
                except httpx.HTTPStatusError as error:
                    response = error.response
                    pending_output = (
                        response.status_code == 409
                        and response.json().get("reason") == "output_not_available"
                        and response.json().get("output_status") == "waiting"
                    )
                    if response.status_code != 404 and not pending_output:
                        raise
                else:
                    result = verified_assessment(output["assessment"], evidence)
                    break
            sleep(5)
        else:
            raise TimeoutError(f"No assessment output in run {run_id}")

    create_markdown_artifact(
        f"# {result['title']}\n\n{result['body']}",
        key=f"fastmcp-canary-{evidence['release']['id']}",
        description=f"{result['decision']} from execution plan run {run_id}",
    )
    return {"run_id": run_id, **result}
