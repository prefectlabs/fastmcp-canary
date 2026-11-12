from datetime import datetime
from uuid import UUID

from fastmcp.exceptions import ToolError
from prefect import flow, runtime, task
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client

from assessment import assess_release
from gateway import FIXTURES, deploy_fixture
from github_checks import publish_check
from releases import has_pypi_artifact, published_release, published_releases


@task(retries=5, retry_delay_seconds=[15, 30, 60, 120, 240])
def wait_for_artifact(release_id: int) -> dict:
    release = published_release(release_id)
    if not has_pypi_artifact(release["version"]):
        raise RuntimeError(f"{release['tag']} is published on GitHub but unavailable on PyPI")
    return release


@flow(name="fastmcp-canary-check-release", log_prints=True)
def check_release(
    release_id: int,
    organization_id: UUID,
    assessment_flow_id: UUID,
    assessment_version_id: UUID,
    baseline_version: str = "4.1.8",
    github_app_id: int | None = None,
) -> dict:
    release = wait_for_artifact(release_id)
    credentials = {
        "deploy_token": Secret.load("fastmcp-canary-gateway-deploy").get(),
        "probe_token": Secret.load("fastmcp-canary-gateway-probe").get(),
    }
    results = []
    for fixture in FIXTURES:
        try:
            candidate = deploy_fixture(fixture, release["version"], str(organization_id), **credentials)
        except (AssertionError, ToolError) as first_failure:
            result = {"fixture": fixture, "candidate": release["version"], "failure": str(first_failure)}
            if release["version"] != baseline_version:
                result.update(compare_fixture(
                    fixture, release["version"], baseline_version, str(organization_id), credentials,
                ))
        else:
            result = {"fixture": fixture, "candidate": candidate}
        results.append(result)

    evidence = {
        "release": release,
        "fixtures": results,
        "run_url": runtime.flow_run.ui_url,
        "coverage": "Catalog covers typed tools, validation, resources, prompts and Annotated dependency injection on supported versions. Composition covers a namespaced tool, result middleware and shipment results. Background task completion is verified when Gateway advertises the tasks extension.",
    }
    assessment = assess_release(assessment_flow_id, assessment_version_id, evidence)
    passed = all("failure" not in result for result in results)
    check_url = publish_check(github_app_id, evidence, passed) if github_app_id else None
    if not passed:
        raise RuntimeError(f"Canary probe failed. Assessment run {assessment['run_id']}")
    return {**evidence, "assessment": assessment, "github_check": check_url}


def compare_fixture(fixture, candidate_version, baseline_version, organization_id, credentials):
    try:
        baseline = deploy_fixture(fixture, baseline_version, organization_id, **credentials)
    except (AssertionError, ToolError) as error:
        return {"baseline_version": baseline_version, "baseline_failure": str(error)}

    try:
        deploy_fixture(fixture, candidate_version, organization_id, **credentials)
    except (AssertionError, ToolError) as error:
        return {"baseline": baseline, "candidate_repeat_failure": str(error)}
    return {"baseline": baseline, "candidate_repeat_passed": True}


@flow(name="fastmcp-canary-intake")
def intake(
    check_deployment_id: UUID,
    since: datetime,
    organization_id: UUID | None = None,
    release_id: int | None = None,
) -> dict:
    if since.utcoffset() is None:
        raise ValueError("since requires a timezone")
    if release_id is None:
        releases = published_releases(since)
    else:
        release = published_release(release_id)
        releases = [release] if datetime.fromisoformat(release["published_at"]) >= since else []

    if organization_id is None:
        return {"discovered": [release["id"] for release in releases], "runs": {}}

    runs = {}
    with get_client(sync_client=True) as client:
        for release in releases:
            run = client.create_flow_run_from_deployment(
                check_deployment_id,
                name=f"fastmcp-canary-{release['tag']}",
                parameters={"release_id": release["id"], "organization_id": str(organization_id)},
                idempotency_key=f"fastmcp-canary-release-{release['id']}",
            )
            runs[release["id"]] = str(run.id)
    return {"discovered": list(runs), "runs": runs}
