from datetime import datetime
from uuid import UUID

from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client

from gateway import FIXTURES, deploy_fixture
from releases import has_pypi_artifact, published_release, published_releases


@task(retries=5, retry_delay_seconds=[15, 30, 60, 120, 240])
def wait_for_artifact(release_id: int) -> dict:
    release = published_release(release_id)
    if not has_pypi_artifact(release["version"]):
        raise RuntimeError(f"{release['tag']} is published on GitHub but unavailable on PyPI")
    return release


@flow(name="fastmcp-canary-check-release", log_prints=True)
def check_release(release_id: int, organization_id: UUID) -> dict:
    release = wait_for_artifact(release_id)
    deploy_token = Secret.load("fastmcp-canary-gateway-deploy").get()
    probe_token = Secret.load("fastmcp-canary-gateway-probe").get()
    results = []
    for fixture in FIXTURES:
        results.append(deploy_fixture(
            fixture, release["version"], str(organization_id),
            deploy_token=deploy_token, probe_token=probe_token,
        ))
    return {"release": release, "fixtures": results}


@flow(name="fastmcp-canary-intake")
def intake(
    check_deployment_id: UUID,
    organization_id: UUID,
    since: datetime,
    release_id: int | None = None,
) -> dict[int, str]:
    if since.utcoffset() is None:
        raise ValueError("since requires a timezone")
    if release_id is None:
        releases = published_releases(since)
    else:
        release = published_release(release_id)
        releases = [release] if datetime.fromisoformat(release["published_at"]) >= since else []

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
    return runs
