from uuid import UUID

from prefect import flow, task
from prefect.blocks.system import Secret

from gateway import FIXTURES, deploy_fixture
from releases import has_pypi_artifact, published_release


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
