import argparse
import json
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from packaging.version import InvalidVersion, Version

REPO = "PrefectHQ/fastmcp"


def get_json(url):
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=30) as response:
        return json.load(response), response.headers.get("Link", "")


def release_record(release):
    if release["draft"] or not release["published_at"]:
        return None
    tag = release["tag_name"]
    try:
        version = Version(tag)
    except InvalidVersion:
        return None
    return {
        "id": release["id"],
        "tag": tag,
        "version": str(version),
        "prerelease": release["prerelease"],
        "published_at": release["published_at"],
        "url": release["html_url"],
    }


def published_releases(since):
    page = f"https://api.github.com/repos/{REPO}/releases?per_page=100"
    releases = {}
    while page:
        batch, links = get_json(page)
        for item in batch:
            release = release_record(item)
            if release and datetime.fromisoformat(release["published_at"]) >= since:
                releases[release["id"]] = release
        page = next(
            (link.split(">;", 1)[0].lstrip(" <") for link in links.split(",")
             if 'rel="next"' in link),
            None,
        )
    return sorted(releases.values(), key=lambda item: (item["published_at"], item["id"]))


def published_release(release_id):
    record = release_record(get_json(
        f"https://api.github.com/repos/{REPO}/releases/{release_id}"
    )[0])
    if record is None:
        raise ValueError(f"FastMCP release {release_id} has no published package version")
    return record


def has_pypi_artifact(version):
    try:
        payload, _ = get_json(f"https://pypi.org/pypi/fastmcp/{version}/json")
    except HTTPError as error:
        if error.code != 404:
            raise
        return False
    return any(not artifact["yanked"] for artifact in payload["urls"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", type=datetime.fromisoformat, required=True)
    args = parser.parse_args()
    if args.since.utcoffset() is None:
        parser.error("--since requires a timezone")
    print(json.dumps(published_releases(args.since), indent=2))
