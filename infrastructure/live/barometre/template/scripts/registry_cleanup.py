"""Delete old tags of a Scaleway Container Registry namespace.

Run by the `registry-cleanup-<env>` Scaleway job (registry_cleanup.tf), standard
library only so it runs on a plain python image.

For each image of the namespace, keeps:
- the KEEP most recent tags;
- the PROTECTED_TAGS (e.g. `latest`);
- every tag referenced by a Scaleway job definition or serverless container of
  the organization (so a job pinned to an old version keeps working).
Every other tag is deleted. A tag sharing its digest with a kept tag is never
deleted (the API refuses it without `force`, which we never send).

Env vars:
- SCW_SECRET_KEY (required): API key with ContainerRegistryFullAccess on the
  namespace project and read access to jobs/containers of the organization.
- SCW_DEFAULT_ORGANIZATION_ID (required).
- REGISTRY_NAMESPACE_ID (required).
- REGION (default fr-par).
- KEEP (default 10).
- PROTECTED_TAGS (default "latest"): comma-separated tag names.
- DRY_RUN (default "true"): "false" to actually delete.
- WORKERS (default 8): parallel deletions.
"""

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

API = "https://api.scaleway.com"
REGION = os.environ.get("REGION", "fr-par")
TOKEN = os.environ.get("SCW_SECRET_KEY", "")
ORG = os.environ["SCW_DEFAULT_ORGANIZATION_ID"]
NAMESPACE_ID = os.environ["REGISTRY_NAMESPACE_ID"]
KEEP = int(os.environ.get("KEEP", "10"))
PROTECTED = {t.strip() for t in os.environ.get("PROTECTED_TAGS", "latest").split(",") if t.strip()}
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
WORKERS = int(os.environ.get("WORKERS", "8"))


def call(method, path, params=None):
    url = f"{API}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, method=method, headers={"X-Auth-Token": TOKEN})
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read()
        return json.loads(body) if body else {}


def list_all(path, key, params):
    """Pages through a Scaleway list endpoint."""
    items, page = [], 1
    while True:
        data = call("GET", path, {**params, "page": page, "page_size": 100})
        batch = data.get(key, [])
        items += batch
        if len(batch) < 100:
            return items
        page += 1


def images_in_use(endpoint):
    """`image:tag` of the namespace referenced by jobs and containers."""
    refs = [
        j.get("image_uri", "")
        for j in list_all(
            f"/serverless-jobs/v1alpha1/regions/{REGION}/job-definitions",
            "job_definitions",
            {"organization_id": ORG},
        )
    ] + [
        c.get("registry_image", "")
        for c in list_all(
            f"/containers/v1beta1/regions/{REGION}/containers",
            "containers",
            {"organization_id": ORG},
        )
    ]
    prefix = endpoint.rstrip("/") + "/"
    return {r[len(prefix):] for r in refs if r.startswith(prefix)}


def delete(tag):
    if DRY_RUN:
        return "would_delete"
    try:
        call("DELETE", f"/registry/v1/regions/{REGION}/tags/{tag['id']}")
        return "deleted"
    except urllib.error.HTTPError as e:
        print(f"  cannot delete {tag['image']}:{tag['name']}: HTTP {e.code} {e.read()[:200]!r}")
        return "failed"


def main():
    namespace = call("GET", f"/registry/v1/regions/{REGION}/namespaces/{NAMESPACE_ID}")
    endpoint = namespace["endpoint"]
    in_use = images_in_use(endpoint)
    print(
        f"Namespace {namespace['name']} ({namespace['size'] / 1e9:.1f} GB), keep={KEEP}, "
        f"protected={sorted(PROTECTED)}, dry_run={DRY_RUN}"
    )
    print(f"Tags in use by jobs/containers: {sorted(in_use)}")

    to_delete = []
    images = list_all(
        f"/registry/v1/regions/{REGION}/images", "images", {"namespace_id": NAMESPACE_ID}
    )
    for image in sorted(images, key=lambda i: i["name"]):
        tags = list_all(
            f"/registry/v1/regions/{REGION}/images/{image['id']}/tags",
            "tags",
            {"order_by": "created_at_desc"},
        )
        kept = {t["name"] for t in tags[:KEEP]} | PROTECTED
        kept |= {ref.split(":", 1)[1] for ref in in_use if ref.split(":", 1)[0] == image["name"]}
        # Never delete a tag whose digest is also carried by a kept tag.
        kept_digests = {t["digest"] for t in tags if t["name"] in kept}
        doomed = [t for t in tags if t["name"] not in kept and t["digest"] not in kept_digests]
        print(f"{image['name']}: {len(tags)} tags, {len(doomed)} to delete")
        to_delete += [{**t, "image": image["name"]} for t in doomed]

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(delete, to_delete))
    summary = {r: results.count(r) for r in set(results)}
    print(f"Done: {summary or 'nothing to delete'}")
    return 1 if summary.get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())
