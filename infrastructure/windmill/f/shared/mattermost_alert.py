"""Posts a flow failure to Mattermost: the failure handler of the main pipelines (Kestra's `errors:` block).

Sends one message to each incoming webhook listed, comma-separated without spaces, in the variable
`webhooks_secret` (MATTERMOST_WEBHOOK_URLS). Every webhook is tried, so a dead one does not prevent the others
from being notified; the step fails afterwards if any of them failed.
"""

import json
import os
import urllib.request

import wmill


def main(flow_name: str, error: dict, webhooks_secret: str = "f/secrets/MATTERMOST_WEBHOOK_URLS"):
    base_url = os.environ.get("WM_BASE_URL", "").rstrip("/")
    workspace = os.environ.get("WM_WORKSPACE", "")
    flow_job_id = os.environ.get("WM_ROOT_FLOW_JOB_ID") or os.environ.get("WM_FLOW_JOB_ID", "")
    run_url = f"{base_url}/run/{flow_job_id}?workspace={workspace}"

    step = (error or {}).get("step_id") or "unknown"
    message = (error or {}).get("message") or ""
    text = f"{flow_name} failed at step `{step}`.\nRun: {run_url}"
    if message:
        text += f"\nError: {message[:500]}"
    payload = json.dumps({"username": "Windmill", "text": text}).encode()

    webhooks = [url.strip() for url in wmill.get_variable(webhooks_secret).split(",") if url.strip()]
    failed = []
    for index, url in enumerate(webhooks, 1):
        request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response.read()
        except Exception as exc:  # the URL holds the webhook token: only its position is logged
            print(f"Webhook {index}/{len(webhooks)} failed: {type(exc).__name__}", flush=True)
            failed.append(index)

    if failed:
        raise RuntimeError(f"{len(failed)} of {len(webhooks)} Mattermost webhooks failed: {failed}")
    return {"sent": len(webhooks), "step": step}
