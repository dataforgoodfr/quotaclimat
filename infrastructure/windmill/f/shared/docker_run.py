"""Run a Docker container on the host and stream its logs: the Windmill equivalent of Kestra's docker.Run.

Every pipeline step (barometre, RRS, dbt...) calls this script with an image, its entrypoint/command and its
environment. It talks to the host's Docker daemon through its Unix socket (mounted into the workers), with the
standard library only, so the script has no dependency to install or lock.

Secrets are given as variable paths in `secret_env` and read here, at run time: their values never appear in
the job arguments (stored in the database and shown in the UI) or in the logs.
"""

import base64
import http.client
import json
import os
import signal
import socket
import sys
import urllib.parse

import wmill

DOCKER_SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
API = "/v1.41"  # Docker Engine API 1.41 (Docker 20.10+), accepted by newer daemons


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, timeout=None):
        super().__init__("localhost", timeout=timeout)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(DOCKER_SOCKET)


def _request(method, path, body=None, headers=None, stream=False, ok=(200, 201, 204)):
    """Calls the Docker API. With stream, returns the open response (to read as it comes)."""
    conn = _UnixConnection()
    payload = json.dumps(body).encode() if body is not None else None
    all_headers = {"Content-Type": "application/json", **(headers or {})}
    conn.request(method, API + path, body=payload, headers=all_headers)
    resp = conn.getresponse()
    if resp.status not in ok:
        detail = resp.read().decode(errors="replace")
        conn.close()
        raise RuntimeError(f"Docker API {method} {path}: HTTP {resp.status} {detail}")
    if stream:
        return resp
    data = resp.read()
    conn.close()
    return json.loads(data) if data else None


def _secret(spec):
    """`f/secrets/NAME` returns the variable; `f/secrets/NAME#a.b` returns key a.b of a JSON variable."""
    path, _, key = spec.partition("#")
    value = wmill.get_variable(path)
    if key:
        data = json.loads(value)
        for part in key.split("."):
            data = data[part]
        value = data
    return value


def _env_value(value):
    """Same rendering as Kestra: booleans in lowercase, empty for null, JSON for objects and lists."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _bytes(size):
    units = {"k": 1024, "m": 1024**2, "g": 1024**3}
    size = size.strip().lower().rstrip("b")
    if size and size[-1] in units:
        return int(float(size[:-1]) * units[size[-1]])
    return int(size)


def _split_image(image):
    """`registry/repo:tag` -> (`registry/repo`, `tag`); the tag defaults to latest."""
    last = image.rsplit("/", 1)[-1]
    if ":" in last and "@" not in last:
        repo, tag = image.rsplit(":", 1)
        return repo, tag
    return image, "latest"


def _pull(image, registry_auth):
    repo, tag = _split_image(image)
    query = urllib.parse.urlencode({"fromImage": repo, "tag": tag})
    headers = {}
    if registry_auth:
        headers["X-Registry-Auth"] = base64.urlsafe_b64encode(json.dumps(registry_auth).encode()).decode()
    resp = _request("POST", f"/images/create?{query}", headers=headers, stream=True)
    # The pull progress is one JSON object per line; an error comes as {"error": ...}
    for line in resp:
        if not line.strip():
            continue
        event = json.loads(line)
        if "error" in event:
            raise RuntimeError(f"Pull of {image} failed: {event['error']}")
    resp.close()
    print(f"Pulled {image}", flush=True)


def _stream_logs(container_id, stdout_logs):
    """Prints the container's output as it comes. Without a TTY, Docker multiplexes stdout and stderr:
    each frame starts with 8 bytes, the stream (1 stdout, 2 stderr) then the size of the frame."""
    query = urllib.parse.urlencode({"follow": 1, "stdout": 1, "stderr": 1})
    resp = _request("GET", f"/containers/{container_id}/logs?{query}", stream=True)
    pending = {1: b"", 2: b""}
    while True:
        header = resp.read(8)
        if len(header) < 8:
            break
        kind = header[0]
        size = int.from_bytes(header[4:8], "big")
        data = resp.read(size)
        if kind == 1 and not stdout_logs:
            continue
        pending[kind] = pending.get(kind, b"") + data
        *lines, pending[kind] = pending[kind].split(b"\n")
        for line in lines:
            print(line.decode(errors="replace"), flush=True)
    for rest in pending.values():
        if rest:
            print(rest.decode(errors="replace"), flush=True)
    resp.close()


def main(
    image: str,
    entrypoint: list[str] | None = None,
    command: list[str] | None = None,
    env: dict | None = None,
    secret_env: dict | None = None,
    registry: str = "",
    registry_password_secret: str = "",
    shm_size: str = "6g",
    stdout_logs: bool = True,
):
    """
    image: image to run, pulled before each run (Kestra's pullPolicy: ALWAYS).
    entrypoint: replaces the image's entrypoint (Kestra's entryPoint); empty keeps the image's.
    command: arguments given to the entrypoint (Kestra's commands); empty keeps the image's.
    env: environment variables of the container.
    secret_env: environment variables read from Windmill variables, {NAME: "f/secrets/X"} or
        {NAME: "f/secrets/X#key"} for one key of a JSON variable.
    registry / registry_password_secret: private registry and the variable holding its password
        (Scaleway registries take the user `nologin` and a secret key).
    shm_size: size of /dev/shm (Kestra's shmSize).
    stdout_logs: false keeps only the container's stderr in the logs. Kestra logs stdout as INFO and stderr as
        WARN, so this is its `logLevel: WARN`, used to keep per-row output out of the logs.
    """
    job_id = os.environ.get("WM_JOB_ID", "local")
    environment = {key: _env_value(value) for key, value in (env or {}).items()}
    for key, spec in (secret_env or {}).items():
        environment[key] = _env_value(_secret(spec))

    registry_auth = None
    if registry:
        registry_auth = {
            "username": "nologin",
            "password": wmill.get_variable(registry_password_secret),
            "serveraddress": registry,
        }
    _pull(image, registry_auth)

    name = f"windmill-{job_id}"
    created = _request(
        "POST",
        f"/containers/create?{urllib.parse.urlencode({'name': name})}",
        body={
            "Image": image,
            "Entrypoint": entrypoint or None,
            "Cmd": command or None,
            "Env": [f"{key}={value}" for key, value in environment.items()],
            "Labels": {"windmill.job_id": job_id},
            "HostConfig": {"ShmSize": _bytes(shm_size)},
        },
    )
    container_id = created["Id"]

    def remove():
        try:
            _request("DELETE", f"/containers/{container_id}?force=1", ok=(204, 404))
        except Exception as exc:  # the job result matters more than the cleanup
            print(f"Could not remove container {name}: {exc}", file=sys.stderr, flush=True)

    # Cancelling the job in Windmill stops the container instead of leaving it running on the host
    def on_cancel(signum, _frame):
        print(f"Job cancelled (signal {signum}): stopping container {name}", flush=True)
        try:
            _request("POST", f"/containers/{container_id}/stop?t=10", ok=(204, 304, 404))
        finally:
            remove()
        sys.exit(1)

    signal.signal(signal.SIGTERM, on_cancel)
    signal.signal(signal.SIGINT, on_cancel)

    try:
        _request("POST", f"/containers/{container_id}/start", ok=(204, 304))
        print(f"Started {image} as {name}", flush=True)
        _stream_logs(container_id, stdout_logs)
        status = _request("POST", f"/containers/{container_id}/wait")["StatusCode"]
    finally:
        remove()

    if status != 0:
        raise RuntimeError(f"{image} exited with code {status}")
    return {"image": image, "exit_code": status}
