"""OIDC -> Metabase OSS session adapter.

Flow: browser -> /sso/login/<org> -> Keycloak (OIDC) -> /sso/callback/<org>
-> find/create Metabase user, rotate a random password, mint a Metabase session
via POST /api/session, set the metabase.SESSION cookie, redirect to /.
"""

import asyncio
import html
import logging
import os
import secrets
from contextlib import asynccontextmanager
from urllib.parse import urlencode

import httpx
from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sso")

PUBLIC_URL = os.environ["PUBLIC_URL"].rstrip("/")
MB_URL = os.environ.get("MB_URL", "http://metabase:3000")
ADMIN_EMAIL = os.environ["MB_ADMIN_EMAIL"].lower()
ADMIN_PW = os.environ["MB_ADMIN_PASSWORD"]
COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "false").lower() == "true"
SESSION_MAX_AGE = int(os.environ.get("SESSION_MAX_AGE", 14 * 24 * 3600))
SESSION_SECRET = os.environ.get("SESSION_SECRET", "change-me")
ALLOW_ANY_DOMAIN = os.environ.get("ALLOW_ANY_DOMAIN", "false").lower() == "true"

# One org (Keycloak, which brokers Google). Same dict shape as the multi-IdP version.
ORGS = {
    "keycloak": {
        "label": "Keycloak",
        "discovery": os.environ["OIDC_DISCOVERY_URL"],
        "client_id": os.environ["OIDC_CLIENT_ID"],
        "client_secret": os.environ["OIDC_CLIENT_SECRET"],
        "domains": [
            d.strip().lower()
            for d in os.environ.get("ALLOWED_EMAIL_DOMAINS", "").split(",")
            if d.strip()
        ],
        # Metabase groups a login may add the user to (exact names, comma separated)
        "groups": [g.strip() for g in os.environ.get("ALLOWED_GROUPS", "").split(",") if g.strip()],
    }
}

# Keycloak admin API, to mirror a user's Metabase groups into Keycloak at their first login. Disabled when
# KC_ADMIN_CLIENT_SECRET is empty. The client only has a service account (manage-users, view-users, query-groups).
KC_URL = os.environ.get("KC_URL", "http://keycloak:8080").rstrip("/")
KC_REALM = os.environ.get("KC_REALM", "")
KC_ADMIN_CLIENT_ID = os.environ.get("KC_ADMIN_CLIENT_ID", "metabase-sso-admin")
KC_ADMIN_CLIENT_SECRET = os.environ.get("KC_ADMIN_CLIENT_SECRET", "")
# Keycloak group that marks a user as already mirrored. Remove the user from it to mirror again at the next login.
KC_MARKER_GROUP = os.environ.get("KC_MARKER_GROUP", "sso-metabase-synced")

oauth = OAuth()
for key, cfg in ORGS.items():
    oauth.register(
        name=f"org_{key}",
        server_metadata_url=cfg["discovery"],
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        client_kwargs={"scope": "openid email profile", "code_challenge_method": "S256"},
    )


# --------------------------------------------------------------------------
# Metabase admin API helper (logs in as the bootstrap admin, re-logs on 401)
# --------------------------------------------------------------------------
class Metabase:
    def __init__(self):
        self.http = httpx.AsyncClient(base_url=MB_URL, timeout=30)
        self.token = None
        self.lock = asyncio.Lock()

    async def login_admin(self):
        r = await self.http.post("/api/session", json={"username": ADMIN_EMAIL, "password": ADMIN_PW})
        r.raise_for_status()
        self.token = r.json()["id"]

    async def admin(self, method, path, **kw):
        async with self.lock:
            if not self.token:
                await self.login_admin()
        for attempt in (1, 2):
            r = await self.http.request(method, path, headers={"X-Metabase-Session": self.token}, **kw)
            if r.status_code == 401 and attempt == 1:
                async with self.lock:
                    await self.login_admin()
                continue
            r.raise_for_status()
            return r.json() if r.content else None


mb = Metabase()


class Keycloak:
    """Minimal Keycloak admin API client (client-credentials token, re-requested on 401)."""

    def __init__(self):
        self.http = httpx.AsyncClient(base_url=KC_URL, timeout=30)
        self.token = None
        self.lock = asyncio.Lock()

    async def login(self):
        r = await self.http.post(
            f"/realms/{KC_REALM}/protocol/openid-connect/token",
            data={
                "grant_type": "client_credentials",
                "client_id": KC_ADMIN_CLIENT_ID,
                "client_secret": KC_ADMIN_CLIENT_SECRET,
            },
        )
        r.raise_for_status()
        self.token = r.json()["access_token"]

    async def admin(self, method, path, **kw):
        async with self.lock:
            if not self.token:
                await self.login()
        for attempt in (1, 2):
            r = await self.http.request(
                method, f"/admin/realms/{KC_REALM}{path}", headers={"Authorization": f"Bearer {self.token}"}, **kw
            )
            if r.status_code == 401 and attempt == 1:
                async with self.lock:
                    await self.login()
                continue
            r.raise_for_status()
            return r
        return r

    async def ensure_group(self, name: str) -> str:
        """Id of the top-level group `name`, created when it does not exist."""
        r = await self.admin("GET", "/groups", params={"search": name, "exact": "true"})
        for g in r.json():
            if g["name"] == name:
                return g["id"]
        r = await self.admin("POST", "/groups", json={"name": name})
        return r.headers["Location"].rsplit("/", 1)[1]

    async def add_to_group(self, user_id: str, group_id: str):
        await self.admin("PUT", f"/users/{user_id}/groups/{group_id}")


kc = Keycloak() if (KC_ADMIN_CLIENT_SECRET and KC_REALM) else None


async def bootstrap():
    """Wait for Metabase; run first-time setup (creates the break-glass admin) if needed."""
    for _ in range(180):
        try:
            r = await mb.http.get("/api/session/properties")
            if r.status_code == 200:
                token = r.json().get("setup-token")
                if token:
                    r = await mb.http.post(
                        "/api/setup",
                        json={
                            "token": token,
                            "user": {
                                "email": ADMIN_EMAIL,
                                "password": ADMIN_PW,
                                "first_name": "Local",
                                "last_name": "Admin",
                            },
                            "prefs": {"site_name": "Metabase SSO", "site_locale": "en", "allow_tracking": False},
                        },
                    )
                    log.info("Metabase setup: HTTP %s", r.status_code)
                await mb.login_admin()
                log.info("Metabase ready; admin session established")
                return
        except Exception as e:  # noqa: BLE001 - keep retrying while Metabase boots
            log.debug("waiting for Metabase: %s", e)
        await asyncio.sleep(5)
    log.error("Metabase did not become ready (is the admin password already set to something else?)")


@asynccontextmanager
async def lifespan(_app):
    task = asyncio.create_task(bootstrap())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="sso_session",
    path="/sso",
    same_site="lax",
    https_only=COOKIE_SECURE,
    max_age=8 * 3600,
)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def page(body: str, status: int = 200) -> HTMLResponse:
    return HTMLResponse(
        "<!doctype html><meta charset=utf-8><title>Sign in</title>"
        "<style>body{font-family:system-ui;max-width:420px;margin:12vh auto;padding:0 1rem}"
        ".btn{display:block;margin:.6rem 0;padding:.8rem 1rem;border:1px solid #888;border-radius:8px;"
        "text-decoration:none;color:inherit;text-align:center}.btn:hover{background:#eee}</style>"
        f"{body}",
        status_code=status,
    )


def error(msg: str, status: int = 403) -> HTMLResponse:
    return page(f"<h2>Sign-in failed</h2><p>{html.escape(msg)}</p><p><a href='/sso/'>Back</a></p>", status)


_locks: dict[str, asyncio.Lock] = {}


def user_lock(email: str) -> asyncio.Lock:
    return _locks.setdefault(email, asyncio.Lock())


async def find_user(email: str):
    res = await mb.admin("GET", "/api/user", params={"query": email, "status": "all", "include_deactivated": "true"})
    users = res["data"] if isinstance(res, dict) else res
    return next((u for u in users if u["email"].lower() == email), None)


async def sync_groups(uid: int, allowed: list[str], claim_groups: list[str]):
    """Add the user to the existing Metabase groups named like their Keycloak groups.

    Additive only: groups are never created, renamed or emptied, their permissions are not touched, and a
    membership is never removed. Only the names in `allowed` count (exact match).
    """
    wanted = {g.strip("/").strip() for g in claim_groups} & set(allowed)
    if not wanted:
        return
    groups = await mb.admin("GET", "/api/permissions/group")
    by_name = {g["name"]: g["id"] for g in groups}
    for name in sorted(wanted - set(by_name)):
        log.warning("Keycloak group %r has no Metabase group of that name: skipped", name)
    mem = await mb.admin("GET", "/api/permissions/membership")
    flat = [m for lst in mem.values() for m in lst] if isinstance(mem, dict) else mem
    current = {m["group_id"] for m in flat if m["user_id"] == uid}
    for name in sorted(wanted & set(by_name)):
        if by_name[name] not in current:
            await mb.admin("POST", "/api/permissions/membership", json={"group_id": by_name[name], "user_id": uid})
            log.info("user %s added to Metabase group %r", uid, name)


async def mirror_metabase_groups_to_keycloak(sub: str, uid: int, allowed: list[str]):
    """First login: add the user, in Keycloak, to the groups they already have in Metabase.

    Only the names in `allowed` are mirrored (so never "All Users"); a missing Keycloak group is created
    (top-level, name only). Then the marker group is set so this runs once per user. Any failure is logged and
    leaves the marker unset: the next login tries again, and the login itself is never blocked.
    """
    try:
        groups = await mb.admin("GET", "/api/permissions/group")
        name_by_id = {g["id"]: g["name"] for g in groups}
        mem = await mb.admin("GET", "/api/permissions/membership")
        flat = [m for lst in mem.values() for m in lst] if isinstance(mem, dict) else mem
        names = sorted({name_by_id[m["group_id"]] for m in flat if m["user_id"] == uid and m["group_id"] in name_by_id})
        for name in [n for n in names if n in allowed]:
            await kc.add_to_group(sub, await kc.ensure_group(name))
            log.info("first login: %s added to Keycloak group %r (from Metabase)", sub, name)
        await kc.add_to_group(sub, await kc.ensure_group(KC_MARKER_GROUP))
    except httpx.HTTPError:
        log.exception("Could not mirror the Metabase groups into Keycloak for %s (will retry at next login)", sub)


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.get("/sso/health")
async def health():
    return {"ok": True}


@app.get("/")  # Traefik sends / here only when there is no Metabase session cookie
@app.get("/auth/login")  # Metabase's native login URL lands here via Traefik
@app.get("/sso")
@app.get("/sso/")
async def landing():
    if len(ORGS) == 1:  # single IdP: skip the button page
        return RedirectResponse(f"{PUBLIC_URL}/sso/login/{next(iter(ORGS))}", status_code=302)
    buttons = "".join(
        f'<a class="btn" href="/sso/login/{k}">Sign in with {html.escape(c["label"])}</a>' for k, c in ORGS.items()
    )
    return page(f"<h2>Metabase</h2>{buttons}")


@app.get("/sso/login/{org}")
async def login(org: str, request: Request):
    if org not in ORGS:
        return error("Unknown organisation", 404)
    return await getattr(oauth, f"org_{org}").authorize_redirect(request, f"{PUBLIC_URL}/sso/callback/{org}")


@app.get("/sso/callback/{org}")
async def callback(org: str, request: Request):
    if org not in ORGS:
        return error("Unknown organisation", 404)
    cfg = ORGS[org]
    client = getattr(oauth, f"org_{org}")
    try:
        tok = await client.authorize_access_token(request)
    except OAuthError as e:
        return error(f"OIDC error: {e.error}", 400)

    claims = tok.get("userinfo") or {}
    email = (claims.get("email") or "").strip().lower()
    if "@" not in email:
        return error("The identity provider returned no email address.")
    if not ALLOW_ANY_DOMAIN and email.rsplit("@", 1)[1] not in cfg["domains"]:
        log.warning("domain mismatch: %s via org %s", email, org)
        return error(f"{email} is not an allowed address for {cfg['label']}.")
    if not claims.get("email_verified"):
        return error("Email address is not verified at the identity provider.")
    if email == ADMIN_EMAIL:
        return error("This address is reserved.")

    first = claims.get("given_name") or email.split("@")[0]
    last = claims.get("family_name") or "-"
    pw = secrets.token_urlsafe(24) + "Aa1!"  # random, never shown; rotated every login

    async with user_lock(email):
        try:
            user = await find_user(email)
            if user is None:
                user = await mb.admin(
                    "POST", "/api/user", json={"email": email, "first_name": first, "last_name": last, "password": pw}
                )
            elif not user.get("is_active", True):
                await mb.admin("PUT", f"/api/user/{user['id']}/reactivate")
            await mb.admin("PUT", f"/api/user/{user['id']}/password", json={"password": pw})
            claim_groups = claims.get("groups") or []
            if kc and claims.get("sub") and KC_MARKER_GROUP not in claim_groups:
                await mirror_metabase_groups_to_keycloak(claims["sub"], user["id"], cfg["groups"])
            await sync_groups(user["id"], cfg["groups"], claim_groups)
            r = await mb.http.post("/api/session", json={"username": email, "password": pw})
            r.raise_for_status()
            sid = r.json()["id"]
        except httpx.HTTPError as e:
            log.exception("Metabase API error")
            return error(f"Metabase API error: {e}", 502)

    request.session["org"] = org
    request.session["id_token"] = tok.get("id_token")
    resp = RedirectResponse(f"{PUBLIC_URL}/", status_code=302)
    resp.set_cookie(
        "metabase.SESSION", sid, httponly=True, secure=COOKIE_SECURE, samesite="lax", max_age=SESSION_MAX_AGE, path="/"
    )
    log.info("login ok: %s via %s", email, org)
    return resp


@app.get("/sso/logout")
async def logout(request: Request):
    org = request.session.get("org")
    id_token = request.session.get("id_token")
    sid = request.cookies.get("metabase.SESSION")
    request.session.clear()
    if sid:  # best effort: invalidate the Metabase session too
        try:
            await mb.http.delete("/api/session", headers={"X-Metabase-Session": sid, "Cookie": f"metabase.SESSION={sid}"})
        except httpx.HTTPError:
            pass
    target = f"{PUBLIC_URL}/sso/"
    if org in ORGS:
        meta = await getattr(oauth, f"org_{org}").load_server_metadata()
        end = meta.get("end_session_endpoint")
        if end:
            params = {"post_logout_redirect_uri": f"{PUBLIC_URL}/sso/", "client_id": ORGS[org]["client_id"]}
            if id_token:
                params["id_token_hint"] = id_token
            target = f"{end}?{urlencode(params)}"
    resp = RedirectResponse(target, status_code=302)
    resp.delete_cookie("metabase.SESSION", path="/")
    return resp
