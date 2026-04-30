import os
import time
import logging
import re
import hashlib
import hmac
import base64
import uuid
import json
import shlex
import shutil
import subprocess
import threading
import requests
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request as UrlRequest
from urllib.error import URLError, HTTPError
from dotenv import load_dotenv
from supabase import create_client
from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Some Windows environments inject dead/broken HTTP proxy variables that make
# Supabase requests fail with WinError 10061/10035. By default we bypass env proxy
# for this gateway process; can be disabled with SUPABASE_BYPASS_PROXY=0.
if os.environ.get("SUPABASE_BYPASS_PROXY", "1") != "0":
    for _proxy_key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        os.environ.pop(_proxy_key, None)

app = FastAPI()
logger = logging.getLogger("v12-gateway")
LOG_LEVEL = str(os.environ.get("LOG_LEVEL", "INFO")).upper()


def sanitize_log_text(text):
    safe = str(text or "")
    replacements = [
        (r"Bearer\s+[A-Za-z0-9\-\._~\+/=]+", "Bearer [redacted]"),
        (r"(access_token['\"=:\s]+)[^,\s\}\]]+", r"\1[redacted]"),
        (r"(refresh_token['\"=:\s]+)[^,\s\}\]]+", r"\1[redacted]"),
        (r"(SUPABASE_KEY=)[^\s]+", r"\1[redacted]"),
        (r"(ADMIN_CREDIT_GRANT_KEY=)[^\s]+", r"\1[redacted]"),
        (r"(AUTH_SECRET=)[^\s]+", r"\1[redacted]"),
        (r"(AUTH_JWT_SECRET=)[^\s]+", r"\1[redacted]"),
        (r"https://[A-Za-z0-9\-]+\.supabase\.co", "https://[supabase-host]"),
        (r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b", "[uuid]"),
        (r"(miner_name[='\":\s]+)([A-Za-z0-9._:-]+)", r"\1[miner]"),
        (r"(hwid[='\":\s]+)([A-Za-z0-9._:-]+)", r"\1[device]"),
    ]
    for pattern, replacement in replacements:
        safe = re.sub(pattern, replacement, safe, flags=re.IGNORECASE)
    return safe


class SafeFormatter(logging.Formatter):
    def format(self, record):
        rendered = super().format(record)
        return sanitize_log_text(rendered)


logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
for _handler in logging.getLogger().handlers:
    _handler.setFormatter(SafeFormatter("%(asctime)s %(levelname)s:%(name)s:%(message)s"))

for _noisy_name in ("httpx", "httpcore", "urllib3", "postgrest", "supabase"):
    logging.getLogger(_noisy_name).setLevel(logging.WARNING)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_trace_middleware(request: Request, call_next):
    request_id = str(request.headers.get("x-request-id") or uuid.uuid4())
    request.state.request_id = request_id
    started_at = time.time()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "request_failed req_id=%s method=%s path=%s",
            request_id,
            request.method,
            request.url.path,
        )
        raise
    elapsed_ms = (time.time() - started_at) * 1000.0
    with _http_metrics_lock:
        _http_status_counters[str(getattr(response, "status_code", "unknown"))] += 1
        _http_path_counters[str(request.url.path or "")] += 1
    response.headers["x-request-id"] = request_id
    logger.info(
        "request_done req_id=%s method=%s path=%s status=%s elapsed_ms=%.1f",
        request_id,
        request.method,
        request.url.path,
        getattr(response, "status_code", "unknown"),
        elapsed_ms,
    )
    return response


def enforce_daily_task_create_limit(user_id: str):
    if CREATE_TASK_USER_DAILY_LIMIT <= 0:
        return True, 0
    normalized = normalize_user_id_for_storage(user_id)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    with _daily_limit_lock:
        bucket = _daily_task_create_state[normalized]
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= CREATE_TASK_USER_DAILY_LIMIT:
            return False, len(bucket)
        bucket.append(now)
        return True, len(bucket)

URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")
SUPABASE_VERIFY_KEY = SUPABASE_ANON_KEY or KEY
CONTROL_PLANE_BASE_URL = str(os.environ.get("CONTROL_PLANE_BASE_URL") or "").strip().rstrip("/")
CONTROL_PLANE_TIMEOUT_SECONDS = float(os.environ.get("CONTROL_PLANE_TIMEOUT_SECONDS", "30"))
TASK_LEASE_TIMEOUT_SECONDS = int(os.environ.get("TASK_LEASE_TIMEOUT_SECONDS", "1200"))
STALE_RECOVERY_INTERVAL_SECONDS = int(os.environ.get("STALE_RECOVERY_INTERVAL_SECONDS", "15"))
LOCAL_MINER_PROFILE_URL = os.environ.get("LOCAL_MINER_PROFILE_URL", "http://127.0.0.1:8765/miner-profile")
OLLAMA_COMMAND_TIMEOUT_SECONDS = int(os.environ.get("OLLAMA_COMMAND_TIMEOUT_SECONDS", "1800"))
LOCAL_OLLAMA_READ_TIMEOUT_SECONDS = int(os.environ.get("LOCAL_OLLAMA_READ_TIMEOUT_SECONDS", "600"))
OLLAMA_WINDOWS_INSTALLER_URL = os.environ.get(
    "OLLAMA_WINDOWS_INSTALLER_URL",
    "https://ollama.com/download/OllamaSetup.exe",
)
RUNTIME_ROOT_DIR = os.path.abspath(os.environ.get("APP_RUNTIME_ROOT") or os.path.join(BASE_DIR, "..", ".."))
OPS_RUNTIME_DIR = os.path.join(RUNTIME_ROOT_DIR, "ops", "runtime")
APP_DATA_DIR = os.path.join(os.environ.get("LOCALAPPDATA") or RUNTIME_ROOT_DIR, "AIInferencePlatform")
APP_DOWNLOAD_DIR = os.path.join(APP_DATA_DIR, "downloads")
LOCAL_HISTORY_MESSAGES = int(os.environ.get("LOCAL_HISTORY_MESSAGES", "3"))
LOCAL_HISTORY_CONTENT_CHARS = int(os.environ.get("LOCAL_HISTORY_CONTENT_CHARS", "300"))
LOCAL_NORMAL_NUM_PREDICT = int(os.environ.get("LOCAL_NORMAL_NUM_PREDICT", "2048"))
LOCAL_DEEP_NUM_PREDICT = int(os.environ.get("LOCAL_DEEP_NUM_PREDICT", "1200"))
LOCAL_OLLAMA_NUM_CTX = int(os.environ.get("LOCAL_OLLAMA_NUM_CTX", "2048"))
LOCAL_OLLAMA_NUM_BATCH = int(os.environ.get("LOCAL_OLLAMA_NUM_BATCH", "32"))
OLLAMA_MODEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}(:[A-Za-z0-9][A-Za-z0-9._-]{0,63})?$")
AUTH_TOKEN_TTL_SECONDS = int(os.environ.get("AUTH_TOKEN_TTL_SECONDS", str(60 * 60 * 24 * 30)))
DEFAULT_USER_CREDITS = float(os.environ.get("DEFAULT_USER_CREDITS", "0.0"))
MESSAGE_CREDIT_COST = float(os.environ.get("MESSAGE_CREDIT_COST", "0.1"))
MINER_REWARD_CREDIT = float(os.environ.get("MINER_REWARD_CREDIT", "0.1"))
ADMIN_CREDIT_GRANT_KEY = str(os.environ.get("ADMIN_CREDIT_GRANT_KEY") or "").strip()

DEFAULT_MODELS = ["qwen3.5:2b", "qwen3.5:9b", "qwen3.5:27b"]

ACTIVE_TASK_STATUSES = ["pending", "claimed", "processing"]
TERMINAL_TASK_STATUSES = ["completed", "failed", "cancelled"]
STALE_REQUEUE_REASON_CODE = "stale_lease_timeout"
ALERT_PENDING_BACKLOG_THRESHOLD = int(os.environ.get("ALERT_PENDING_BACKLOG_THRESHOLD", "20"))
ALERT_CLAIM_TIMEOUT_THRESHOLD = int(os.environ.get("ALERT_CLAIM_TIMEOUT_THRESHOLD", "1"))
CLAIM_USE_RPC = str(os.environ.get("CLAIM_USE_RPC", "0")).lower() in ("1", "true", "yes", "on")
CREATE_TASK_USER_DAILY_LIMIT = int(os.environ.get("CREATE_TASK_USER_DAILY_LIMIT", "300"))
ENABLE_OPS_METRICS = str(os.environ.get("ENABLE_OPS_METRICS", "1")).lower() in ("1", "true", "yes", "on")
_gateway_started_at = datetime.now(timezone.utc)

_last_stale_recovery_at = 0.0
_stale_recovery_enabled = True
_rate_limit_lock = threading.Lock()
_rate_limit_state = defaultdict(deque)
_daily_limit_lock = threading.Lock()
_daily_task_create_state = defaultdict(deque)
_schema_missing_lock = threading.Lock()
_schema_missing_columns_by_table = defaultdict(set)
_http_metrics_lock = threading.Lock()
_http_status_counters = defaultdict(int)
_http_path_counters = defaultdict(int)

# In-memory SSE stream cache for local tasks to avoid Supabase round-trips
_task_stream_queues = {}  # task_id -> asyncio.Queue
_task_stream_lock = threading.Lock()
_task_stream_ttl_seconds = 600  # Keep streams for 10 minutes after completion
_local_task_records = {}
_local_task_lock = threading.Lock()
SUPABASE_RETRY_ATTEMPTS = int(os.environ.get("SUPABASE_RETRY_ATTEMPTS", "3"))
SUPABASE_RETRY_BASE_DELAY_SECONDS = float(os.environ.get("SUPABASE_RETRY_BASE_DELAY_SECONDS", "0.25"))

if not URL or not KEY:
    supabase = None
else:
    supabase = create_client(URL, KEY)

AUTH_SECRET = os.environ.get("AUTH_SECRET") or hashlib.sha256(str(KEY or "dev-secret").encode("utf-8")).hexdigest()
MINER_API_KEY = str(os.environ.get("MINER_API_KEY") or "").strip()
SUPABASE_AUTH_TIMEOUT_SECONDS = int(os.environ.get("SUPABASE_AUTH_TIMEOUT_SECONDS", "10"))
ALLOW_LEGACY_AUTH_SESSION = str(os.environ.get("ALLOW_LEGACY_AUTH_SESSION", "0")).lower() in ("1", "true", "yes", "on")


def error_response(code: str, message: str, **extra):
    payload = {"status": "error", "message": message, "code": code}
    payload.update(extra)
    return payload


def internal_error(endpoint: str, exc: Exception):
    logger.exception("Unhandled error at %s", endpoint)
    return error_response("internal_error", "Internal server error")


def has_control_plane_proxy() -> bool:
    return bool(CONTROL_PLANE_BASE_URL)


def build_default_credit_summary(user_id: str):
    normalized_uid = normalize_user_id_for_storage(user_id)
    return {
        "user_id": normalized_uid,
        "total": DEFAULT_USER_CREDITS,
        "spent": 0.0,
        "reserved": 0.0,
        "available": DEFAULT_USER_CREDITS,
        "tasks": {"completed": 0, "failed": 0, "cancelled": 0, "active": 0},
    }


def proxy_control_plane_json(request: Request, path: str, method: str = "GET", json_body=None, params=None):
    if not has_control_plane_proxy():
        return error_response("service_unavailable", "Control plane unavailable")
    try:
        target = f"{CONTROL_PLANE_BASE_URL}{path}"
        headers = {}
        auth_header = str(request.headers.get("authorization") or "").strip()
        if auth_header:
            headers["Authorization"] = auth_header
        request_id = str(getattr(request.state, "request_id", "") or request.headers.get("x-request-id") or "")
        if request_id:
            headers["x-request-id"] = request_id
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        response = requests.request(
            method.upper(),
            target,
            headers=headers,
            json=json_body,
            params=params,
            timeout=CONTROL_PLANE_TIMEOUT_SECONDS,
        )
        if not response.content:
            return {"status": "error", "message": f"Empty control plane response: HTTP {response.status_code}", "code": "control_plane_error"}
        try:
            return response.json()
        except Exception:
            return error_response("control_plane_error", f"Invalid control plane response: HTTP {response.status_code}")
    except Exception as e:
        logger.warning("control plane proxy failed path=%s: %s", path, e)
        return error_response("service_unavailable", "Control plane request failed")


def get_client_ip(request: Request) -> str:
    try:
        xff = request.headers.get("x-forwarded-for", "")
        if xff:
            return xff.split(",")[0].strip() or "unknown"
        return (request.client.host if request.client else "unknown") or "unknown"
    except Exception:
        return "unknown"


def rate_limit_guard(request: Request, scope: str, limit: int, window_seconds: int):
    now = time.time()
    key = f"{scope}:{get_client_ip(request)}"
    with _rate_limit_lock:
        bucket = _rate_limit_state[key]
        cutoff = now - window_seconds
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= limit:
            retry_after = max(1, int(window_seconds - (now - bucket[0])))
            return False, retry_after
        bucket.append(now)
        return True, 0


def is_transient_supabase_error(exc: Exception) -> bool:
    msg = str(exc or "")
    lowered = msg.lower()
    transient_markers = [
        "readerror",
        "connecterror",
        "timeouterror",
        "timed out",
        "connection reset",
        "connection aborted",
        "winerror 10035",
        "winerror 10054",
        "winerror 10060",
    ]
    return any(marker in lowered for marker in transient_markers)


def supabase_execute_with_retry(run_query):
    attempts = max(1, SUPABASE_RETRY_ATTEMPTS)
    for idx in range(attempts):
        try:
            return run_query()
        except Exception as e:
            if idx >= attempts - 1 or not is_transient_supabase_error(e):
                raise
            delay = SUPABASE_RETRY_BASE_DELAY_SECONDS * (2 ** idx)
            logger.warning("supabase transient error, retrying in %.2fs (%s)", delay, e)
            time.sleep(delay)


def normalize_user_id_for_storage(raw_user_id):
    raw = str(raw_user_id or "").strip()
    if not raw:
        return str(uuid.uuid4())
    try:
        return str(uuid.UUID(raw))
    except Exception:
        # Stable mapping for non-UUID user ids to satisfy uuid column constraints.
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"user:{raw}"))


def b64url_encode(raw_bytes: bytes) -> str:
    return base64.urlsafe_b64encode(raw_bytes).decode("utf-8").rstrip("=")


def b64url_decode(raw_text: str) -> bytes:
    padded = raw_text + "=" * ((4 - len(raw_text) % 4) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def issue_auth_token(user_id: str, ttl_seconds: int = AUTH_TOKEN_TTL_SECONDS) -> str:
    payload = {
        "uid": normalize_user_id_for_storage(user_id),
        "exp": int(time.time()) + max(60, int(ttl_seconds)),
        "ver": 1,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload_b64 = b64url_encode(payload_bytes)
    sig = hmac.new(AUTH_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_b64}.{b64url_encode(sig)}"


def verify_auth_token(token: str):
    try:
        token = str(token or "").strip()
        if "." not in token:
            return None
        payload_b64, sig_b64 = token.split(".", 1)
        expected = hmac.new(AUTH_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
        actual = b64url_decode(sig_b64)
        if not hmac.compare_digest(expected, actual):
            return None
        payload = json.loads(b64url_decode(payload_b64).decode("utf-8"))
        exp = int(payload.get("exp") or 0)
        uid = str(payload.get("uid") or "").strip()
        if not uid or exp <= int(time.time()):
            return None
        return {"uid": normalize_user_id_for_storage(uid), "exp": exp}
    except Exception:
        return None


def extract_auth_bearer(request: Request) -> str:
    auth = str(request.headers.get("Authorization") or "")
    if not auth.lower().startswith("bearer "):
        return ""
    return auth[7:].strip()


def require_auth_user_id(request: Request):
    token = extract_auth_bearer(request)
    verified = verify_auth_token(token)
    if not verified:
        supabase_user = verify_supabase_access_token(token)
        if not supabase_user:
            return None, {"status": "error", "message": "Unauthorized", "code": "unauthorized"}
        return normalize_user_id_for_storage(supabase_user.get("id") or ""), None
    return verified["uid"], None


def verify_supabase_access_token(access_token: str):
    token = str(access_token or "").strip()
    if not token or not URL or not SUPABASE_VERIFY_KEY:
        return None
    try:
        resp = requests.get(
            f"{str(URL).rstrip('/')}/auth/v1/user",
            headers={"apikey": SUPABASE_VERIFY_KEY, "Authorization": f"Bearer {token}"},
            timeout=SUPABASE_AUTH_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            return None
        data = resp.json() if resp.content else {}
        user_id = str(data.get("id") or "").strip()
        if not user_id:
            return None
        return data
    except Exception:
        return None


def require_miner_auth(request: Request):
    # Optional hardening: when MINER_API_KEY is configured, miners must provide X-Miner-Key.
    if not MINER_API_KEY:
        return None
    incoming = str(request.headers.get("X-Miner-Key") or "").strip()
    if not incoming or not hmac.compare_digest(incoming, MINER_API_KEY):
        return {"status": "error", "message": "Unauthorized miner", "code": "unauthorized_miner"}
    return None


def estimate_credits_from_payload(prompt: str = "", model_name: str = "", deep_think: bool = False) -> float:
    # Internal test billing policy: fixed per-message reservation/charge.
    return float(round(max(0.0, MESSAGE_CREDIT_COST), 4))


def extract_billing(context_obj):
    context = context_obj if isinstance(context_obj, dict) else {}
    billing = context.get("billing") if isinstance(context.get("billing"), dict) else {}
    reserved = float(billing.get("reserved") or 0.0)
    charged = float(billing.get("charged") or 0.0)
    refunded = float(billing.get("refunded") or 0.0)
    state = str(billing.get("state") or "unknown")
    return {
        "reserved": max(0.0, reserved),
        "charged": max(0.0, charged),
        "refunded": max(0.0, refunded),
        "state": state,
    }


def is_remote_execution(task_context) -> bool:
    ctx = task_context if isinstance(task_context, dict) else {}
    # Single source of truth for billing: only execution_mode controls charging.
    mode = str(ctx.get("execution_mode") or "").strip().lower()
    return mode == "remote"


def compute_user_credit_summary(user_id: str):
    normalized_uid = normalize_user_id_for_storage(user_id)
    rows = select_rows_with_schema_fallback(
        "tasks",
        ["id", "status", "model", "deep_think", "context", "created_at", "completed_at", "miner_name"],
        filters=[{"op": "eq", "col": "user_id", "val": normalized_uid}],
        limit=2000,
        order_by="created_at",
        ascending=False,
    )
    spent = 0.0
    reserved = 0.0
    granted = 0.0
    miner_earned = 0.0
    completed = 0
    failed = 0
    cancelled = 0
    pending_like = 0
    for row in rows:
        status = str(row.get("status") or "").lower()
        context = row.get("context") if isinstance(row.get("context"), dict) else {}
        billing = extract_billing(context)
        remote_execution = is_remote_execution(context)
        if status == "completed":
            completed += 1
            if not remote_execution:
                continue
            if billing["state"] == "charged":
                spent += billing["charged"]
            else:
                spent += estimate_credits_from_payload("", row.get("model") or context.get("model") or "", bool(row.get("deep_think")))
        elif status == "failed":
            failed += 1
        elif status == "cancelled":
            cancelled += 1
        else:
            pending_like += 1
            if remote_execution and billing["state"] in {"reserved", "pending"}:
                reserved += billing["reserved"]
    ledger_rows = []
    try:
        ledger_rows = select_rows_with_schema_fallback(
            "credit_ledger",
            ["phase", "direction", "amount", "actor_type", "actor_id", "created_at", "note", "task_id"],
            filters=[
                {"op": "eq", "col": "actor_type", "val": "user"},
                {"op": "eq", "col": "actor_id", "val": normalized_uid},
            ],
            limit=2000,
            order_by="created_at",
            ascending=False,
        )
    except Exception as e:
        logger.warning("credit_ledger query failed in compute_user_credit_summary, fallback without grants: %s", e)
        ledger_rows = []
    for row in ledger_rows:
        phase = str(row.get("phase") or "").lower()
        direction = str(row.get("direction") or "").lower()
        amount = round(max(0.0, float(row.get("amount") or 0.0)), 4)
        if direction == "credit" and phase in {"grant", "airdrop", "campaign_reward", "event_reward", "manual_adjustment"}:
            granted += amount

    # Miner rewards are recorded under actor_type=miner. For desktop self-host flow,
    # bind them back to the user credit view by effective miner identity.
    miner_identities = set()
    effective_miner_name = str(derive_effective_miner_name_for_user(normalized_uid) or "").strip()
    if effective_miner_name:
        miner_identities.add(effective_miner_name)
    # Fallback: include recently observed miner names on this user's own tasks.
    for row in rows[:120]:
        candidate = str((row or {}).get("miner_name") or "").strip()
        if candidate:
            miner_identities.add(candidate)

    if miner_identities:
        try:
            miner_ledger_rows = select_rows_with_schema_fallback(
                "credit_ledger",
                ["phase", "direction", "amount", "actor_type", "actor_id", "created_at", "note", "task_id"],
                filters=[
                    {"op": "eq", "col": "actor_type", "val": "miner"},
                    {"op": "in", "col": "actor_id", "val": list(miner_identities)},
                ],
                limit=2000,
                order_by="created_at",
                ascending=False,
            )
            for row in miner_ledger_rows:
                phase = str(row.get("phase") or "").lower()
                direction = str(row.get("direction") or "").lower()
                amount = round(max(0.0, float(row.get("amount") or 0.0)), 4)
                if direction == "credit" and phase in {"rewarded", "reward", "miner_reward"}:
                    miner_earned += amount
        except Exception as e:
            logger.warning("miner reward query failed in compute_user_credit_summary: %s", e)

    total = float(DEFAULT_USER_CREDITS) + granted + miner_earned
    available = max(0.0, round(total - spent - reserved, 4))
    return {
        "user_id": normalized_uid,
        "total": round(total, 4),
        "granted": round(granted, 4),
        "miner_earned": round(miner_earned, 4),
        "spent": round(spent, 4),
        "reserved": round(reserved, 4),
        "available": round(available, 4),
        "tasks": {
            "completed": completed,
            "failed": failed,
            "cancelled": cancelled,
            "active": pending_like,
        },
    }


def build_billing_context_on_settle(task_context, *, next_state: str, charged: float = 0.0, refunded: float = 0.0):
    base_context = dict(task_context) if isinstance(task_context, dict) else {}
    current = extract_billing(base_context)
    current_billing = base_context.get("billing") if isinstance(base_context.get("billing"), dict) else {}
    current_events = current_billing.get("events") if isinstance(current_billing.get("events"), list) else []
    billing = {
        "reserved": current["reserved"],
        "charged": max(0.0, float(charged)),
        "refunded": max(0.0, float(refunded)),
        "state": next_state,
        "settled_at": datetime.now(timezone.utc).isoformat(),
        "events": list(current_events)[-200:],
    }
    if next_state == "charged":
        billing["charged"] = max(billing["charged"], current["reserved"])
    if next_state in {"refunded", "cancelled"}:
        billing["refunded"] = max(billing["refunded"], current["reserved"])
        billing["charged"] = 0.0
    base_context["billing"] = billing
    return base_context


def append_billing_event(
    task_context,
    *,
    phase: str,
    direction: str,
    amount: float,
    actor_type: str,
    actor_id: str = "",
    note: str = "",
):
    base_context = dict(task_context) if isinstance(task_context, dict) else {}
    billing = base_context.get("billing") if isinstance(base_context.get("billing"), dict) else {}
    events = billing.get("events") if isinstance(billing.get("events"), list) else []
    events.append(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "phase": str(phase or "").strip() or "unknown",
            "direction": str(direction or "").strip() or "unknown",
            "amount": round(max(0.0, float(amount or 0.0)), 4),
            "actor_type": str(actor_type or "").strip() or "unknown",
            "actor_id": str(actor_id or "").strip(),
            "note": str(note or "").strip()[:240],
        }
    )
    billing["events"] = events[-200:]
    base_context["billing"] = billing
    return base_context


_credit_ledger_unavailable = False


def record_credit_ledger_event(task_id: str, phase: str, direction: str, amount: float, actor_type: str, actor_id: str, note: str = ""):
    global _credit_ledger_unavailable
    if not supabase or _credit_ledger_unavailable:
        return
    normalized_direction = str(direction or "").strip().lower()
    if normalized_direction.startswith("debit"):
        normalized_direction = "debit"
    elif normalized_direction.startswith("credit"):
        normalized_direction = "credit"
    else:
        normalized_direction = "unknown"
    payload = {
        "id": str(uuid.uuid4()),
        "task_id": task_id,
        "phase": str(phase or "").strip() or "unknown",
        "direction": normalized_direction,
        "amount": round(max(0.0, float(amount or 0.0)), 4),
        "actor_type": str(actor_type or "").strip() or "unknown",
        "actor_id": str(actor_id or "").strip(),
        "note": str(note or "").strip()[:240],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        insert_row_with_schema_fallback(
            "credit_ledger",
            payload,
            optional_drop_order=["id", "note", "created_at"],
        )
    except Exception as e:
        # Keep business flow alive even when ledger table is missing.
        _credit_ledger_unavailable = True
        logger.warning("credit_ledger unavailable, fall back to task context audit only: %s", e)


def require_admin_credit_grant(request: Request):
    if not ADMIN_CREDIT_GRANT_KEY:
        return error_response("service_unavailable", "Admin credit grant key missing")
    incoming = str(request.headers.get("x-admin-credit-key") or "").strip()
    if not incoming or not hmac.compare_digest(incoming, ADMIN_CREDIT_GRANT_KEY):
        return error_response("forbidden", "Invalid admin credit grant key")
    return None


def grant_credit_to_user(user_id, amount, phase="grant", note="", admin_actor="admin"):
    normalized_user_id = normalize_user_id_for_storage(user_id)
    if not normalized_user_id:
        raise ValueError("Missing user_id")

    try:
        normalized_amount = round(max(0.0, float(amount or 0.0)), 4)
    except Exception:
        raise ValueError("Invalid amount")
    if normalized_amount <= 0:
        raise ValueError("Amount must be greater than 0")

    normalized_phase = str(phase or "grant").strip().lower()
    if normalized_phase not in {"grant", "airdrop", "campaign_reward", "event_reward", "manual_adjustment"}:
        raise ValueError("Unsupported phase")

    normalized_note = str(note or "").strip()[:240]
    normalized_admin_actor = str(admin_actor or "admin").strip()[:120] or "admin"
    task_id = f"credit-{normalized_phase}-{uuid.uuid4()}"

    record_credit_ledger_event(
        task_id=task_id,
        phase=normalized_phase,
        direction="credit",
        amount=normalized_amount,
        actor_type="user",
        actor_id=normalized_user_id,
        note=normalized_note,
    )

    return {
        "task_id": task_id,
        "grant": {
            "user_id": normalized_user_id,
            "amount": normalized_amount,
            "phase": normalized_phase,
            "note": normalized_note,
            "admin_id": normalized_admin_actor,
        },
        "credits": compute_user_credit_summary(normalized_user_id),
    }


def fetch_local_miner_profile():
    req = UrlRequest(LOCAL_MINER_PROFILE_URL, headers={"Accept": "application/json"})
    with urlopen(req, timeout=1.5) as response:
        raw = response.read().decode("utf-8")
    payload = json.loads(raw)
    profile = payload.get("profile") if isinstance(payload, dict) else None
    if payload.get("status") != "success" or not isinstance(profile, dict):
        raise ValueError(payload.get("message") or "Local miner profile failed")
    return profile


def derive_effective_miner_name_for_user(user_id: str):
    normalized_uid = normalize_user_id_for_storage(user_id)
    if not normalized_uid:
        return ""
    try:
        rows = select_rows_with_schema_fallback(
            "tasks",
            ["miner_name", "created_at"],
            filters=[{"op": "eq", "col": "user_id", "val": normalized_uid}],
            limit=20,
            order_by="created_at",
            ascending=False,
        )
        for row in rows:
            candidate = str((row or {}).get("miner_name") or "").strip()
            if candidate:
                return candidate
        return ""
    except Exception:
        return ""


def validate_ollama_model_name(model_name):
    model = str(model_name or "").strip()
    if not model:
        raise ValueError("Missing model name")
    if len(model) > 160:
        raise ValueError("Model name is too long")
    if model.startswith("-") or ".." in model or "\\" in model:
        raise ValueError("Unsafe model name")
    if not OLLAMA_MODEL_RE.match(model):
        raise ValueError("Invalid model name")
    return model


def parse_allowed_ollama_command(data):
    command = str(data.get("command") or "").strip()
    if command:
        parts = shlex.split(command, posix=False)
        if len(parts) < 2 or parts[0].lower() != "ollama":
            raise ValueError("Only ollama commands are allowed")
        action = parts[1].lower()
        if action == "list" and len(parts) == 2:
            return "list", ""
        if action in {"pull", "rm"} and len(parts) == 3:
            return action, validate_ollama_model_name(parts[2])
        raise ValueError("Allowed commands: ollama list, ollama pull <model>, ollama rm <model>")

    action = str(data.get("action") or "").strip().lower()
    if action == "list":
        return "list", ""
    if action in {"pull", "rm"}:
        return action, validate_ollama_model_name(data.get("model"))
    raise ValueError("Allowed actions: list, pull, rm")


def run_allowed_ollama_command(action, model_name=""):
    ollama_bin = shutil.which("ollama")
    if not ollama_bin:
        bundled = os.path.join(RUNTIME_ROOT_DIR, "ollama", "ollama.exe")
        if os.path.exists(bundled):
            ollama_bin = bundled
    if not ollama_bin:
        raise RuntimeError("ollama executable not found in PATH")

    if action == "list":
        args = [ollama_bin, "list"]
    elif action in {"pull", "rm"}:
        args = [ollama_bin, action, validate_ollama_model_name(model_name)]
    else:
        raise ValueError("Unsupported ollama action")

    completed = subprocess.run(
        args,
        shell=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=OLLAMA_COMMAND_TIMEOUT_SECONDS,
    )
    return {
        "command": " ".join(["ollama", action] + ([model_name] if model_name else [])),
        "exit_code": completed.returncode,
        "stdout": completed.stdout[-12000:],
        "stderr": completed.stderr[-12000:],
    }


def parse_ollama_list_stdout(output):
    models = []
    for line in str(output or "").splitlines():
        parts = line.split()
        if not parts or parts[0] == "NAME":
            continue
        models.append(parts[0])
    return models


def list_local_ollama_models():
    result = run_allowed_ollama_command("list")
    if result["exit_code"] != 0:
        return []
    return parse_ollama_list_stdout(result.get("stdout") or "")


def local_ollama_has_model(model_name):
    wanted = str(model_name or "").strip().lower()
    if not wanted:
        return False
    try:
        available = set([m.lower() for m in list_local_ollama_models()])
        # Try exact match first
        if wanted in available:
            return True
        # Try prefix match (e.g., "qwen3.5" matches "qwen3.5:27b")
        for avail in available:
            if avail.startswith(wanted + ":") or wanted.startswith(avail + ":"):
                return True
        return False
    except Exception as e:
        logger.warning("local_ollama_has_model failed for %s: %s", wanted, e)
        return False


def is_ollama_service_ready():
    try:
        response = requests.get("http://127.0.0.1:11434/api/tags", timeout=2)
        return 200 <= response.status_code < 500
    except Exception:
        return False


def read_ollama_source():
    source_file = os.path.join(OPS_RUNTIME_DIR, "ollama.source")
    try:
        with open(source_file, "r", encoding="utf-8", errors="replace") as handle:
            return handle.read().strip() or ""
    except Exception:
        return ""


def get_ollama_runtime_info():
    system_ollama = shutil.which("ollama")
    bundled_ollama = os.path.join(RUNTIME_ROOT_DIR, "ollama", "ollama.exe")
    service_ready = is_ollama_service_ready()
    source = read_ollama_source()
    if service_ready and not source:
        source = "external"
    return {
        "status": "success",
        "service_ready": service_ready,
        "source": source or "missing",
        "system_installed": bool(system_ollama),
        "system_path": system_ollama or "",
        "bundled_available": os.path.exists(bundled_ollama),
        "bundled_path": bundled_ollama if os.path.exists(bundled_ollama) else "",
    }


def download_and_launch_ollama_installer(force=False):
    if os.name != "nt":
        raise RuntimeError("Ollama installer automation is only available on Windows")
    info = get_ollama_runtime_info()
    if not force and (info.get("system_installed") or info.get("service_ready")):
        return {**info, "installer_started": False, "message": "Ollama is already available"}

    os.makedirs(APP_DOWNLOAD_DIR, exist_ok=True)
    installer_path = os.path.join(APP_DOWNLOAD_DIR, "OllamaSetup.exe")
    tmp_path = installer_path + ".tmp"

    with requests.get(OLLAMA_WINDOWS_INSTALLER_URL, stream=True, timeout=(10, 120)) as response:
        response.raise_for_status()
        with open(tmp_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    os.replace(tmp_path, installer_path)

    try:
        os.startfile(installer_path)  # type: ignore[attr-defined]
    except Exception:
        subprocess.Popen([installer_path], shell=False)
    return {
        **get_ollama_runtime_info(),
        "installer_started": True,
        "installer_path": installer_path,
        "message": "Ollama installer started",
    }


def build_universal_llm_protocol(deep_think):
    if deep_think:
        return (
            "/think\n"
            "You are using DEEP THINK mode.\n"
            "Use the same language as the user's latest message.\n"
            "You must output exactly two tagged sections and nothing else:\n"
            "<think>\n"
            "Write concise but complete reasoning. For simple questions, keep this very short. "
            "For complex questions, reason enough to be useful.\n"
            "</think>\n"
            "<answer>\n"
            "Write the final answer only.\n"
            "</answer>\n"
            "Do not repeat system prompts, prior hidden text, continuation instructions, or meta commentary."
        )
    return (
        "/no_think\n"
        "Use the same language as the user.\n"
        "Output only the final answer in this format:\n"
        "<answer>\n"
        "Your answer here\n"
        "</answer>"
    )


def extract_tag_content(text, tag):
    raw = str(text or "")
    match = re.search(rf"<{tag}>(.*?)(?:</{tag}>|$)", raw, flags=re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else ""


def normalize_model_output(text, deep_think):
    raw = str(text or "").strip()
    if not raw:
        return ""
    think = extract_tag_content(raw, "think")
    answer = extract_tag_content(raw, "answer")
    if deep_think:
        if answer or think:
            return f"<think>{think}</think><answer>{answer}</answer>"
        return f"<think></think><answer>{re.sub(r'</?(?:think|answer)>', '', raw, flags=re.IGNORECASE).strip()}</answer>"
    if answer:
        return f"<answer>{answer}</answer>"
    if "</think>" in raw.lower():
        tail = re.split(r"</think>", raw, flags=re.IGNORECASE)[-1]
        tail = re.sub(r"</?(?:think|answer)>", "", tail, flags=re.IGNORECASE).strip()
        return f"<answer>{tail}</answer>"
    cleaned = re.sub(r"<think>.*?(?:</think>|$)", "", raw, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"</?(?:think|answer)>", "", cleaned, flags=re.IGNORECASE).strip()
    return f"<answer>{cleaned}</answer>"


def looks_like_reasoning_leak(text):
    sample = str(text or "").strip()[:320].lower()
    if not sample:
        return False
    return bool(
        re.match(
            r"^(okay|ok,|first,|let me|i need to|i should|we need to|hmm|wait,|the user|user asked|thinking process|analysis:|current user request|continue in the same language as before|the previous responses|the assistant has been replying)",
            sample,
        )
    )


def extract_fallback_answer_from_leak(text):
    raw = str(text or "").strip()
    if not raw:
        return ""
    answer = extract_tag_content(raw, "answer")
    if answer:
        return answer
    lines = [
        line.strip()
        for line in re.split(r"\n+", raw)
        if line and line.strip()
    ]
    lines = [
        line for line in lines
        if not looks_like_reasoning_leak(line)
        and not re.match(r"^(the user|current user|possible response|the key points|the instruction|wait,|but the instruction)", line.strip(), flags=re.IGNORECASE)
    ]
    short_cjk = next((line for line in reversed(lines) if re.search(r"[\u4e00-\u9fff]", line) and len(line) <= 80), "")
    if short_cjk:
        return re.sub(r"</?(?:think|answer)>", "", short_cjk, flags=re.IGNORECASE).strip()
    short_line = next((line for line in reversed(lines) if len(line) <= 120), "")
    return re.sub(r"</?(?:think|answer)>", "", short_line, flags=re.IGNORECASE).strip()


def sanitize_history_message(role, content):
    role_value = str(role or "").strip().lower()
    text = str(content or "").strip()
    if role_value not in {"user", "assistant"} or not text:
        return ""
    text = re.sub(r"</?(?:think|answer)>", "", text, flags=re.IGNORECASE).strip()
    if role_value == "assistant":
        fallback = extract_fallback_answer_from_leak(text)
        if looks_like_reasoning_leak(text):
            text = fallback
        if not text or looks_like_reasoning_leak(text):
            return ""
    return text


def is_valid_standard_answer(text):
    raw = str(text or "").strip()
    if not raw:
        return False
    answer = extract_tag_content(raw, "answer") or re.sub(r"</?(?:think|answer)>", "", raw, flags=re.IGNORECASE).strip()
    if not answer:
        return False
    if looks_like_reasoning_leak(answer) and len(answer) > 80:
        return False
    return True


def repair_local_standard_answer(model, prompt, leaked_text):
    fallback = extract_fallback_answer_from_leak(leaked_text)
    
    # Use standard num_predict for repair (model-agnostic)
    repair_num_predict = LOCAL_NORMAL_NUM_PREDICT
    
    repair_prompts = [
        (
            "/no_think\n"
            "The draft below is invalid because it contains reasoning instead of the final answer.\n"
            "Rewrite it into the final answer only, in the same language as the user's request.\n"
            "Do not explain. Do not add reasoning. Do not add commentary.\n"
            "Output exactly:\n"
            "<answer>\n"
            "Final answer only.\n"
            "</answer>\n\n"
            f"User request:\n{prompt}\n\n"
            f"Invalid draft:\n{str(leaked_text or '')[:2200]}"
        ),
        (
            "/no_think\n"
            "Answer the user's request directly in the same language as the user.\n"
            "Ignore any previous hidden reasoning, continuation instructions, or invalid draft text.\n"
            "Output exactly one block:\n"
            "<answer>\n"
            "Final answer only.\n"
            "</answer>\n\n"
            f"User request:\n{prompt}"
        ),
    ]
    for repair_prompt in repair_prompts:
        body = {
            "model": model,
            "prompt": repair_prompt,
            "stream": False,
            "options": {
                "temperature": 0.2,
                "num_predict": repair_num_predict,
            },
        }
        response = requests.post(
            "http://127.0.0.1:11434/api/generate",
            json=body,
            timeout=(5, LOCAL_OLLAMA_READ_TIMEOUT_SECONDS),
        )
        response.raise_for_status()
        payload = response.json()
        raw = str(payload.get("response") or "")
        normalized = normalize_model_output(raw, False)
        answer = extract_tag_content(normalized, "answer") or extract_fallback_answer_from_leak(normalized)
        if answer and not looks_like_reasoning_leak(answer):
            return f"<answer>{answer}</answer>"
    if fallback:
        return f"<answer>{fallback}</answer>"
    return "<answer>回答格式异常，请重新生成。</answer>"


def build_local_prompt(prompt, context, deep_think):
    history = context.get("history") if isinstance(context, dict) else None
    history_lines = []
    if isinstance(history, list):
        for item in history[-LOCAL_HISTORY_MESSAGES:]:
            role = str(item.get("role") or "").strip()
            content = sanitize_history_message(role, item.get("content"))
            if role and content:
                if len(content) > LOCAL_HISTORY_CONTENT_CHARS:
                    content = content[:LOCAL_HISTORY_CONTENT_CHARS].rstrip() + "\n...(truncated)"
                history_lines.append(f"{role}: {content}")
    history_text = "\n".join(history_lines)
    protocol = build_universal_llm_protocol(deep_think)
    if history_text:
        return f"{protocol}\n\nConversation history:\n{history_text}\n\nUser:\n{prompt}"
    return f"{protocol}\n\nUser:\n{prompt}"


def update_local_task(task_id, payload, filters=None):
    if supabase:
        safe_update_with_fallback(
            "tasks",
            payload,
            filters=filters or [{"op": "eq", "col": "id", "val": task_id}],
            optional_drop_order=["result_delta", "completed_at", "claimed_at", "miner_name"],
        )
        return
    with _local_task_lock:
        task = _local_task_records.get(task_id)
        if not task:
            return
        task.update(payload or {})


def create_local_task_record(row: dict):
    with _local_task_lock:
        _local_task_records[str(row.get("id"))] = dict(row or {})


def get_local_task_record(task_id: str):
    with _local_task_lock:
        row = _local_task_records.get(str(task_id))
        return dict(row) if isinstance(row, dict) else None


def _notify_sse_clients(task_id: str, data: dict):
    """Notify all SSE clients for a task with new data."""
    global _task_stream_queues
    with _task_stream_lock:
        queue = _task_stream_queues.get(task_id)
        if queue:
            try:
                # Use asyncio.run_coroutine_threadsafe if we're in a thread context
                loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(loop)
                    # Put the data in the queue - non-blocking
                    if not queue.full():
                        queue.put_nowait(data)
                finally:
                    loop.close()
            except Exception:
                pass


def run_local_ollama_task(task_id, prompt, model, deep_think, context):
    """Run local Ollama inference with SSE streaming support."""
    global _task_stream_queues
    
    # Create async queue for SSE streaming
    with _task_stream_lock:
        if task_id not in _task_stream_queues:
            _task_stream_queues[task_id] = asyncio.Queue(maxsize=100)
    
    started_at = time.time()
    first_token_ms = None
    try:
        # Update status via SSE immediately
        _notify_sse_clients(task_id, {"type": "status", "status": "processing", "delta": ""})
        
        update_local_task(
            task_id,
            {
                "status": "processing",
                "claimed_at": datetime.now(timezone.utc).isoformat(),
                "miner_name": "local-ollama",
            },
            filters=[
                {"op": "eq", "col": "id", "val": task_id},
                {"op": "eq", "col": "status", "val": "pending"},
            ],
        )
        
        # Give Supabase time to update, but don't block on read
        time.sleep(0.05)
        
        final_prompt = build_local_prompt(prompt, context, deep_think)
        
        prompt_len = len(final_prompt)
        generation_options = get_local_ollama_generation_options(model, prompt_len, deep_think)
        print(
            f"[DEBUG] Model: {model}, deep_think: {deep_think}, prompt_len: {prompt_len}, "
            f"options={generation_options}"
        )
        full_result = ""
        last_flush = 0.0
        last_supabase_flush = 0.0
        
        with requests.post(
            "http://127.0.0.1:11434/api/generate",
            json={
                "model": model,
                "prompt": final_prompt,
                "stream": True,
                "options": generation_options,
            },
            stream=True,
            timeout=(5, LOCAL_OLLAMA_READ_TIMEOUT_SECONDS),
        ) as response:
            response.raise_for_status()
            for raw_line in response.iter_lines():
                # Check cancellation via Supabase periodically
                if time.time() - last_supabase_flush > 2.0:  # Check every 2 seconds
                    if get_task_status(task_id) == "cancelled":
                        _notify_sse_clients(task_id, {"type": "status", "status": "cancelled"})
                        return
                    last_supabase_flush = time.time()
                    
                # Decode bytes to string
                if isinstance(raw_line, bytes):
                    line = raw_line.decode('utf-8', errors='ignore')
                else:
                    line = str(raw_line or "")
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                token = payload.get("response") or ""
                if token:
                    full_result += token
                    if first_token_ms is None:
                        first_token_ms = max(1, int((time.time() - started_at) * 1000))
                now = time.time()
                
                # SSE streaming - frequent updates to frontend (every 100ms)
                if token and (now - last_flush >= 0.1):
                    _notify_sse_clients(task_id, {"type": "delta", "delta": full_result})
                    last_flush = now
                    
                if payload.get("done"):
                    break

        # Final check for cancellation
        if get_task_status(task_id) == "cancelled":
            _notify_sse_clients(task_id, {"type": "status", "status": "cancelled"})
            return
            
        print(f"[DEBUG] Model: {model}, Raw output length: {len(full_result)}, Preview: {full_result[:200]}...")
        final_result = normalize_model_output(full_result, deep_think)
        print(f"[DEBUG] Normalized result: {final_result[:200]}...")
        if not deep_think and not is_valid_standard_answer(final_result):
            answer = extract_tag_content(final_result, "answer") or re.sub(r"</?(?:think|answer)>", "", final_result, flags=re.IGNORECASE).strip()
            print(f"[DEBUG] Answer validation failed - answer: '{answer[:100]}...', looks_like_reasoning_leak: {looks_like_reasoning_leak(answer)}, len: {len(answer)}")
            print(f"[DEBUG] Attempting repair...")
            final_result = repair_local_standard_answer(model, prompt, full_result)
            final_result = normalize_model_output(final_result, False)
            print(f"[DEBUG] Repaired result: {final_result[:200]}...")
        
        # Notify completion via SSE
        _notify_sse_clients(task_id, {"type": "complete", "status": "completed", "result": final_result})

        next_context = build_billing_context_on_settle(
            context,
            next_state="local_completed",
        )
        metrics = next_context.get("metrics") if isinstance(next_context.get("metrics"), dict) else {}
        if first_token_ms is not None:
            metrics["first_token_ms"] = float(first_token_ms)
            next_context["metrics"] = metrics

        # Write final result to Supabase (not every token)
        update_local_task(
            task_id,
            {
                "status": "completed",
                "result": final_result,
                "result_delta": "",
                "context": next_context,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            filters=[
                {"op": "eq", "col": "id", "val": task_id},
                {"op": "in", "col": "status", "val": ACTIVE_TASK_STATUSES},
            ],
        )
    except Exception as e:
        logger.warning("local ollama task failed for %s: %s", task_id, e)
        failure_reason = str(e or "local_ollama_failed").strip()[:240] or "local_ollama_failed"
        _notify_sse_clients(task_id, {"type": "error", "error": failure_reason})

        # Important:
        # Do NOT silently fall back to remote when local execution fails.
        # Routing (auto->local/remote) is decided at task creation time.
        # A local failure must remain local-failed to avoid unexpected credit
        # charge and status oscillation (pending->claimed->processing loops).
        next_context = build_billing_context_on_settle(
            context,
            next_state="local_failed",
        )
        metrics = next_context.get("metrics") if isinstance(next_context.get("metrics"), dict) else {}
        if first_token_ms is not None:
            metrics["first_token_ms"] = float(first_token_ms)
            next_context["metrics"] = metrics
        update_local_task(
            task_id,
            {
                "status": "failed",
                "result": f"Local Ollama failed: {failure_reason}",
                "result_delta": "",
                "failure_reason": failure_reason,
                "context": next_context,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            filters=[
                {"op": "eq", "col": "id", "val": task_id},
                {"op": "in", "col": "status", "val": ACTIVE_TASK_STATUSES},
            ],
        )
    finally:
        # Clean up queue after TTL
        def cleanup_queue():
            time.sleep(_task_stream_ttl_seconds)
            with _task_stream_lock:
                _task_stream_queues.pop(task_id, None)
        threading.Thread(target=cleanup_queue, daemon=True).start()


def start_local_ollama_task(task_id, prompt, model, deep_think, context):
    thread = threading.Thread(
        target=run_local_ollama_task,
        args=(task_id, prompt, model, deep_think, context),
        daemon=True,
        name=f"local-ollama-{task_id[:8]}",
    )
    thread.start()


def update_task_record(task_id, required_payload, optional_payload=None):
    payload = dict(required_payload)
    if optional_payload:
        payload.update(optional_payload)
    try:
        supabase_execute_with_retry(lambda: supabase.table("tasks").update(payload).eq("id", task_id).execute())
    except Exception:
        if optional_payload:
            supabase_execute_with_retry(
                lambda: supabase.table("tasks").update(required_payload).eq("id", task_id).execute()
            )
            return
        raise


def parse_missing_column_from_error(error_message):
    msg = str(error_message or "")
    patterns = [
        r"column [^.]+\.([A-Za-z0-9_]+) does not exist",
        r"column \"?([A-Za-z0-9_]+)\"? does not exist",
        r"Could not find the '([^']+)' column",
    ]
    for pattern in patterns:
        match = re.search(pattern, msg, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def remember_missing_column(table_name, column_name):
    table = str(table_name or "").strip()
    col = str(column_name or "").strip()
    if not table or not col:
        return
    with _schema_missing_lock:
        _schema_missing_columns_by_table[table].add(col)


def get_known_missing_columns(table_name):
    table = str(table_name or "").strip()
    if not table:
        return set()
    with _schema_missing_lock:
        return set(_schema_missing_columns_by_table.get(table, set()))


def safe_update_with_fallback(table_name, payload, filters, optional_drop_order=None):
    optional_drop_order = optional_drop_order or []
    current_payload = dict(payload)
    for known_missing in get_known_missing_columns(table_name):
        current_payload.pop(known_missing, None)
    dropped = set()

    while True:
        try:
            query = supabase.table(table_name).update(current_payload)
            for f in filters:
                op = f["op"]
                col = f["col"]
                val = f["val"]
                if op == "eq":
                    query = query.eq(col, val)
                elif op == "in":
                    query = query.in_(col, val)
                else:
                    raise ValueError(f"Unsupported filter op: {op}")
            return supabase_execute_with_retry(lambda: query.execute())
        except Exception as e:
            missing_col = parse_missing_column_from_error(e)
            if not missing_col:
                raise
            remember_missing_column(table_name, missing_col)
            if missing_col in current_payload:
                current_payload.pop(missing_col, None)
                dropped.add(missing_col)
                continue
            if missing_col in optional_drop_order and missing_col not in dropped:
                dropped.add(missing_col)
                continue
            raise


def insert_row_with_schema_fallback(table_name, payload, optional_drop_order=None):
    current_payload = dict(payload)
    for known_missing in get_known_missing_columns(table_name):
        current_payload.pop(known_missing, None)
    drop_queue = list(optional_drop_order or [])
    while True:
        try:
            return supabase_execute_with_retry(lambda: supabase.table(table_name).insert(current_payload).execute())
        except Exception as e:
            missing_col = parse_missing_column_from_error(e)
            if missing_col:
                remember_missing_column(table_name, missing_col)
                if missing_col not in current_payload:
                    raise
                current_payload.pop(missing_col, None)
                continue

            # Schema drift fallback: when error text is not parseable but table is older/newer,
            # drop optional fields one by one to maximize insert compatibility.
            dropped_any = False
            while drop_queue:
                candidate = drop_queue.pop(0)
                if candidate in current_payload:
                    current_payload.pop(candidate, None)
                    dropped_any = True
                    break
            if dropped_any:
                continue
            raise


def select_one_with_schema_fallback(table_name, columns, filters):
    known_missing = get_known_missing_columns(table_name)
    current_columns = [col for col in list(columns) if col not in known_missing]
    if not current_columns:
        current_columns = ["id"]
    while True:
        try:
            query = supabase.table(table_name).select(",".join(current_columns))
            for f in filters:
                op = f["op"]
                col = f["col"]
                val = f["val"]
                if op == "eq":
                    query = query.eq(col, val)
                else:
                    raise ValueError(f"Unsupported filter op: {op}")
            query = query.limit(1)
            result = supabase_execute_with_retry(lambda: query.execute())
            row = result.data[0] if result.data else None
            if row is not None:
                for col in columns:
                    row.setdefault(col, None)
            return row
        except Exception as e:
            missing_col = parse_missing_column_from_error(e)
            if not missing_col:
                raise
            remember_missing_column(table_name, missing_col)
            if missing_col not in current_columns:
                raise
            current_columns = [col for col in current_columns if col != missing_col]


def select_rows_with_schema_fallback(table_name, columns, filters=None, limit=None, order_by=None, ascending=True):
    filters = filters or []
    known_missing = get_known_missing_columns(table_name)
    current_columns = [col for col in list(columns) if col not in known_missing]
    if not current_columns:
        current_columns = ["id"]
    while True:
        try:
            query = supabase.table(table_name).select(",".join(current_columns))
            for f in filters:
                op = f["op"]
                col = f["col"]
                val = f["val"]
                if op == "eq":
                    query = query.eq(col, val)
                elif op == "in":
                    query = query.in_(col, val)
                else:
                    raise ValueError(f"Unsupported filter op: {op}")
            if order_by:
                query = query.order(order_by, desc=not ascending)
            if limit:
                query = query.limit(limit)
            result = supabase_execute_with_retry(lambda: query.execute())
            rows = result.data or []
            for row in rows:
                for col in columns:
                    row.setdefault(col, None)
            return rows
        except Exception as e:
            missing_col = parse_missing_column_from_error(e)
            if not missing_col:
                raise
            remember_missing_column(table_name, missing_col)
            if missing_col not in current_columns:
                raise
            current_columns = [col for col in current_columns if col != missing_col]


def count_rows_with_schema_fallback(table_name, filters=None):
    filters = filters or []
    try:
        query = supabase.table(table_name).select("id", count="exact")
        for f in filters:
            op = f["op"]
            col = f["col"]
            val = f["val"]
            if op == "eq":
                query = query.eq(col, val)
            elif op == "in":
                query = query.in_(col, val)
            else:
                raise ValueError(f"Unsupported filter op: {op}")
        result = supabase_execute_with_retry(lambda: query.limit(1).execute())
        return int(result.count or 0)
    except Exception:
        rows = select_rows_with_schema_fallback(table_name, ["id"], filters=filters, limit=1000)
        return len(rows)


def log_transition(event, **fields):
    safe = {}
    for k, v in fields.items():
        if v is None:
            continue
        key = str(k)
        if key in {"user_id", "request_user_id"}:
            safe[key] = "[uuid]"
        elif key in {"miner_name", "hwid"}:
            safe[key] = "[redacted]"
        else:
            safe[key] = v
    logger.info("%s | %s", event, safe)


def parse_utc_ts(value):
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def try_atomic_claim(task_data, miner_name):
    if not task_data:
        return False, None, None, None

    task_id = task_data.get("id")
    if not task_id:
        return False, None, None, None

    task_status = str(task_data.get("status", "")).lower()
    old_status = task_status or "pending"
    # If a task was already assigned to this miner, let the miner pick it up.
    # Terminal tasks or tasks claimed by another miner must never be treated as claimable.
    if task_status and task_status != "pending":
        existing_miner = str(task_data.get("miner_name") or "").strip()
        if task_status in {"claimed", "processing"} and existing_miner == miner_name:
            return True, task_id, old_status, old_status
        return False, task_id, old_status, old_status

    claim_payload = {
        "status": "claimed",
        "claimed_at": datetime.now(timezone.utc).isoformat(),
        "miner_name": miner_name,
    }
    safe_update_with_fallback(
        "tasks",
        claim_payload,
        filters=[
            {"op": "eq", "col": "id", "val": task_id},
            {"op": "eq", "col": "status", "val": "pending"},
        ],
    )
    verify = (
        supabase.table("tasks")
        .select("id,status,miner_name")
        .eq("id", task_id)
        .eq("status", "claimed")
        .eq("miner_name", miner_name)
        .limit(1)
        .execute()
    )
    claimed = bool(verify.data)
    return claimed, task_id, old_status, ("claimed" if claimed else old_status)
    
def try_recover_stale_tasks():
    global _last_stale_recovery_at, _stale_recovery_enabled

    if not _stale_recovery_enabled:
        return

    now = time.time()
    if now - _last_stale_recovery_at < STALE_RECOVERY_INTERVAL_SECONDS:
        return
    _last_stale_recovery_at = now

    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=TASK_LEASE_TIMEOUT_SECONDS)).isoformat()
    try:
        # Prefer claimed_at since this schema may not have updated_at.
        stale = (
            supabase.table("tasks")
            .select("id,status,claimed_at,created_at")
            .in_("status", ["claimed", "processing"])
            .lt("claimed_at", cutoff)
            .limit(100)
            .execute()
        )
    except Exception as e:
        _stale_recovery_enabled = False
        logger.warning("Stale task recovery disabled: %s", e)
        return

    stale_rows = stale.data or []
    if not stale_rows:
        return

    recovered = 0
    for row in stale_rows:
        task_id = row.get("id")
        if not task_id:
            continue
        previous_status = row.get("status")
        age_seconds = None
        ts = parse_utc_ts(row.get("claimed_at")) or parse_utc_ts(row.get("created_at"))
        if ts:
            age_seconds = max(0, round((datetime.now(timezone.utc) - ts).total_seconds(), 3))
        try:
            safe_update_with_fallback(
                "tasks",
                {
                    "status": "pending",
                    "result_delta": "",
                    "failure_reason": STALE_REQUEUE_REASON_CODE,
                    "miner_name": "",
                    "claimed_at": None,
                },
                filters=[
                    {"op": "eq", "col": "id", "val": task_id},
                    {"op": "in", "col": "status", "val": ["claimed", "processing"]},
                ],
                optional_drop_order=["miner_name", "claimed_at", "failure_reason"],
            )
            if get_task_status(task_id) == "pending":
                recovered += 1
                log_transition(
                    "stale_requeue",
                    task_id=task_id,
                    previous_status=previous_status,
                    new_status="pending",
                    reason_code=STALE_REQUEUE_REASON_CODE,
                    age_seconds=age_seconds,
                )
        except Exception as e:
            logger.warning("Recover stale task failed for %s: %s", task_id, e)

    if recovered:
        logger.info("Recovered %s stale task(s)", recovered)


def get_task_status(task_id, max_retries=3, retry_delay=0.1):
    """Get task status with retry logic to handle Supabase replication delays."""
    if not supabase:
        local_row = get_local_task_record(task_id)
        if not local_row:
            return None
        return str(local_row.get("status") or "").lower() or None
    for attempt in range(max_retries):
        try:
            res = (
                supabase.table("tasks")
                .select("status")
                .eq("id", task_id)
                .limit(1)
                .execute()
            )
            if not res.data:
                return None
            return str(res.data[0].get("status", "")).lower()
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            logger.warning("get_task_status failed for %s after %d retries: %s", task_id, max_retries, e)
            return None
    return None


def fetch_pending_task_fallback():
    # Fallback path for schema/RPC mismatches: pull one pending task directly.
    res = (
        supabase.table("tasks")
        .select("*")
        .eq("status", "pending")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not res.data:
        return None
    return res.data[0]


def normalize_source_filters(value):
    if value is None:
        return set()
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    return {
        str(item).strip().lower()
        for item in items
        if str(item).strip()
    }


def get_task_source(task_data):
    context = task_data.get("context") if isinstance(task_data.get("context"), dict) else {}
    return str(context.get("source") or "").strip().lower()


def task_matches_source_filters(task_data, accepted_sources=None, excluded_sources=None):
    accepted_sources = accepted_sources or set()
    excluded_sources = excluded_sources or set()
    source = get_task_source(task_data)
    if accepted_sources and source not in accepted_sources:
        return False
    if excluded_sources and source in excluded_sources:
        return False
    return True


def fetch_pending_task_filtered(accepted_sources=None, excluded_sources=None):
    accepted_sources = accepted_sources or set()
    excluded_sources = excluded_sources or set()
    res = (
        supabase.table("tasks")
        .select("*")
        .eq("status", "pending")
        .order("created_at", desc=True)
        .limit(100)
        .execute()
    )
    for row in res.data or []:
        if task_matches_source_filters(row, accepted_sources, excluded_sources):
            return row
    return None


def fetch_pending_task_for_miner(miner_row, accepted_sources=None, excluded_sources=None):
    accepted_sources = accepted_sources or set()
    excluded_sources = excluded_sources or set()
    res = (
        supabase.table("tasks")
        .select("*")
        .eq("status", "pending")
        .order("created_at", desc=True)
        .limit(100)
        .execute()
    )
    for row in res.data or []:
        if not task_matches_source_filters(row, accepted_sources, excluded_sources):
            continue
        can_run, _ = validate_miner_can_run_task(miner_row, row)
        if can_run:
            return row
    return None


def fetch_assigned_claimed_task_for_miner(miner_row, miner_name, accepted_sources=None, excluded_sources=None):
    accepted_sources = accepted_sources or set()
    excluded_sources = excluded_sources or set()
    res = (
        supabase.table("tasks")
        .select("*")
        .eq("status", "claimed")
        .eq("miner_name", miner_name)
        .order("claimed_at", desc=False)
        .limit(20)
        .execute()
    )
    for row in res.data or []:
        if not task_matches_source_filters(row, accepted_sources, excluded_sources):
            continue
        can_run, _ = validate_miner_can_run_task(miner_row, row)
        if can_run:
            return row
    return None


def build_ops_alerts_snapshot():
    now = datetime.now(timezone.utc)
    alerts = []

    pending_count = 0
    stale_claim_count = 0
    oldest_pending_age_s = 0.0

    try:
        pending_count = count_rows_with_schema_fallback(
            "tasks",
            filters=[{"op": "eq", "col": "status", "val": "pending"}],
        )
    except Exception as e:
        logger.warning("pending count check failed: %s", e)

    try:
        oldest_pending = select_rows_with_schema_fallback(
            "tasks",
            ["id", "created_at", "status"],
            filters=[{"op": "eq", "col": "status", "val": "pending"}],
            limit=1,
            order_by="created_at",
            ascending=True,
        )
        if oldest_pending:
            ts = parse_utc_ts(oldest_pending[0].get("created_at"))
            if ts:
                oldest_pending_age_s = max(0.0, round((now - ts).total_seconds(), 2))
    except Exception as e:
        logger.warning("oldest pending check failed: %s", e)

    try:
        claimed_rows = select_rows_with_schema_fallback(
            "tasks",
            ["id", "status", "claimed_at", "created_at"],
            filters=[{"op": "in", "col": "status", "val": ["claimed", "processing"]}],
            limit=1000,
            order_by="created_at",
            ascending=True,
        )
        for row in claimed_rows:
            ts = parse_utc_ts(row.get("claimed_at")) or parse_utc_ts(row.get("created_at"))
            if not ts:
                continue
            if (now - ts).total_seconds() > TASK_LEASE_TIMEOUT_SECONDS:
                stale_claim_count += 1
    except Exception as e:
        logger.warning("stale claim check failed: %s", e)

    if pending_count >= ALERT_PENDING_BACKLOG_THRESHOLD:
        alerts.append(
            {
                "type": "task_backlog",
                "severity": "warning",
                "message": f"Pending backlog is {pending_count}, threshold {ALERT_PENDING_BACKLOG_THRESHOLD}",
                "pending_count": pending_count,
            }
        )
    if stale_claim_count >= ALERT_CLAIM_TIMEOUT_THRESHOLD:
        alerts.append(
            {
                "type": "claim_timeout",
                "severity": "critical",
                "message": f"Stale claimed/processing tasks: {stale_claim_count}",
                "stale_claim_count": stale_claim_count,
                "lease_timeout_seconds": TASK_LEASE_TIMEOUT_SECONDS,
            }
        )

    return {
        "pending_count": pending_count,
        "oldest_pending_age_seconds": oldest_pending_age_s,
        "stale_claim_count": stale_claim_count,
        "alerts": alerts,
    }


def build_task_preview(prompt: str, limit: int = 120) -> str:
    text = " ".join(str(prompt or "").split())
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3] + "..."


def derive_model_capability(vram_gb):
    try:
        vram = float(vram_gb or 0)
    except (TypeError, ValueError):
        vram = 0.0
    if vram >= 24:
        return {"score": 4, "label": "unlimited"}
    if vram >= 14:
        return {"score": 3, "label": "27b"}
    if vram >= 6:
        return {"score": 2, "label": "9b"}
    return {"score": 1, "label": "4b"}


def get_local_gpu_vram_gb():
    """Get local GPU VRAM in GB using nvidia-smi or wmi."""
    vram_gb = 0
    gpu_count = 1
    try:
        try:
            import subprocess
            cmd = "nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits"
            output = subprocess.check_output(cmd, shell=True).decode().strip()
            vram_gb = max([int(x) for x in output.split('\n')]) / 1024
            gpu_count = len(output.split('\n'))
            if vram_gb > 0:
                return vram_gb
        except Exception:
            pass

        # Fallback to WMI for Windows
        try:
            import wmi
            c = wmi.WMI()
            for gpu in c.Win32_VideoController():
                raw_ram = gpu.AdapterRAM
                ram_fixed = (raw_ram + 2 ** 32) if raw_ram < 0 else raw_ram
                vram_curr = ram_fixed / (1024 ** 3)
                # Handle 4GB GPUs that are actually 8GB (common issue)
                if 3.8 <= vram_curr <= 4.2:
                    if any(x in gpu.Name.upper() for x in ["RTX", "RX", "ARC", "GTX"]):
                        vram_curr = 8.0
                vram_gb = max(vram_gb, vram_curr)
        except Exception:
            pass
    except Exception:
        vram_gb = 4.0  # Default fallback
    return vram_gb


def local_gpu_can_run_model(model_name):
    """Check if local GPU has enough VRAM to run the model.
    Small models (4b and below) can run on CPU or integrated graphics.
    Larger models require sufficient GPU VRAM.
    """
    vram_gb = get_local_gpu_vram_gb()
    model_score = get_model_capability_score(model_name)
    
    # Small models (score 1: 4b and below) can run on any hardware
    if model_score == 1:
        return True
    
    # Larger models require GPU with sufficient VRAM
    if vram_gb < 4:
        return False
    gpu_capability = derive_model_capability(vram_gb)
    return model_score <= gpu_capability["score"]


def get_model_capability_score(model_name):
    text = str(model_name or "").lower()
    match = re.search(r"(\d+(?:\.\d+)?)b", text)
    if not match:
        return 2  # Default to medium if no size specified
    size = float(match.group(1))
    if size >= 27:
        return 4  # Requires 24GB+ VRAM
    if size >= 14:
        return 3  # Requires 14GB+ VRAM
    if size >= 9:
        return 2  # Requires 6GB+ VRAM
    return 1  # Can run on <6GB VRAM (4b and below)


def get_local_ollama_generation_options(model_name: str, prompt_len: int, deep_think: bool):
    model_score = get_model_capability_score(model_name)
    vram_gb = float(get_local_gpu_vram_gb() or 0)

    base_num_predict = LOCAL_NORMAL_NUM_PREDICT if not deep_think else LOCAL_DEEP_NUM_PREDICT
    min_num_predict = max(256, min(1024, prompt_len * 2))
    num_predict = min(base_num_predict, max(min_num_predict, 512))

    num_ctx = LOCAL_OLLAMA_NUM_CTX
    num_batch = LOCAL_OLLAMA_NUM_BATCH

    # 8GB-class GPUs can run 9b models, but 4096 context tends to spill into CPU/GPU
    # mixed execution and causes unstable first-token latency. Keep local defaults tighter.
    if model_score >= 2 and vram_gb and vram_gb <= 8.5:
        num_ctx = min(num_ctx, 1536)
        num_batch = min(num_batch, 16)
    elif model_score >= 3 and vram_gb and vram_gb <= 12.5:
        num_ctx = min(num_ctx, 1024)
        num_batch = min(num_batch, 16)

    if deep_think:
        num_ctx = min(num_ctx, 2048)

    return {
        "temperature": 0.3,
        "num_predict": max(256, int(num_predict)),
        "num_ctx": max(512, int(num_ctx)),
        "num_batch": max(1, int(num_batch)),
    }


def model_is_installed_for_miner(miner_row, model_name):
    installed = miner_row.get("installed_models") if isinstance(miner_row, dict) else []
    if not isinstance(installed, list):
        return False
    wanted = str(model_name or "").strip()
    return bool(wanted) and wanted in {str(item).strip() for item in installed}


def task_model_name(task_row):
    context = task_row.get("context") if isinstance(task_row.get("context"), dict) else {}
    return task_row.get("model") or context.get("model") or ""


def validate_miner_can_run_task(miner_row, task_row):
    model_name = task_model_name(task_row)
    miner_capability = derive_model_capability(miner_row.get("vram_gb"))
    if get_model_capability_score(model_name) > miner_capability["score"]:
        return False, f"Miner VRAM is not enough for {model_name}"
    if model_name and not model_is_installed_for_miner(miner_row, model_name):
        return False, f"Miner has not installed model: {model_name}"
    return True, ""


def serialize_order_task(task_row):
    context = task_row.get("context") if isinstance(task_row.get("context"), dict) else {}
    metrics = context.get("metrics") if isinstance(context.get("metrics"), dict) else {}
    raw_user_id = str(task_row.get("user_id") or "").strip()
    user_seed = raw_user_id or "anonymous"
    requester_masked = f"user-{hashlib.sha256(user_seed.encode('utf-8')).hexdigest()[:10]}"
    cipher_seed = "|".join(
        [
            str(task_row.get("id") or ""),
            user_seed,
            str(task_row.get("model") or context.get("model") or ""),
            str(task_row.get("created_at") or ""),
        ]
    )
    content_cipher = f"sha256:{hashlib.sha256(cipher_seed.encode('utf-8')).hexdigest()}" if cipher_seed else ""
    return {
        "id": task_row.get("id"),
        "status": task_row.get("status") or "unknown",
        "prompt_preview": "",
        "content_cipher": content_cipher,
        "model": task_row.get("model") or context.get("model") or "",
        "mode": context.get("mode") or "Auto",
        "source": context.get("source") or "frontend",
        "deep_think": bool(task_row.get("deep_think", False)),
        "requester_id_masked": requester_masked,
        "miner_name": task_row.get("miner_name") or "",
        "created_at": task_row.get("created_at"),
        "claimed_at": task_row.get("claimed_at"),
        "completed_at": task_row.get("completed_at"),
        "failure_reason": task_row.get("failure_reason") or "",
        "first_token_ms": metrics.get("first_token_ms"),
    }


@app.get("/")
def home():
    return {"status": "V12 AI Bank Gateway Online"}


@app.get("/healthz")
def healthz():
    snapshot = build_ops_alerts_snapshot()
    uptime_seconds = max(
        0.0,
        round((datetime.now(timezone.utc) - _gateway_started_at).total_seconds(), 2),
    )
    severity_rank = {"ok": 0, "warning": 1, "critical": 2}
    overall = "ok"
    for item in snapshot["alerts"]:
        level = str(item.get("severity") or "ok").lower()
        if severity_rank.get(level, 0) > severity_rank.get(overall, 0):
            overall = level
    return {
        "status": "success",
        "service": "v12-gateway",
        "health": overall,
        "time": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": uptime_seconds,
        "log_level": LOG_LEVEL,
        "metrics": {
            "pending_count": snapshot["pending_count"],
            "oldest_pending_age_seconds": snapshot["oldest_pending_age_seconds"],
            "stale_claim_count": snapshot["stale_claim_count"],
        },
        "alerts": snapshot["alerts"],
    }


@app.get("/ops/health")
def ops_health(request: Request):
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    return healthz()


@app.get("/ops/metrics")
def ops_metrics(request: Request):
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    if not ENABLE_OPS_METRICS:
        return error_response("forbidden", "Ops metrics endpoint disabled")

    snapshot = build_ops_alerts_snapshot()
    with _http_metrics_lock:
        status_counts = dict(_http_status_counters)
        path_counts = dict(_http_path_counters)
    total_requests = sum(status_counts.values()) or 1
    five_xx = sum(count for code, count in status_counts.items() if str(code).startswith("5"))
    five_xx_ratio = round(float(five_xx) / float(total_requests), 6)

    refund_anomaly_count = 0
    try:
        refund_rows = select_rows_with_schema_fallback(
            "credit_ledger",
            ["task_id", "phase", "amount", "created_at"],
            filters=[{"op": "eq", "col": "phase", "val": "refunded"}],
            limit=500,
            order_by="created_at",
            ascending=False,
        )
        for row in refund_rows:
            try:
                if float(row.get("amount") or 0) <= 0:
                    refund_anomaly_count += 1
            except Exception:
                refund_anomaly_count += 1
    except Exception:
        refund_anomaly_count = -1

    return {
        "status": "success",
        "service": "v12-gateway",
        "time": datetime.now(timezone.utc).isoformat(),
        "http": {
            "total": total_requests,
            "five_xx": five_xx,
            "five_xx_ratio": five_xx_ratio,
            "status_counts": status_counts,
            "top_paths": sorted(path_counts.items(), key=lambda x: x[1], reverse=True)[:20],
        },
        "tasks": {
            "pending_count": snapshot["pending_count"],
            "oldest_pending_age_seconds": snapshot["oldest_pending_age_seconds"],
            "stale_claim_count": snapshot["stale_claim_count"],
        },
        "billing": {
            "refund_anomaly_count": refund_anomaly_count,
        },
        "thresholds": {
            "TASK_LEASE_TIMEOUT_SECONDS": TASK_LEASE_TIMEOUT_SECONDS,
            "ALERT_PENDING_BACKLOG_THRESHOLD": ALERT_PENDING_BACKLOG_THRESHOLD,
            "ALERT_CLAIM_TIMEOUT_THRESHOLD": ALERT_CLAIM_TIMEOUT_THRESHOLD,
            "CREATE_TASK_USER_DAILY_LIMIT": CREATE_TASK_USER_DAILY_LIMIT,
        },
    }


@app.post("/auth/session")
async def create_auth_session(request: Request):
    if not ALLOW_LEGACY_AUTH_SESSION:
        return error_response("forbidden", "Legacy auth/session is disabled; use Supabase login")
    try:
        data = await request.json()
    except Exception:
        data = {}
    user_id = normalize_user_id_for_storage(data.get("user_id"))
    token = issue_auth_token(user_id)
    if supabase:
        credits = compute_user_credit_summary(user_id)
    else:
        credits = {
            "user_id": user_id,
            "total": DEFAULT_USER_CREDITS,
            "spent": 0.0,
            "reserved": 0.0,
            "available": DEFAULT_USER_CREDITS,
            "tasks": {"completed": 0, "failed": 0, "cancelled": 0, "active": 0},
        }
    return {
        "status": "success",
        "session": {"user_id": user_id, "token": token, "expires_in": AUTH_TOKEN_TTL_SECONDS},
        "credits": credits,
    }


@app.post("/auth/supabase/session")
async def create_supabase_auth_session(request: Request):
    access_token = extract_auth_bearer(request)
    supabase_user = verify_supabase_access_token(access_token)
    if not supabase_user:
        return error_response("unauthorized", "Invalid Supabase session token")

    user_id = normalize_user_id_for_storage(supabase_user.get("id"))
    token = issue_auth_token(user_id) if supabase else access_token
    if supabase:
        credits = compute_user_credit_summary(user_id)
    elif has_control_plane_proxy():
        proxied = proxy_control_plane_json(request, "/credits/me", method="GET")
        credits = proxied.get("credits") if isinstance(proxied, dict) and proxied.get("status") == "success" else build_default_credit_summary(user_id)
    else:
        credits = build_default_credit_summary(user_id)
    return {
        "status": "success",
        "session": {"user_id": user_id, "token": token, "expires_in": AUTH_TOKEN_TTL_SECONDS},
        "supabase_user": {
            "id": user_id,
            "email": supabase_user.get("email"),
            "phone": supabase_user.get("phone"),
            "user_metadata": supabase_user.get("user_metadata") or {},
        },
        "credits": credits,
    }


@app.get("/credits/me")
def credits_me(request: Request):
    if not supabase:
        if has_control_plane_proxy():
            return proxy_control_plane_json(request, "/credits/me", method="GET")
        return error_response("service_unavailable", "Supabase credentials missing")
    user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    try:
        return {"status": "success", "credits": compute_user_credit_summary(user_id)}
    except Exception as e:
        return internal_error("/credits/me", e)


@app.get("/credits/ledger/me")
def credits_ledger_me(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    try:
        normalized_uid = normalize_user_id_for_storage(user_id)
        try:
            rows = select_rows_with_schema_fallback(
                "credit_ledger",
                ["task_id", "phase", "direction", "amount", "actor_type", "actor_id", "note", "created_at"],
                filters=[
                    {"op": "eq", "col": "actor_type", "val": "user"},
                    {"op": "eq", "col": "actor_id", "val": normalized_uid},
                ],
                limit=200,
                order_by="created_at",
                ascending=False,
            )
        except Exception as e:
            logger.warning("credit_ledger query failed in /credits/ledger/me, return empty items: %s", e)
            rows = []
        return {"status": "success", "items": rows}
    except Exception as e:
        return internal_error("/credits/ledger/me", e)


@app.post("/credits/grant")
async def credits_grant(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    admin_error = require_admin_credit_grant(request)
    if admin_error:
        return admin_error
    try:
        data = await request.json()
    except Exception:
        return error_response("bad_request", "Invalid JSON body")

    admin_actor = str(data.get("admin_id") or request.headers.get("x-admin-id") or "admin").strip()[:120] or "admin"
    try:
        result = grant_credit_to_user(
            user_id=data.get("user_id"),
            amount=data.get("amount"),
            phase=data.get("phase") or "grant",
            note=data.get("note") or "",
            admin_actor=admin_actor,
        )
    except ValueError as e:
        return error_response("bad_request", str(e))

    return {"status": "success", **result}


@app.post("/credits/grant/batch")
async def credits_grant_batch(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    admin_error = require_admin_credit_grant(request)
    if admin_error:
        return admin_error
    try:
        data = await request.json()
    except Exception:
        return error_response("bad_request", "Invalid JSON body")

    items = data.get("items")
    if not isinstance(items, list) or not items:
        return error_response("bad_request", "items must be a non-empty array")
    if len(items) > 500:
        return error_response("bad_request", "Batch size exceeds 500 items")

    phase = str(data.get("phase") or "grant").strip().lower()
    note = str(data.get("note") or "").strip()[:240]
    admin_actor = str(data.get("admin_id") or request.headers.get("x-admin-id") or "admin").strip()[:120] or "admin"

    results = []
    success_count = 0
    error_count = 0

    for item in items:
        if not isinstance(item, dict):
            results.append({"status": "error", "message": "Each item must be an object"})
            error_count += 1
            continue
        item_note = str(item.get("note") or note or "").strip()[:240]
        try:
            result = grant_credit_to_user(
                user_id=item.get("user_id"),
                amount=item.get("amount"),
                phase=item.get("phase") or phase,
                note=item_note,
                admin_actor=admin_actor,
            )
            results.append({"status": "success", **result})
            success_count += 1
        except ValueError as e:
            results.append(
                {
                    "status": "error",
                    "user_id": normalize_user_id_for_storage(item.get("user_id")),
                    "amount": item.get("amount"),
                    "message": str(e),
                }
            )
            error_count += 1

    return {
        "status": "success",
        "summary": {
            "requested": len(items),
            "success": success_count,
            "failed": error_count,
            "phase": phase,
            "admin_id": admin_actor,
        },
        "results": results,
    }


@app.get("/dashboard/dispatch")
def get_dispatch_dashboard(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error

    try:
        miners = select_rows_with_schema_fallback(
            "miners",
            [
                "miner_name",
                "status",
                "last_seen",
                "vram_gb",
                "completed_tasks",
                "failed_tasks",
            ],
            limit=100,
            order_by="last_seen",
            ascending=False,
        )

        now_utc = datetime.now(timezone.utc)
        normalized_miners = []
        for miner in miners:
            last_seen = parse_utc_ts(miner.get("last_seen"))
            seconds_since_seen = None
            freshness = "unknown"
            if last_seen:
                seconds_since_seen = max(0, round((now_utc - last_seen).total_seconds(), 2))
                freshness = "online" if seconds_since_seen <= 75 else "stale"
            normalized_miners.append(
                {
                    "miner_name": miner.get("miner_name") or "unknown",
                    "status": miner.get("status") or "unknown",
                    "last_seen": miner.get("last_seen"),
                    "seconds_since_seen": seconds_since_seen,
                    "freshness": freshness,
                    "vram_gb": miner.get("vram_gb") or 0,
                    "completed_tasks": miner.get("completed_tasks") or 0,
                    "failed_tasks": miner.get("failed_tasks") or 0,
                }
            )

        task_counts = {
            "pending": count_rows_with_schema_fallback("tasks", filters=[{"op": "eq", "col": "status", "val": "pending"}]),
            "claimed": count_rows_with_schema_fallback("tasks", filters=[{"op": "eq", "col": "status", "val": "claimed"}]),
            "processing": count_rows_with_schema_fallback("tasks", filters=[{"op": "eq", "col": "status", "val": "processing"}]),
            "completed": count_rows_with_schema_fallback("tasks", filters=[{"op": "eq", "col": "status", "val": "completed"}]),
            "failed": count_rows_with_schema_fallback("tasks", filters=[{"op": "eq", "col": "status", "val": "failed"}]),
        }
        task_counts["active"] = task_counts["pending"] + task_counts["claimed"] + task_counts["processing"]

        frontend_counts = {
            "pending": 0,
            "claimed": 0,
            "processing": 0,
        }
        benchmark_counts = {
            "pending": 0,
            "claimed": 0,
            "processing": 0,
        }
        recent_pool = select_rows_with_schema_fallback(
            "tasks",
            ["status", "context", "created_at"],
            filters=[{"op": "in", "col": "status", "val": ACTIVE_TASK_STATUSES}],
            limit=200,
            order_by="created_at",
            ascending=False,
        )
        for row in recent_pool:
            status = str(row.get("status") or "").lower()
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            source = context.get("source") or "frontend"
            if source == "code_eval":
                if status in benchmark_counts:
                    benchmark_counts[status] += 1
            else:
                if status in frontend_counts:
                    frontend_counts[status] += 1

        return {
            "status": "success",
            "summary": {
                "miners_online": sum(1 for item in normalized_miners if item["freshness"] == "online"),
                "miners_total": len(normalized_miners),
                "task_counts": task_counts,
                "frontend_counts": frontend_counts,
                "benchmark_counts": benchmark_counts,
            },
            "miners": normalized_miners,
        }
    except Exception as e:
        return internal_error("/dashboard/dispatch", e)


@app.get("/dashboard/metrics")
def get_dashboard_metrics(request: Request, limit: int = 500):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    try:
        rows = select_rows_with_schema_fallback(
            "tasks",
            ["id", "status", "failure_reason", "context", "created_at", "completed_at"],
            limit=max(50, min(int(limit or 500), 2000)),
            order_by="created_at",
            ascending=False,
        )
        done = 0
        success = 0
        first_tokens = []
        fail_reasons = {}
        for row in rows:
            status = str(row.get("status") or "").lower()
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            metrics = context.get("metrics") if isinstance(context.get("metrics"), dict) else {}
            first_token_ms = metrics.get("first_token_ms")
            if first_token_ms is not None:
                try:
                    first_tokens.append(float(first_token_ms))
                except Exception:
                    pass
            if status in {"completed", "failed"}:
                done += 1
                if status == "completed":
                    success += 1
                else:
                    reason = str(row.get("failure_reason") or "unknown_failure").strip()[:80] or "unknown_failure"
                    fail_reasons[reason] = int(fail_reasons.get(reason) or 0) + 1
        success_rate = (success / done) if done else 0.0
        top_fail = sorted(fail_reasons.items(), key=lambda kv: kv[1], reverse=True)[:5]
        avg_first_token_ms = (sum(first_tokens) / len(first_tokens)) if first_tokens else 0.0
        return {
            "status": "success",
            "metrics": {
                "sample_size": len(rows),
                "success_rate": round(success_rate, 4),
                "avg_first_token_ms": round(avg_first_token_ms, 2),
                "failure_top": [{"reason": k, "count": v} for k, v in top_fail],
            },
        }
    except Exception as e:
        return internal_error("/dashboard/metrics", e)


@app.get("/orders")
def list_orders(request: Request, status: str = "pending", source: str = "frontend", limit: int = 50):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error

    try:
        safe_limit = max(1, min(int(limit or 50), 100))
        status_filters = normalize_source_filters(status)
        source_filters = normalize_source_filters(source)
        rows = select_rows_with_schema_fallback(
            "tasks",
            [
                "id",
                "status",
                "model",
                "deep_think",
                "user_id",
                "miner_name",
                "created_at",
                "claimed_at",
                "completed_at",
                "context",
            ],
            limit=min(200, safe_limit * 4),
            order_by="created_at",
            ascending=False,
        )

        filtered = []
        for row in rows:
            row_status = str(row.get("status") or "").lower()
            row_source = get_task_source(row) or "frontend"
            if status_filters and row_status not in status_filters:
                continue
            if source_filters and row_source not in source_filters:
                continue
            filtered.append(serialize_order_task(row))
            if len(filtered) >= safe_limit:
                break

        return {"status": "success", "orders": filtered}
    except Exception as e:
        return internal_error("/orders", e)


@app.get("/orders/profile")
def get_order_profile(request: Request, miner_name: str = "", hwid: str = ""):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    if not miner_name and not hwid:
        return error_response("bad_request", "Missing miner_name or hwid")

    try:
        filters = [{"op": "eq", "col": "hwid", "val": hwid}] if hwid else [{"op": "eq", "col": "miner_name", "val": miner_name}]
        miner = select_one_with_schema_fallback(
            "miners",
            [
                "miner_name",
                "status",
                "last_seen",
                "vram_gb",
                "installed_models",
                "completed_tasks",
                "failed_tasks",
            ],
            filters=filters,
        )
        if not miner:
            return {
                "status": "success",
                "profile": {
                    "miner_name": miner_name,
                    "found": False,
                    "status": "unknown",
                "vram_gb": 0,
                "installed_models": [],
                "capability_score": None,
                "capability_label": "",
                },
            }

        capability = derive_model_capability(miner.get("vram_gb"))
        return {
            "status": "success",
            "profile": {
                "miner_name": miner.get("miner_name") or miner_name,
                "found": True,
                "status": miner.get("status") or "unknown",
                "last_seen": miner.get("last_seen"),
                "vram_gb": miner.get("vram_gb") or 0,
                "installed_models": miner.get("installed_models") or [],
                "completed_tasks": miner.get("completed_tasks") or 0,
                "failed_tasks": miner.get("failed_tasks") or 0,
                "capability_score": capability["score"],
                "capability_label": capability["label"],
            },
        }
    except Exception as e:
        return internal_error("/orders/profile", e)


@app.get("/orders/local-profile")
def get_local_order_profile(request: Request):
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    effective_miner_name = derive_effective_miner_name_for_user(request_user_id)
    try:
        profile = fetch_local_miner_profile()
        if isinstance(profile, dict):
            profile = {k: v for k, v in profile.items() if k.lower() not in {"hwid", "machine_id", "fingerprint"}}
        if isinstance(profile, dict):
            current_name = str(profile.get("miner_name") or "").strip()
            if current_name:
                effective_miner_name = current_name
            profile["effective_miner_name"] = effective_miner_name
        return {"status": "success", "profile": profile, "effective_miner_name": effective_miner_name}
    except (URLError, HTTPError, TimeoutError, ValueError):
        return {
            "status": "success",
            "profile": {"found": False, "effective_miner_name": effective_miner_name},
            "effective_miner_name": effective_miner_name,
            "warning": "local_profile_unavailable",
        }
    except Exception as e:
        return internal_error("/orders/local-profile", e)


@app.post("/models/ollama")
async def manage_ollama_model(request: Request):
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    allowed, retry_after = rate_limit_guard(request, "models_ollama", limit=20, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)
    try:
        data = await request.json()
        action, model_name = parse_allowed_ollama_command(data if isinstance(data, dict) else {})
        result = await run_in_threadpool(run_allowed_ollama_command, action, model_name)
        return {
            "status": "success" if result["exit_code"] == 0 else "error",
            "action": action,
            "model": model_name,
            **result,
        }
    except subprocess.TimeoutExpired:
        return error_response(
            "timeout",
            f"Ollama command timed out after {OLLAMA_COMMAND_TIMEOUT_SECONDS}s",
        )
    except Exception as e:
        return internal_error("/models/ollama", e)


@app.get("/ollama/runtime")
def get_ollama_runtime(request: Request):
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    return get_ollama_runtime_info()


@app.post("/ollama/install")
async def install_or_repair_ollama(request: Request):
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    allowed, retry_after = rate_limit_guard(request, "ollama_install", limit=3, window_seconds=300)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)
    try:
        data = await request.json()
    except Exception:
        data = {}
    try:
        force = bool(data.get("force")) if isinstance(data, dict) else False
        result = await run_in_threadpool(download_and_launch_ollama_installer, force)
        return {"status": "success", **result}
    except Exception as e:
        return internal_error("/ollama/install", e)


@app.get("/orders/mine")
def list_my_orders(request: Request, miner_name: str, limit: int = 50):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    if not miner_name:
        return error_response("bad_request", "Missing miner_name")

    try:
        safe_limit = max(1, min(int(limit or 50), 2000))
        base_filters = [{"op": "eq", "col": "miner_name", "val": miner_name}]
        rows = select_rows_with_schema_fallback(
            "tasks",
            [
                "id",
                "status",
                "model",
                "deep_think",
                "user_id",
                "miner_name",
                "created_at",
                "claimed_at",
                "completed_at",
                "context",
            ],
            filters=base_filters,
            limit=safe_limit,
            order_by="created_at",
            ascending=False,
        )
        total_count = count_rows_with_schema_fallback("tasks", filters=base_filters)
        claimed_count = count_rows_with_schema_fallback(
            "tasks",
            filters=base_filters + [{"op": "eq", "col": "status", "val": "claimed"}],
        )
        processing_count = count_rows_with_schema_fallback(
            "tasks",
            filters=base_filters + [{"op": "eq", "col": "status", "val": "processing"}],
        )
        completed_count = count_rows_with_schema_fallback(
            "tasks",
            filters=base_filters + [{"op": "eq", "col": "status", "val": "completed"}],
        )
        failed_count = count_rows_with_schema_fallback(
            "tasks",
            filters=base_filters + [{"op": "eq", "col": "status", "val": "failed"}],
        )
        cancelled_count = count_rows_with_schema_fallback(
            "tasks",
            filters=base_filters + [{"op": "eq", "col": "status", "val": "cancelled"}],
        )
        return {
            "status": "success",
            "orders": [serialize_order_task(row) for row in rows],
            "summary": {
                "total_count": total_count,
                "claimed_count": claimed_count,
                "processing_count": processing_count,
                "active_count": claimed_count + processing_count,
                "completed_count": completed_count,
                "failed_count": failed_count,
                "cancelled_count": cancelled_count,
            },
        }
    except Exception as e:
        return internal_error("/orders/mine", e)


@app.post("/orders/claim")
async def claim_order_manually(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error

    allowed, retry_after = rate_limit_guard(request, "orders_claim_manual", limit=60, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)

    try:
        data = await request.json()
        task_id = data.get("id")
        miner_name = (data.get("miner_name") or "").strip()
        incoming_installed_models = data.get("installed_models")
        if not isinstance(incoming_installed_models, list):
            incoming_installed_models = []
        incoming_installed_models = [
            str(item).strip()
            for item in incoming_installed_models
            if str(item).strip()
        ][:200]

        if not task_id or not miner_name:
            return error_response("bad_request", "Missing id or miner_name")

        task_row = select_one_with_schema_fallback(
            "tasks",
            [
                "id",
                "status",
                "model",
                "deep_think",
                "user_id",
                "miner_name",
                "created_at",
                "claimed_at",
                "completed_at",
                "context",
            ],
            filters=[{"op": "eq", "col": "id", "val": task_id}],
        )
        if not task_row:
            return error_response("not_found", "Task not found")

        miner = select_one_with_schema_fallback(
            "miners",
            ["miner_name", "vram_gb", "installed_models"],
            filters=[{"op": "eq", "col": "miner_name", "val": miner_name}],
        )
        if not miner:
            return error_response("not_found", "Miner profile not found")
        if incoming_installed_models:
            miner["installed_models"] = incoming_installed_models
        can_run, reject_reason = validate_miner_can_run_task(miner, task_row)
        if not can_run:
            return error_response("miner_ineligible", reject_reason)

        claimed, _, old_status, new_status = try_atomic_claim(task_row, miner_name)
        if not claimed:
            return {
                "status": "error",
                "message": f"Task not claimable: {new_status or old_status or 'unknown'}",
                "code": "conflict",
            }

        claimed_row = select_one_with_schema_fallback(
            "tasks",
            [
                "id",
                "status",
                "model",
                "deep_think",
                "user_id",
                "miner_name",
                "created_at",
                "claimed_at",
                "completed_at",
                "context",
            ],
            filters=[{"op": "eq", "col": "id", "val": task_id}],
        )
        log_transition(
            "manual_claim_success",
            task_id=task_id,
            miner_name=miner_name,
            old_status=old_status,
            new_status="claimed",
        )
        return {"status": "success", "order": serialize_order_task(claimed_row or task_row)}
    except Exception as e:
        return internal_error("/orders/claim", e)


@app.post("/task")
async def create_task(request: Request):
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    allowed, retry_after = rate_limit_guard(request, "create_task", limit=40, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)
    auth_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error
    daily_allowed, daily_used = enforce_daily_task_create_limit(auth_user_id)
    if not daily_allowed:
        return error_response(
            "daily_limit_exceeded",
            "Daily task limit exceeded",
            limit=CREATE_TASK_USER_DAILY_LIMIT,
            used=daily_used,
            window_hours=24,
        )

    try:
        data = await request.json()
        prompt = (data.get("prompt") or "").strip()
        image_url = (data.get("image_url") or "").strip() or None
        model = (data.get("model") or "").strip()
        mode = (data.get("mode") or "Auto").strip() or "Auto"
        mode_normalized = mode.lower()
        deep_think = bool(data.get("deep_think", False))
        incoming_context = data.get("context") if isinstance(data.get("context"), dict) else {}

        logger.info(
            "task_create_in req_id=%s user_id=%s model=%s mode=%s deep_think=%s prompt_len=%s has_image=%s",
            request_id,
            auth_user_id,
            model,
            mode_normalized,
            deep_think,
            len(prompt),
            bool(image_url),
        )

        if not prompt:
            return error_response("bad_request", "Missing prompt")
        if not model:
            return error_response("bad_request", "Missing model")

        local_model_ready = False
        local_gpu_ready = False
        if mode_normalized in {"local", "auto"}:
            local_model_ready = await run_in_threadpool(local_ollama_has_model, model)
            if local_model_ready:
                local_gpu_ready = await run_in_threadpool(local_gpu_can_run_model, model)
        logger.info(
            "task_create_local_probe req_id=%s model=%s mode=%s local_model_ready=%s local_gpu_ready=%s",
            request_id,
            model,
            mode_normalized,
            local_model_ready,
            local_gpu_ready,
        )

        if mode_normalized == "local" and not local_model_ready:
            logger.warning("task_create_local_mode_but_model_missing req_id=%s model=%s", request_id, model)
            return error_response("local_model_missing", f"Local Ollama model is not installed: {model}")

        # Billing / routing rule:
        # - Local mode: force local (model install is required; keep current behavior).
        # - Auto mode: local only when BOTH conditions are true:
        #   1) local model installed
        #   2) local GPU VRAM can run this model
        #   Otherwise fallback to remote.
        execution_source = "frontend"
        routing_reason = "remote_default"
        if mode_normalized == "local":
            execution_source = "local"
            routing_reason = "local_forced"
        elif mode_normalized == "auto" and local_model_ready and local_gpu_ready:
            execution_source = "local"
            routing_reason = "auto_local_ready"
        elif mode_normalized == "auto" and not local_model_ready:
            routing_reason = "auto_remote_model_missing"
        elif mode_normalized == "auto" and not local_gpu_ready:
            routing_reason = "auto_remote_gpu_insufficient"
        elif mode_normalized == "remote":
            routing_reason = "remote_forced"
        execution_mode = "local" if execution_source == "local" else "remote"
        logger.info(
            "task_create_route req_id=%s model=%s mode=%s local_model_ready=%s local_gpu_ready=%s source=%s execution_mode=%s routing_reason=%s",
            request_id,
            model,
            mode_normalized,
            local_model_ready,
            local_gpu_ready,
            execution_source,
            execution_mode,
            routing_reason,
        )

        if execution_mode == "remote" and not supabase:
            if has_control_plane_proxy():
                forwarded_body = dict(data)
                forwarded_body["mode"] = mode
                return proxy_control_plane_json(request, "/task", method="POST", json_body=forwarded_body)
            return error_response("service_unavailable", "Remote control plane unavailable")

        task_id = str(uuid.uuid4())
        user_id = auth_user_id
        
        # Only reserve credits for remote mode, local mode is free
        if execution_mode != "remote":
            reserved_credits = 0.0
            logger.info("task_create_local_mode_free req_id=%s execution_mode=%s reserved_credits=0", request_id, execution_mode)
        else:
            reserved_credits = estimate_credits_from_payload(prompt, model, deep_think)
            logger.info("task_create_remote_mode_charge req_id=%s execution_mode=%s reserved_credits=%s", request_id, execution_mode, reserved_credits)
            credit_summary = compute_user_credit_summary(user_id)
            if credit_summary["available"] < reserved_credits:
                return {
                    "status": "error",
                    "message": "Insufficient credits",
                    "code": "insufficient_credits",
                    "required": reserved_credits,
                    "available": credit_summary["available"],
                }
        
        context = {
            **incoming_context,
            "model": model,
            "mode": mode,
            "execution_mode": execution_mode,
            "routing_reason": routing_reason,
            "source": execution_source,
            "billing": {
                "reserved": reserved_credits,
                "charged": 0.0,
                "state": "reserved",
                "reserved_at": datetime.now(timezone.utc).isoformat(),
            },
        }
        
        # Only record credit event for remote mode
        if execution_mode == "remote":
            context = append_billing_event(
                context,
                phase="reserved",
                direction="debit_hold",
                amount=reserved_credits,
                actor_type="requester",
                actor_id=user_id,
                note="task created",
            )
        row = {
            "id": task_id,
            "prompt": prompt,
            "image_url": image_url,
            "deep_think": deep_think,
            "status": "pending",
            "result": "",
            "result_delta": "",
            "model": model,
            "context": context,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if supabase:
            insert_row_with_schema_fallback(
                "tasks",
                row,
                optional_drop_order=[
                    "image_url",
                    "deep_think",
                    "model",
                    "context",
                    "created_at",
                ],
            )
        else:
            create_local_task_record(row)
        # Only record credit ledger event for remote mode
        if execution_mode == "remote":
            record_credit_ledger_event(
                task_id=task_id,
                phase="reserved",
                direction="debit_hold",
                amount=reserved_credits,
                actor_type="requester",
                actor_id=user_id,
                note="task created",
            )
        if execution_source == "local":
            start_local_ollama_task(task_id, prompt, model, deep_think, context)
            logger.info("task_create_local_start req_id=%s task_id=%s model=%s", request_id, task_id, model)
        else:
            logger.info("task_create_remote_queue req_id=%s task_id=%s model=%s", request_id, task_id, model)
        updated_credit_summary = compute_user_credit_summary(user_id) if supabase else build_default_credit_summary(user_id)
        return {
            "status": "success",
            "task_id": task_id,
            "trace_id": request_id,
            "credits": updated_credit_summary,
            "task": {
                "id": task_id,
                "model": model,
                "mode": mode,
                "execution_mode": execution_mode,
                "routing_reason": routing_reason,
                "deep_think": deep_think,
            },
        }
    except Exception:
        logger.exception("task_create_failed req_id=%s", request_id)
        return error_response("internal_error", "Internal server error", trace_id=request_id)


@app.get("/task/{task_id}")
def get_task(task_id: str, request: Request):
    allowed, retry_after = rate_limit_guard(request, "get_task", limit=240, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)

    try:
        task = None
        if supabase:
            task = select_one_with_schema_fallback(
                "tasks",
                [
                    "id",
                    "status",
                    "deep_think",
                    "result",
                    "result_delta",
                    "failure_reason",
                    "user_id",
                    "miner_name",
                    "claimed_at",
                    "completed_at",
                    "created_at",
                    "context",
                ],
                filters=[{"op": "eq", "col": "id", "val": task_id}],
            )
        else:
            task = get_local_task_record(task_id)
            if not task and has_control_plane_proxy():
                return proxy_control_plane_json(request, f"/task/{task_id}", method="GET")
        if not task:
            return error_response("not_found", "Task not found")
        request_user_id, auth_error = require_auth_user_id(request)
        if auth_error:
            return auth_error
        task_owner = str(task.get("user_id") or "")
        if task_owner and request_user_id != task_owner:
            return error_response("forbidden", "Forbidden")
        task.pop("user_id", None)
        task_context = task.get("context") if isinstance(task.get("context"), dict) else {}
        billing_ctx = task_context.get("billing") if isinstance(task_context.get("billing"), dict) else {}
        metrics_ctx = task_context.get("metrics") if isinstance(task_context.get("metrics"), dict) else {}
        task["context"] = {
            "model": task_context.get("model"),
            "mode": task_context.get("mode"),
            "execution_mode": task_context.get("execution_mode"),
            "routing_reason": task_context.get("routing_reason"),
            "source": task_context.get("source"),
            "billing": {
                "reserved": float(billing_ctx.get("reserved") or 0.0),
                "charged": float(billing_ctx.get("charged") or 0.0),
                "refunded": float(billing_ctx.get("refunded") or 0.0),
                "state": str(billing_ctx.get("state") or ""),
            },
            "metrics": {
                "first_token_ms": metrics_ctx.get("first_token_ms"),
            },
        }
        deep_think = bool(task.get("deep_think", False) or task_context.get("deep_think", False))
        if not deep_think:
            raw_result = str(task.get("result") or "")
            normalized_result = normalize_model_output(raw_result, False)
            if is_valid_standard_answer(normalized_result):
                task["result"] = normalized_result
            else:
                fallback = extract_fallback_answer_from_leak(raw_result)
                task["result"] = f"<answer>{fallback}</answer>" if fallback else "<answer>回答格式异常，请重新生成。</answer>"
            raw_delta = str(task.get("result_delta") or "")
            if raw_delta:
                delta_normalized = normalize_model_output(raw_delta, False)
                if is_valid_standard_answer(delta_normalized):
                    task["result_delta"] = delta_normalized
                else:
                    fallback_delta = extract_fallback_answer_from_leak(raw_delta)
                    task["result_delta"] = f"<answer>{fallback_delta}</answer>" if fallback_delta else ""
        if str(task.get("status") or "").lower() == "failed" and not task.get("failure_reason"):
            task["failure_reason"] = "unknown_failure"
        return {"status": "success", "task": task}
    except Exception as e:
        return internal_error("/task/{task_id}", e)


async def _task_stream_generator(task_id: str):
    """Generator for SSE streaming of task updates."""
    global _task_stream_queues
    
    # Get or create queue for this task
    with _task_stream_lock:
        if task_id not in _task_stream_queues:
            _task_stream_queues[task_id] = asyncio.Queue(maxsize=100)
        queue = _task_stream_queues[task_id]
    
    try:
        while True:
            try:
                # Wait for data with timeout
                data = await asyncio.wait_for(queue.get(), timeout=30.0)
                if data is None:
                    break
                # Format as SSE event
                yield f"data: {json.dumps(data)}\n\n"
                
                # If task is complete, close the stream
                if data.get("type") in ["complete", "error", "cancelled"]:
                    break
            except asyncio.TimeoutError:
                # Send keepalive
                yield ": keepalive\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        pass


@app.get("/task/{task_id}/stream")
async def stream_task(task_id: str, request: Request):
    """SSE endpoint for real-time task updates."""
    # Verify task exists and user has access
    try:
        task = None
        if supabase:
            task = select_one_with_schema_fallback(
                "tasks",
                ["id", "user_id", "status"],
                filters=[{"op": "eq", "col": "id", "val": task_id}],
            )
        else:
            task = get_local_task_record(task_id)
            if not task and has_control_plane_proxy():
                return error_response("not_supported", "Remote streaming via local gateway is not supported")
        if not task:
            return error_response("not_found", "Task not found")
        
        request_user_id, auth_error = require_auth_user_id(request)
        if auth_error:
            return auth_error
        task_owner = str(task.get("user_id") or "")
        if task_owner and request_user_id != task_owner:
            return error_response("forbidden", "Forbidden")
    except Exception as e:
        return internal_error("/task/{task_id}/stream", e)
    
    return StreamingResponse(
        _task_stream_generator(task_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.api_route("/claim", methods=["GET", "POST"])
async def claim_task(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    miner_auth_error = require_miner_auth(request)
    if miner_auth_error:
        return miner_auth_error

    if request.method == "GET":
        return error_response("method_not_allowed", "Please use POST to claim tasks")

    allowed, retry_after = rate_limit_guard(request, "claim_task", limit=120, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)

    try:
        data = await request.json()
        miner_name = data.get("miner_name", "unknown")
        accepted_sources = normalize_source_filters(data.get("accepted_sources"))
        excluded_sources = normalize_source_filters(data.get("excluded_sources"))
        try_recover_stale_tasks()
        incoming_installed_models = data.get("installed_models")
        if not isinstance(incoming_installed_models, list):
            incoming_installed_models = []
        incoming_installed_models = [
            str(item).strip()
            for item in incoming_installed_models
            if str(item).strip()
        ][:200]
        miner = select_one_with_schema_fallback(
            "miners",
            ["miner_name", "vram_gb", "installed_models"],
            filters=[{"op": "eq", "col": "miner_name", "val": miner_name}],
        ) or {
            "miner_name": miner_name,
            "vram_gb": data.get("vram_gb", 0),
            "installed_models": incoming_installed_models,
        }
        if incoming_installed_models:
            miner["installed_models"] = incoming_installed_models

        task_data = None
        assigned_task = fetch_assigned_claimed_task_for_miner(
            miner,
            miner_name,
            accepted_sources,
            excluded_sources,
        )
        if assigned_task:
            task_data = assigned_task

        if not task_data:
            task_data = fetch_pending_task_for_miner(miner, accepted_sources, excluded_sources)

        if CLAIM_USE_RPC and not task_data:
            try:
                res = supabase.rpc('v12_claim_logic', {
                    "miner_name": miner_name,
                    "score_limit": data.get("score_limit", data.get("score", 100)),
                    "can_see": data.get("can_see", True),
                    "mode": data.get("mode", "all"),
                    "gpu_count": data.get("gpu_count", 1),
                    "vram_gb": data.get("vram_gb", 0)
                }).execute()
                if res.data:
                    task_data = res.data[0]
            except Exception as rpc_error:
                logger.warning("claim rpc failed; keep direct-query path: %s", str(rpc_error))

        if task_data and not task_matches_source_filters(task_data, accepted_sources, excluded_sources):
            task_data = fetch_pending_task_for_miner(miner, accepted_sources, excluded_sources)

        if task_data:
            can_run, reject_reason = validate_miner_can_run_task(miner, task_data)
            if not can_run:
                log_transition(
                    "claim_rejected_capability",
                    task_id=task_data.get("id"),
                    miner_name=miner_name,
                    old_status=task_data.get("status"),
                    new_status=task_data.get("status"),
                    extra={"reason": reject_reason},
                )
                task_data = fetch_pending_task_for_miner(miner, accepted_sources, excluded_sources)

        if not task_data:
            log_transition(
                "claim_idle",
                task_id=None,
                miner_name=miner_name,
                old_status=None,
                new_status=None,
            )
            return {
                "status": "idle",
                "msg": "No pending tasks in pool",
                "code": "no_pending_task",
                "task_data": None
            }

        claimed, task_id, old_status, new_status = try_atomic_claim(task_data, miner_name)
        if not claimed:
            log_transition(
                "claim_race_lost",
                task_id=task_id,
                miner_name=miner_name,
                old_status=old_status,
                new_status=new_status,
            )
            return {
                "status": "idle",
                "msg": "Race lost while claiming task",
                "code": "claim_race_lost",
                "task_data": None
            }

        log_transition(
            "claim_success",
            task_id=task_id,
            miner_name=miner_name,
            old_status=old_status,
            new_status=new_status,
        )

        return {
            "status": "success",
            "task_data": task_data
        }

    except Exception as e:
        return internal_error("/claim", e)


@app.post("/submit")
async def submit_task(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    miner_auth_error = require_miner_auth(request)
    if miner_auth_error:
        return miner_auth_error

    allowed, retry_after = rate_limit_guard(request, "submit_task", limit=180, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)

    try:
        data = await request.json()
        task_id = data.get("id")
        result = data.get("result")
        result_hash = data.get("hash")
        token_count = data.get("token_count")
        first_token_ms = data.get("first_token_ms")
        miner_name = data.get("miner_name", "unknown")

        if not task_id or not result:
            return error_response("bad_request", "Missing id or result")

        old_status = get_task_status(task_id)
        if old_status in TERMINAL_TASK_STATUSES:
            if old_status == "completed":
                log_transition(
                    "submit_idempotent_completed",
                    task_id=task_id,
                    miner_name=miner_name,
                    old_status=old_status,
                    new_status=old_status,
                    token_count=token_count,
                    outcome="success",
                )
                return {"status": "success", "message": "Task already completed"}
            log_transition(
                "submit_ignored",
                task_id=task_id,
                miner_name=miner_name,
                old_status=old_status,
                new_status=old_status,
                token_count=token_count,
                outcome="ignored",
            )
            return {"status": "ignored", "message": f"Task already {old_status}"}

        task_meta = select_one_with_schema_fallback(
            "tasks",
            ["deep_think", "context", "user_id", "miner_name"],
            [{"op": "eq", "col": "id", "val": task_id}],
        ) or {}
        deep_think = bool(task_meta.get("deep_think", False))
        task_context = task_meta.get("context") if isinstance(task_meta.get("context"), dict) else {}
        task_user_id = str(task_meta.get("user_id") or "").strip()
        task_miner_name = str(task_meta.get("miner_name") or "").strip()
        raw_result = str(result or "")
        result = normalize_model_output(raw_result, deep_think)
        if not deep_think and not is_valid_standard_answer(result):
            fallback = extract_fallback_answer_from_leak(raw_result)
            result = f"<answer>{fallback}</answer>" if fallback else "<answer>回答格式异常，请重新生成。</answer>"
        result_hash = result_hash or str(uuid.uuid5(uuid.NAMESPACE_DNS, result))
        remote_execution = is_remote_execution(task_context)
        current_billing = extract_billing(task_context)
        charged_credits = 0.0
        if remote_execution:
            charged_credits = float(
                current_billing["reserved"]
                or estimate_credits_from_payload(result, task_context.get("model") or "", deep_think)
            )
        settle_state = "charged" if remote_execution else "local_completed"
        next_context = build_billing_context_on_settle(
            task_context,
            next_state=settle_state,
            charged=charged_credits,
        )
        if remote_execution:
            next_context = append_billing_event(
                next_context,
                phase="charged",
                direction="debit_final",
                amount=charged_credits,
                actor_type="requester",
                actor_id=task_user_id,
                note="task completed",
            )
        reward_miner_name = str(miner_name or task_miner_name).strip()
        if reward_miner_name:
            next_context = append_billing_event(
                next_context,
                phase="rewarded",
                direction="credit",
                amount=MINER_REWARD_CREDIT,
                actor_type="miner",
                actor_id=reward_miner_name,
                note="task reward",
            )
        metrics = next_context.get("metrics") if isinstance(next_context.get("metrics"), dict) else {}
        if first_token_ms is not None:
            try:
                metrics["first_token_ms"] = float(first_token_ms)
                next_context["metrics"] = metrics
            except Exception:
                pass

        safe_update_with_fallback(
            "tasks",
            {
                "status": "completed",
                "result": result,
                "verification_hash": result_hash,
                "result_hash": result_hash,
                "token_count": token_count,
                "result_delta": "",
                "failure_reason": None,
                "context": next_context,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            filters=[
                {"op": "eq", "col": "id", "val": task_id},
                {"op": "in", "col": "status", "val": ACTIVE_TASK_STATUSES},
            ],
            optional_drop_order=["failure_reason"],
        )

        current_status = get_task_status(task_id)
        if current_status != "completed":
            if not current_status:
                return error_response("not_found", "Task not found")
            return error_response("conflict", f"Task not submittable: {current_status}")

        if remote_execution:
            record_credit_ledger_event(
                task_id=task_id,
                phase="charged",
                direction="debit_final",
                amount=charged_credits,
                actor_type="requester",
                actor_id=task_user_id,
                note="task completed",
            )
        if reward_miner_name:
            record_credit_ledger_event(
                task_id=task_id,
                phase="rewarded",
                direction="credit",
                amount=MINER_REWARD_CREDIT,
                actor_type="miner",
                actor_id=reward_miner_name,
                note="task reward",
            )

        supabase.rpc("increment_miner_tasks", {
            "p_miner_name": miner_name,
            "p_success": True
        }).execute()

        log_transition(
            "submit_success",
            task_id=task_id,
            miner_name=miner_name,
            old_status=old_status,
            new_status="completed",
            token_count=token_count,
            outcome="success",
        )
        return {"status": "success", "message": "Task submitted"}

    except Exception as e:
        return internal_error("/submit", e)


@app.post("/fail")
async def fail_task(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    miner_auth_error = require_miner_auth(request)
    if miner_auth_error:
        return miner_auth_error

    allowed, retry_after = rate_limit_guard(request, "fail_task", limit=180, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)

    try:
        data = await request.json()
        task_id = data.get("id")
        miner_name = data.get("miner_name", "unknown")
        reason_text = str(data.get("reason") or "miner_reported_failure").strip()[:240]

        if not task_id:
            return error_response("bad_request", "Missing id")

        old_status = get_task_status(task_id)
        if old_status in TERMINAL_TASK_STATUSES:
            log_transition(
                "fail_ignored",
                task_id=task_id,
                miner_name=miner_name,
                old_status=old_status,
                new_status=old_status,
                outcome="ignored",
            )
            return {"status": "ignored", "message": f"Task already {old_status}"}

        task_meta = select_one_with_schema_fallback(
            "tasks",
            ["context", "user_id"],
            [{"op": "eq", "col": "id", "val": task_id}],
        ) or {}
        task_user_id = str(task_meta.get("user_id") or "").strip()
        task_context = task_meta.get("context") if isinstance(task_meta.get("context"), dict) else {}
        remote_execution = is_remote_execution(task_context)
        refund_credits = float(extract_billing(task_context)["reserved"] or 0.0) if remote_execution else 0.0
        settle_state = "refunded" if remote_execution else "local_failed"
        next_context = build_billing_context_on_settle(
            task_context,
            next_state=settle_state,
        )
        if remote_execution and refund_credits > 0:
            next_context = append_billing_event(
                next_context,
                phase="refunded",
                direction="credit",
                amount=refund_credits,
                actor_type="requester",
                actor_id=task_user_id,
                note=reason_text or "task failed",
            )

        safe_update_with_fallback(
            "tasks",
            {
                "status": "failed",
                "result_delta": "",
                "failure_reason": reason_text,
                "context": next_context,
            },
            filters=[
                {"op": "eq", "col": "id", "val": task_id},
                {"op": "in", "col": "status", "val": ACTIVE_TASK_STATUSES},
            ],
            optional_drop_order=["failure_reason"],
        )

        current_status = get_task_status(task_id)
        if current_status != "failed":
            if not current_status:
                return error_response("not_found", "Task not found")
            return error_response("conflict", f"Task not fail-able: {current_status}")

        if remote_execution and refund_credits > 0:
            record_credit_ledger_event(
                task_id=task_id,
                phase="refunded",
                direction="credit",
                amount=refund_credits,
                actor_type="requester",
                actor_id=task_user_id,
                note=reason_text or "task failed",
            )

        supabase.rpc("increment_miner_tasks", {
            "p_miner_name": miner_name,
            "p_success": False
        }).execute()

        log_transition(
            "fail_success",
            task_id=task_id,
            miner_name=miner_name,
            old_status=old_status,
            new_status="failed",
            outcome="success",
        )
        return {"status": "success"}

    except Exception as e:
        return internal_error("/fail", e)


@app.post("/cancel")
async def cancel_task(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")

    allowed, retry_after = rate_limit_guard(request, "cancel_task", limit=120, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)
    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error

    try:
        data = await request.json()
        task_id = data.get("id")

        if not task_id:
            return error_response("bad_request", "Missing id")

        owner_row = select_one_with_schema_fallback(
            "tasks",
            ["id", "user_id", "status"],
            filters=[{"op": "eq", "col": "id", "val": task_id}],
        )
        if not owner_row:
            return error_response("not_found", "Task not found")
        if str(owner_row.get("user_id") or "") != request_user_id:
            return error_response("forbidden", "Forbidden")

        old_status = str(owner_row.get("status") or "") or get_task_status(task_id)
        if old_status in TERMINAL_TASK_STATUSES:
            return {"status": "ignored", "message": f"Task already {old_status}"}

        filters = [
            {"op": "eq", "col": "id", "val": task_id},
            {"op": "in", "col": "status", "val": ACTIVE_TASK_STATUSES},
        ]
        task_meta = select_one_with_schema_fallback(
            "tasks",
            ["context", "user_id"],
            [{"op": "eq", "col": "id", "val": task_id}],
        ) or {}
        task_user_id = str(task_meta.get("user_id") or "").strip()
        task_context = task_meta.get("context") if isinstance(task_meta.get("context"), dict) else {}
        remote_execution = is_remote_execution(task_context)
        refund_credits = float(extract_billing(task_context)["reserved"] or 0.0) if remote_execution else 0.0
        settle_state = "cancelled" if remote_execution else "local_cancelled"
        next_context = build_billing_context_on_settle(
            task_context,
            next_state=settle_state,
        )
        if remote_execution and refund_credits > 0:
            next_context = append_billing_event(
                next_context,
                phase="cancelled",
                direction="credit",
                amount=refund_credits,
                actor_type="requester",
                actor_id=task_user_id,
                note="task cancelled by user",
            )

        safe_update_with_fallback(
            "tasks",
            {
                "status": "cancelled",
                "result_delta": "",
                "context": next_context,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            filters=filters,
        )

        current_status = get_task_status(task_id)
        if current_status != "cancelled":
            if not current_status:
                return error_response("not_found", "Task not found")
            return error_response("conflict", f"Task not cancellable: {current_status}")

        if remote_execution and refund_credits > 0:
            record_credit_ledger_event(
                task_id=task_id,
                phase="cancelled",
                direction="credit",
                amount=refund_credits,
                actor_type="requester",
                actor_id=task_user_id,
                note="task cancelled by user",
            )

        log_transition(
            "cancel_success",
            task_id=task_id,
            miner_name=request_user_id or "user",
            old_status=old_status,
            new_status="cancelled",
            outcome="success",
        )
        return {"status": "success"}
    except Exception as e:
        return internal_error("/cancel", e)


@app.get("/billing/reconcile/{task_id}")
def reconcile_task_billing(task_id: str, request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")

    allowed, retry_after = rate_limit_guard(request, "billing_reconcile", limit=240, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)

    request_user_id, auth_error = require_auth_user_id(request)
    if auth_error:
        return auth_error

    try:
        task = select_one_with_schema_fallback(
            "tasks",
            ["id", "user_id", "miner_name", "status", "context", "created_at", "completed_at"],
            filters=[{"op": "eq", "col": "id", "val": task_id}],
        )
        if not task:
            return error_response("not_found", "Task not found")
        if str(task.get("user_id") or "") != request_user_id:
            return error_response("forbidden", "Forbidden")

        task_context = task.get("context") if isinstance(task.get("context"), dict) else {}
        billing_ctx = task_context.get("billing") if isinstance(task_context.get("billing"), dict) else {}
        context_events = billing_ctx.get("events") if isinstance(billing_ctx.get("events"), list) else []
        context_events = sorted(context_events, key=lambda item: str(item.get("ts") or ""))

        ledger_events = []
        try:
            ledger_events = select_rows_with_schema_fallback(
                "credit_ledger",
                ["task_id", "phase", "direction", "amount", "actor_type", "actor_id", "note", "created_at"],
                filters=[{"op": "eq", "col": "task_id", "val": task_id}],
                limit=300,
                order_by="created_at",
                ascending=True,
            )
        except Exception:
            ledger_events = []

        phases = [str(item.get("phase") or "") for item in context_events]
        has_reserved = "reserved" in phases
        has_charged = "charged" in phases
        has_refunded = ("refunded" in phases) or ("cancelled" in phases)
        task_status = str(task.get("status") or "").lower()
        state_ok = has_reserved and (
            (task_status == "completed" and has_charged)
            or (task_status in {"failed", "cancelled"} and has_refunded)
            or (task_status in {"pending", "claimed", "processing"})
        )

        return {
            "status": "success",
            "reconcile": {
                "task_id": task_id,
                "task_status": task_status,
                "request_user_id": request_user_id,
                "miner_name": task.get("miner_name") or "",
                "billing": {
                    "reserved": float(billing_ctx.get("reserved") or 0.0),
                    "charged": float(billing_ctx.get("charged") or 0.0),
                    "refunded": float(billing_ctx.get("refunded") or 0.0),
                    "state": str(billing_ctx.get("state") or ""),
                },
                "state_ok": bool(state_ok),
                "context_events": context_events,
                "ledger_events": ledger_events,
                "created_at": task.get("created_at"),
                "completed_at": task.get("completed_at"),
            },
        }
    except Exception as e:
        return internal_error("/billing/reconcile/{task_id}", e)


@app.post("/heartbeat")
async def heartbeat(request: Request):
    if not supabase:
        return error_response("service_unavailable", "Supabase credentials missing")
    miner_auth_error = require_miner_auth(request)
    if miner_auth_error:
        return miner_auth_error

    allowed, retry_after = rate_limit_guard(request, "heartbeat", limit=360, window_seconds=60)
    if not allowed:
        return error_response("rate_limited", "Rate limit exceeded", retry_after=retry_after)

    try:
        data = await request.json()
        miner_name = data.get("miner_name")
        hwid = data.get("hwid")
        gpu_count = data.get("gpu_count", 1)
        vram_value = data.get("vram_gb", data.get("vram", 0))
        installed_models = data.get("installed_models")
        if not isinstance(installed_models, list):
            installed_models = []
        installed_models = [str(item).strip() for item in installed_models if str(item).strip()][:200]

        if not miner_name:
            return error_response("bad_request", "Missing miner_name")

        payload = {
            "miner_name": miner_name,
            "status": "active",
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "vram_gb": vram_value,
            "gpu_count": gpu_count,
            "hwid": hwid,
            "installed_models": installed_models,
        }
        existing = select_one_with_schema_fallback(
            "miners",
            ["miner_name"],
            filters=[{"op": "eq", "col": "miner_name", "val": miner_name}],
        )
        if existing:
            safe_update_with_fallback(
                "miners",
                payload,
                filters=[{"op": "eq", "col": "miner_name", "val": miner_name}],
                optional_drop_order=["gpu_count", "hwid", "installed_models"],
            )
        else:
            insert_row_with_schema_fallback("miners", payload)

        return {"status": "ok"}

    except Exception as e:
        return internal_error("/heartbeat", e)


if __name__ == "__main__":
    import uvicorn
    import threading

    def ensure_default_models_installed():
        """Ensure default models are installed in Ollama (runs in background)."""
        for model in DEFAULT_MODELS:
            if not local_ollama_has_model(model):
                logger.info("Pulling default model: %s", model)
                try:
                    run_allowed_ollama_command("pull", model)
                    logger.info("Successfully pulled default model: %s", model)
                except Exception as e:
                    logger.error("Failed to pull default model %s: %s", model, e)

    # Start model download in background thread
    threading.Thread(target=ensure_default_models_installed, daemon=True).start()

    host = os.environ.get("GATEWAY_HOST", "127.0.0.1")
    port = int(os.environ.get("GATEWAY_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
