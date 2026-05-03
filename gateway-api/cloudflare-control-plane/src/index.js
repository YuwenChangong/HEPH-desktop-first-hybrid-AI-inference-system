const JSON_HEADERS = { "content-type": "application/json; charset=utf-8" };

function json(payload, status = 200, headers = {}) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { ...JSON_HEADERS, ...headers },
  });
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeUserId(v) {
  return String(v || "").trim().toLowerCase();
}

function getBearerToken(request) {
  const auth = String(request.headers.get("authorization") || "").trim();
  if (auth.toLowerCase().startsWith("bearer ")) {
    return auth.slice(7).trim();
  }
  return "";
}

function parseJsonSafe(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function numberEnv(env, key, fallback) {
  const raw = env[key];
  if (raw === undefined || raw === null || raw === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

function escapeFilterValue(v) {
  return String(v).replaceAll(",", "\\,");
}

async function supabaseRequest(env, path, init = {}) {
  const url = `${String(env.SUPABASE_URL || "").replace(/\/+$/, "")}${path}`;
  const headers = new Headers(init.headers || {});
  const apiKey = env.SUPABASE_KEY || env.SUPABASE_ANON_KEY || "";
  if (!headers.has("apikey")) headers.set("apikey", apiKey);
  if (!headers.has("authorization")) headers.set("authorization", `Bearer ${apiKey}`);
  const method = init.method || "GET";
  if (init.body && !headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }
  const response = await fetch(url, { ...init, method, headers });
  const text = await response.text();
  const body = text ? parseJsonSafe(text) ?? text : null;
  return { ok: response.ok, status: response.status, body };
}

async function verifySupabaseAccessToken(env, token) {
  if (!token) return null;
  const base = String(env.SUPABASE_URL || "").replace(/\/+$/, "");
  const anon = env.SUPABASE_ANON_KEY || "";
  if (!base || !anon) return null;
  const res = await fetch(`${base}/auth/v1/user`, {
    headers: {
      apikey: anon,
      authorization: `Bearer ${token}`,
    },
  });
  if (!res.ok) return null;
  const user = await res.json().catch(() => null);
  return user && user.id ? user : null;
}

function unauthorized() {
  return json({ status: "error", message: "Unauthorized", code: "unauthorized" }, 401);
}

async function requireUser(env, request) {
  const token = getBearerToken(request);
  const user = await verifySupabaseAccessToken(env, token);
  if (!user) return { error: unauthorized() };
  return { user, token, userId: normalizeUserId(user.id) };
}

async function selectOneByUserId(env, table, userId) {
  const q = `?select=*&user_id=eq.${encodeURIComponent(userId)}&limit=1`;
  const res = await supabaseRequest(env, `/rest/v1/${table}${q}`);
  if (!res.ok || !Array.isArray(res.body) || !res.body.length) return null;
  return res.body[0];
}

async function ensureCreditAccount(env, userId) {
  const existing = await selectOneByUserId(env, "user_credits", userId);
  if (existing) return existing;
  const inserted = await supabaseRequest(env, "/rest/v1/user_credits?select=*", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: JSON.stringify({
      user_id: userId,
      total_credits: numberEnv(env, "DEFAULT_USER_CREDITS", 0),
      used_credits: 0,
      reserved_credits: 0,
      updated_at: nowIso(),
    }),
  });
  if (inserted.ok && Array.isArray(inserted.body) && inserted.body[0]) return inserted.body[0];
  return null;
}

async function creditsSummary(env, userId) {
  const row = await ensureCreditAccount(env, userId);
  if (!row) {
    return {
      user_id: userId,
      total: numberEnv(env, "DEFAULT_USER_CREDITS", 0),
      spent: 0,
      reserved: 0,
      available: numberEnv(env, "DEFAULT_USER_CREDITS", 0),
      tasks: { completed: 0, failed: 0, cancelled: 0, active: 0 },
      miner_earned: 0,
    };
  }
  const total = Number(row.total_credits || 0);
  const spent = Number(row.used_credits || 0);
  const reserved = Number(row.reserved_credits || 0);
  const available = total - spent - reserved;
  return {
    user_id: userId,
    total,
    spent,
    reserved,
    available,
    tasks: { completed: 0, failed: 0, cancelled: 0, active: 0 },
    miner_earned: 0,
  };
}

function billingContext(reserved, source, mode, executionMode) {
  return {
    model: null,
    mode,
    execution_mode: executionMode,
    source,
    billing: {
      reserved,
      charged: 0,
      refunded: 0,
      state: "reserved",
      reserved_at: nowIso(),
      events: [
        {
          ts: nowIso(),
          phase: "reserved",
          direction: "debit_hold",
          amount: reserved,
          actor_type: "requester",
          actor_id: null,
          note: "task created",
        },
      ],
    },
    metrics: {
      first_token_ms: null,
    },
  };
}

function responseTaskRow(row) {
  const context = row.context && typeof row.context === "object" ? row.context : {};
  const billing = context.billing && typeof context.billing === "object" ? context.billing : {};
  const metrics = context.metrics && typeof context.metrics === "object" ? context.metrics : {};
  return {
    id: row.id,
    status: row.status,
    deep_think: !!row.deep_think,
    result: row.result || "",
    result_delta: row.result_delta || "",
    failure_reason: row.failure_reason || "",
    miner_name: row.miner_name || "",
    claimed_at: row.claimed_at || null,
    completed_at: row.completed_at || null,
    created_at: row.created_at || null,
    context: {
      model: context.model || row.model || "",
      mode: context.mode || "",
      execution_mode: context.execution_mode || "remote",
      routing_reason: context.routing_reason || "",
      source: context.source || "frontend",
      billing: {
        reserved: Number(billing.reserved || 0),
        charged: Number(billing.charged || 0),
        refunded: Number(billing.refunded || 0),
        state: String(billing.state || ""),
      },
      metrics: {
        first_token_ms: metrics.first_token_ms ?? null,
      },
    },
  };
}

async function createRemoteTask(env, userId, body) {
  const prompt = String(body.prompt || "").trim();
  const model = String(body.model || "").trim();
  const mode = String(body.mode || "Remote").trim();
  const deepThink = !!body.deep_think;
  if (!prompt) return json({ status: "error", message: "Missing prompt", code: "bad_request" }, 400);
  if (!model) return json({ status: "error", message: "Missing model", code: "bad_request" }, 400);

  const reserved = numberEnv(env, "MESSAGE_CREDIT_COST", 0.1);
  const summary = await creditsSummary(env, userId);
  if (summary.available < reserved) {
    return json(
      {
        status: "error",
        message: "Insufficient credits",
        code: "insufficient_credits",
        required: reserved,
        available: summary.available,
      },
      400,
    );
  }

  const taskId = crypto.randomUUID();
  const ctx = billingContext(reserved, "frontend", mode, "remote");
  ctx.model = model;
  ctx.deep_think = deepThink;

  const insertRes = await supabaseRequest(env, "/rest/v1/tasks?select=id,model,status,context,created_at", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: JSON.stringify({
      id: taskId,
      prompt,
      image_url: body.image_url || null,
      deep_think: deepThink,
      status: "pending",
      result: "",
      result_delta: "",
      model,
      context: ctx,
      user_id: userId,
      created_at: nowIso(),
    }),
  });
  if (!insertRes.ok || !Array.isArray(insertRes.body) || !insertRes.body[0]) {
    return json({ status: "error", message: "Failed to create task", code: "db_error", detail: insertRes.body }, 500);
  }

  await supabaseRequest(
    env,
    `/rest/v1/user_credits?user_id=eq.${encodeURIComponent(userId)}`,
    {
      method: "PATCH",
      headers: { Prefer: "return=minimal" },
      body: JSON.stringify({
        reserved_credits: Number(summary.reserved + reserved),
        updated_at: nowIso(),
      }),
    },
  );
  await supabaseRequest(env, "/rest/v1/credit_ledger", {
    method: "POST",
    body: JSON.stringify({
      task_id: taskId,
      phase: "reserved",
      direction: "debit_hold",
      amount: reserved,
      actor_type: "requester",
      actor_id: userId,
      note: "task created",
      created_at: nowIso(),
    }),
  });

  const updatedCredits = await creditsSummary(env, userId);
  return json({
    status: "success",
    task_id: taskId,
    credits: updatedCredits,
    task: {
      id: taskId,
      model,
      mode,
      execution_mode: "remote",
      routing_reason: "remote_forced",
      deep_think: deepThink,
    },
  });
}

async function getTaskById(env, taskId) {
  const q = `?select=*&id=eq.${encodeURIComponent(taskId)}&limit=1`;
  const res = await supabaseRequest(env, `/rest/v1/tasks${q}`);
  if (!res.ok || !Array.isArray(res.body) || !res.body.length) return null;
  return res.body[0];
}

async function listOrders(env, status = "pending", source = "frontend", limit = 50) {
  const q =
    `?select=id,model,deep_think,status,context,user_id,miner_name,claimed_at,completed_at,created_at` +
    `&status=eq.${encodeURIComponent(status)}` +
    `&order=created_at.asc` +
    `&limit=${Math.max(1, Math.min(Number(limit) || 50, 200))}`;
  const res = await supabaseRequest(env, `/rest/v1/tasks${q}`);
  if (!res.ok || !Array.isArray(res.body)) return [];
  return res.body
    .filter((r) => {
      const ctx = r.context && typeof r.context === "object" ? r.context : {};
      return String(ctx.source || "frontend") === source;
    })
    .map((row) => {
      const ctx = row.context && typeof row.context === "object" ? row.context : {};
      return {
        id: row.id,
        status: row.status,
        model: row.model || ctx.model || "",
        deep_think: !!row.deep_think,
        user_id: row.user_id || "",
        miner_name: row.miner_name || "",
        claimed_at: row.claimed_at || null,
        completed_at: row.completed_at || null,
        created_at: row.created_at || null,
        context: ctx,
      };
    });
}

function serializeOrderTask(task) {
  const context = task.context && typeof task.context === "object" ? task.context : {};
  return {
    id: task.id,
    status: task.status || "pending",
    model: task.model || context.model || "",
    deep_think: !!task.deep_think,
    user_id: task.user_id || "",
    miner_name: task.miner_name || "",
    claimed_at: task.claimed_at || null,
    completed_at: task.completed_at || null,
    created_at: task.created_at || null,
    vram_required_gb: modelToVramRequired(task.model || context.model || ""),
    source: context.source || "frontend",
  };
}

function modelToVramRequired(modelName) {
  const m = String(modelName || "").toLowerCase();
  if (m.includes("27b")) return 6;
  if (m.includes("14b")) return 6;
  if (m.includes("9b")) return 4;
  if (m.includes("7b")) return 4;
  if (m.includes("4b")) return 3;
  if (m.includes("2b")) return 2;
  return 2;
}

function minerCanRunTask(miner, task) {
  const installed = Array.isArray(miner.installed_models) ? miner.installed_models.map((x) => String(x)) : [];
  const model = String(task.model || "");
  if (!installed.includes(model)) {
    return { ok: false, reason: "model_not_installed" };
  }
  const need = modelToVramRequired(model);
  const vram = Number(miner.vram_gb || 0);
  if (vram && vram < need) {
    return { ok: false, reason: "vram_insufficient" };
  }
  return { ok: true };
}

async function claimTaskAtomic(env, taskId, minerName) {
  const now = nowIso();
  const patch = await supabaseRequest(
    env,
    `/rest/v1/tasks?id=eq.${encodeURIComponent(taskId)}&status=in.(pending,claimed,processing)&select=id,status,model,deep_think,user_id,miner_name,created_at,claimed_at,completed_at,context`,
    {
      method: "PATCH",
      headers: { Prefer: "return=representation" },
      body: JSON.stringify({
        status: "claimed",
        miner_name: minerName,
        claimed_at: now,
      }),
    },
  );
  if (!patch.ok || !Array.isArray(patch.body) || !patch.body.length) {
    return null;
  }
  return patch.body[0];
}

async function getMinerProfile(env, minerName) {
  const q = `?select=miner_name,vram_gb,installed_models&miner_name=eq.${encodeURIComponent(minerName)}&limit=1`;
  const res = await supabaseRequest(env, `/rest/v1/miners${q}`);
  if (!res.ok || !Array.isArray(res.body) || !res.body.length) return null;
  return res.body[0];
}

async function staleRecovery(env) {
  const leaseSec = numberEnv(env, "TASK_LEASE_TIMEOUT_SECONDS", 250);
  const pendingSec = numberEnv(env, "PENDING_TASK_TIMEOUT_SECONDS", 250);
  const now = Date.now();

  const claimedRows = await supabaseRequest(
    env,
    "/rest/v1/tasks?select=id,status,claimed_at,created_at&status=in.(claimed,processing)&order=claimed_at.asc&limit=200",
  );
  if (claimedRows.ok && Array.isArray(claimedRows.body)) {
    for (const row of claimedRows.body) {
      const ts = new Date(row.claimed_at || row.created_at || 0).getTime();
      if (!Number.isFinite(ts) || ts <= 0) continue;
      if (now - ts < leaseSec * 1000) continue;
      await supabaseRequest(
        env,
        `/rest/v1/tasks?id=eq.${encodeURIComponent(row.id)}`,
        {
          method: "PATCH",
          body: JSON.stringify({
            status: "pending",
            miner_name: null,
            claimed_at: null,
            failure_reason: "stale_lease_timeout",
          }),
        },
      );
    }
  }

  const pendingRows = await supabaseRequest(
    env,
    "/rest/v1/tasks?select=id,status,created_at&status=eq.pending&order=created_at.asc&limit=300",
  );
  if (pendingRows.ok && Array.isArray(pendingRows.body)) {
    for (const row of pendingRows.body) {
      const ts = new Date(row.created_at || 0).getTime();
      if (!Number.isFinite(ts) || ts <= 0) continue;
      if (now - ts < pendingSec * 1000) continue;
      await supabaseRequest(
        env,
        `/rest/v1/tasks?id=eq.${encodeURIComponent(row.id)}`,
        {
          method: "PATCH",
          body: JSON.stringify({
            status: "cancelled",
            completed_at: nowIso(),
            failure_reason: "pending_task_timeout",
          }),
        },
      );
    }
  }
}

async function createSupabaseSession(env, request) {
  const token = getBearerToken(request);
  const user = await verifySupabaseAccessToken(env, token);
  if (!user) {
    return json({ status: "error", message: "Invalid Supabase session token", code: "unauthorized" }, 401);
  }
  const userId = normalizeUserId(user.id);
  const credits = await creditsSummary(env, userId);
  return json({
    status: "success",
    session: {
      user_id: userId,
      token,
      expires_in: 60 * 60 * 24 * 30,
    },
    supabase_user: {
      id: userId,
      email: user.email || null,
      phone: user.phone || null,
      user_metadata: user.user_metadata || {},
    },
    credits,
  });
}

async function handleCreditsMe(env, request) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  const credits = await creditsSummary(env, auth.userId);
  return json({ status: "success", credits });
}

async function handleOrdersList(env, request, url) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  await staleRecovery(env);
  const status = String(url.searchParams.get("status") || "pending");
  const source = String(url.searchParams.get("source") || "frontend");
  const limit = Number(url.searchParams.get("limit") || 50);
  const rows = await listOrders(env, status, source, limit);
  return json({ status: "success", orders: rows.map(serializeOrderTask) });
}

async function handleOrdersMine(env, request, url) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  const minerName = String(url.searchParams.get("miner_name") || "").trim();
  if (!minerName) return json({ status: "error", message: "Missing miner_name", code: "bad_request" }, 400);
  const limit = Math.max(1, Math.min(Number(url.searchParams.get("limit") || 50), 200));
  const q =
    `?select=id,status,model,deep_think,user_id,miner_name,claimed_at,completed_at,created_at,context,failure_reason` +
    `&miner_name=eq.${encodeURIComponent(minerName)}` +
    `&order=created_at.desc&limit=${limit}`;
  const res = await supabaseRequest(env, `/rest/v1/tasks${q}`);
  const rows = res.ok && Array.isArray(res.body) ? res.body : [];
  return json({
    status: "success",
    orders: rows.map((r) => ({ ...serializeOrderTask(r), failure_reason: r.failure_reason || "", first_token_ms: r.context?.metrics?.first_token_ms ?? null })),
  });
}

async function handleOrdersProfile(env, request, url) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  const minerName = String(url.searchParams.get("miner_name") || "").trim();
  if (!minerName) return json({ status: "error", message: "Missing miner_name", code: "bad_request" }, 400);
  const miner = await getMinerProfile(env, minerName);
  if (!miner) return json({ status: "error", message: "Miner profile not found", code: "not_found" }, 404);
  return json({ status: "success", profile: miner });
}

async function handleOrdersClaim(env, request) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  await staleRecovery(env);
  const body = await request.json().catch(() => ({}));
  const taskId = String(body.id || "").trim();
  const minerName = String(body.miner_name || "").trim();
  if (!taskId || !minerName) {
    return json({ status: "error", message: "Missing id or miner_name", code: "bad_request" }, 400);
  }

  const task = await getTaskById(env, taskId);
  if (!task) return json({ status: "error", message: "Task not found", code: "not_found" }, 404);
  const miner = await getMinerProfile(env, minerName);
  if (!miner) return json({ status: "error", message: "Miner profile not found", code: "not_found" }, 404);
  if (Array.isArray(body.installed_models) && body.installed_models.length) {
    miner.installed_models = body.installed_models.map((x) => String(x));
  }
  const can = minerCanRunTask(miner, task);
  if (!can.ok) {
    return json({ status: "error", message: can.reason, code: "miner_ineligible" }, 409);
  }

  const claimed = await claimTaskAtomic(env, taskId, minerName);
  if (!claimed) {
    return json({ status: "error", message: "Task not claimable", code: "conflict" }, 409);
  }
  return json({ status: "success", order: serializeOrderTask(claimed) });
}

async function handleTaskCreate(env, request) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  await staleRecovery(env);
  const body = await request.json().catch(() => ({}));
  return createRemoteTask(env, auth.userId, body);
}

async function handleTaskGet(env, request, taskId) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  const row = await getTaskById(env, taskId);
  if (!row) return json({ status: "error", message: "Task not found", code: "not_found" }, 404);
  if (normalizeUserId(row.user_id) !== auth.userId) {
    return json({ status: "error", message: "Forbidden", code: "forbidden" }, 403);
  }
  return json({ status: "success", task: responseTaskRow(row) });
}

async function handleCancel(env, request) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  const body = await request.json().catch(() => ({}));
  const taskId = String(body.task_id || body.id || "").trim();
  const reason = String(body.reason || "cancelled_by_user").slice(0, 80);
  if (!taskId) return json({ status: "error", message: "Missing task_id", code: "bad_request" }, 400);
  const row = await getTaskById(env, taskId);
  if (!row) return json({ status: "error", message: "Task not found", code: "not_found" }, 404);
  if (normalizeUserId(row.user_id) !== auth.userId) {
    return json({ status: "error", message: "Forbidden", code: "forbidden" }, 403);
  }
  if (["completed", "failed", "cancelled"].includes(String(row.status || "").toLowerCase())) {
    return json({ status: "success", task_id: taskId, already_terminal: true });
  }
  const patch = await supabaseRequest(env, `/rest/v1/tasks?id=eq.${encodeURIComponent(taskId)}&select=id,status,context`, {
    method: "PATCH",
    headers: { Prefer: "return=representation" },
    body: JSON.stringify({
      status: "cancelled",
      completed_at: nowIso(),
      failure_reason: reason,
    }),
  });
  if (!patch.ok) return json({ status: "error", message: "Cancel failed", code: "db_error" }, 500);
  return json({ status: "success", task_id: taskId });
}

async function handleOrdersStream(env, request, url) {
  const auth = await requireUser(env, request);
  if (auth.error) return auth.error;
  const status = String(url.searchParams.get("status") || "pending");
  const source = String(url.searchParams.get("source") || "frontend");
  const limit = Number(url.searchParams.get("limit") || 30);
  const pollSec = Math.max(0.5, numberEnv(env, "ORDER_STREAM_POLL_SECONDS", 1));

  const stream = new ReadableStream({
    start(controller) {
      let stopped = false;
      let lastPayload = "";
      const encoder = new TextEncoder();

      const writeEvent = (event, dataObj) => {
        const payload = typeof dataObj === "string" ? dataObj : JSON.stringify(dataObj);
        controller.enqueue(encoder.encode(`event: ${event}\ndata: ${payload}\n\n`));
      };

      const loop = async () => {
        while (!stopped) {
          try {
            await staleRecovery(env);
            const orders = await listOrders(env, status, source, limit);
            const snapshot = {
              status: "success",
              orders: orders.map(serializeOrderTask),
              profile: { found: false, effective_miner_name: "" },
              my_orders: [],
              metrics: {
                success_rate: 100,
                avg_first_token_ms: null,
                top_failure_reason: "",
                mine_total: 0,
                mine_active: 0,
                mine_completed: 0,
                mine_failed: 0,
                local_models: 0,
              },
              ts: nowIso(),
            };
            const serialized = JSON.stringify(snapshot);
            if (serialized !== lastPayload) {
              lastPayload = serialized;
              writeEvent("snapshot", snapshot);
            } else {
              writeEvent("ping", {});
            }
          } catch (e) {
            writeEvent("error", {
              status: "error",
              message: `orders stream failed: ${String(e?.message || e)}`,
              code: "stream_error",
            });
          }
          await new Promise((resolve) => setTimeout(resolve, pollSec * 1000));
        }
      };
      loop();

      request.signal.addEventListener("abort", () => {
        stopped = true;
        try {
          controller.close();
        } catch {}
      });
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    },
  });
}

function checkEnv(env) {
  const missing = [];
  for (const key of ["SUPABASE_URL", "SUPABASE_ANON_KEY", "SUPABASE_KEY"]) {
    if (!String(env[key] || "").trim()) missing.push(key);
  }
  return missing;
}

export default {
  async fetch(request, env) {
    const missing = checkEnv(env);
    if (missing.length) {
      return json(
        {
          status: "error",
          message: `Missing env: ${missing.join(", ")}`,
          code: "misconfigured",
        },
        500,
      );
    }

    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/healthz") {
      return json({
        status: "success",
        service: "heph-cloudflare-control-plane",
        health: "ok",
        time: nowIso(),
      });
    }

    if (request.method === "POST" && path === "/auth/supabase/session") {
      return createSupabaseSession(env, request);
    }

    if (request.method === "GET" && path === "/credits/me") {
      return handleCreditsMe(env, request);
    }

    if (request.method === "GET" && path === "/orders") {
      return handleOrdersList(env, request, url);
    }

    if (request.method === "GET" && path === "/orders/stream") {
      return handleOrdersStream(env, request, url);
    }

    if (request.method === "GET" && path === "/orders/mine") {
      return handleOrdersMine(env, request, url);
    }

    if (request.method === "GET" && path === "/orders/profile") {
      return handleOrdersProfile(env, request, url);
    }

    if (request.method === "POST" && path === "/orders/claim") {
      return handleOrdersClaim(env, request);
    }

    if (request.method === "POST" && path === "/task") {
      return handleTaskCreate(env, request);
    }

    if (request.method === "POST" && path === "/cancel") {
      return handleCancel(env, request);
    }

    if (request.method === "GET" && path.startsWith("/task/")) {
      const m = path.match(/^\/task\/([^\/]+)$/);
      if (m) return handleTaskGet(env, request, m[1]);
    }

    return json({ status: "error", message: "Not found", code: "not_found" }, 404);
  },
};
