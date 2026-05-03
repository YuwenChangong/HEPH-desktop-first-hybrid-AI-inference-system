# HEPH Control Plane Migration (Render -> Cloudflare)

## Scope

Migrate remote coordination APIs from Render to Cloudflare Workers:

- `/auth/supabase/session`
- `/credits/me`
- `/task`
- `/task/{id}`
- `/cancel`
- `/orders`
- `/orders/stream`
- `/orders/mine`
- `/orders/profile`
- `/orders/claim`
- `/healthz`

## 1) Deploy Worker

```powershell
Set-Location D:\project\python\cloudflare-control-plane
npm install
npx wrangler login
npx wrangler secret put SUPABASE_URL
npx wrangler secret put SUPABASE_ANON_KEY
npx wrangler secret put SUPABASE_KEY
npx wrangler deploy
```

## 2) Switch client gateway to Cloudflare control plane

Client config file:

- `C:\Users\1\AppData\Local\HEPH\config\gateway.env`
- fallback legacy path: `C:\Users\1\AppData\Local\heph\config\gateway.env`

Use client-mode values (no platform admin secrets):

```env
SUPABASE_URL=https://<your-project>.supabase.co
SUPABASE_ANON_KEY=<anon-key>
CONTROL_PLANE_BASE_URL=https://<worker>.<subdomain>.workers.dev
LOG_LEVEL=INFO
CLAIM_USE_RPC=0
TASK_LEASE_TIMEOUT_SECONDS=250
PENDING_TASK_TIMEOUT_SECONDS=250
ORDER_STREAM_POLL_SECONDS=1
LOCAL_OLLAMA_NUM_CTX=1024
LOCAL_OLLAMA_NUM_BATCH=8
```

## 3) Restart local runtime

```powershell
powershell -ExecutionPolicy Bypass -File D:\project\python\ops\start-gateway.ps1
powershell -ExecutionPolicy Bypass -File D:\project\python\ops\start-miner.ps1
powershell -ExecutionPolicy Bypass -File D:\project\python\ops\start-frontend.ps1
```

## 4) Verify

```powershell
Invoke-RestMethod https://<worker>.<subdomain>.workers.dev/healthz
```

Then in app:

1. Login
2. Send one `Remote` task
3. Check task appears in orders quickly
4. Claim task from orders page
5. Verify status transitions from `pending -> claimed -> processing/completed`

## 5) Rollback

Set `CONTROL_PLANE_BASE_URL` back to previous control plane URL and restart gateway.
