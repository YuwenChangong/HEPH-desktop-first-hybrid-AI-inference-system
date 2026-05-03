# HEPH Cloudflare Control Plane

This service replaces the Render-hosted Python control plane for remote task coordination.

## What it provides

- `POST /auth/supabase/session`
- `GET /credits/me`
- `POST /task`
- `GET /task/:id`
- `POST /cancel`
- `GET /orders`
- `GET /orders/mine`
- `GET /orders/profile`
- `POST /orders/claim`
- `GET /orders/stream` (SSE snapshots)
- `GET /healthz`

## Required env vars

Set these with `wrangler secret put` (or dashboard secrets):

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_KEY` (service role key)

Optional vars (defined in `wrangler.toml` defaults):

- `TASK_LEASE_TIMEOUT_SECONDS` default `250`
- `PENDING_TASK_TIMEOUT_SECONDS` default `250`
- `ORDER_STREAM_POLL_SECONDS` default `1`
- `DEFAULT_USER_CREDITS` default `0`
- `MESSAGE_CREDIT_COST` default `0.1`
- `MINER_REWARD_CREDIT` default `0.1`

## Deploy

```bash
cd cloudflare-control-plane
npm install
npx wrangler login
npx wrangler secret put SUPABASE_URL
npx wrangler secret put SUPABASE_ANON_KEY
npx wrangler secret put SUPABASE_KEY
npx wrangler deploy
```

After deploy, copy the worker URL and set desktop clients:

`CONTROL_PLANE_BASE_URL=https://<your-worker>.<subdomain>.workers.dev`

## Client mode config

Desktop `gateway.env` must not include platform admin secrets. Use:

```env
SUPABASE_URL=...
SUPABASE_ANON_KEY=...
CONTROL_PLANE_BASE_URL=https://<worker-url>
AUTH_SECRET=<local-random>
```
