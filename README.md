# Kepto ingest service

Resolves a saved social link to real media, uploads it to Cloudflare R2,
and calls the Kepto Worker back. The Worker never downloads video and the
iOS app never uploads — this service is the only place that touches bytes.

Contract (matches `src/worker/index.ts` in `sadboizx/Kepto`):

```
Worker  -> POST {INGEST_URL}  { item_id, url }   Bearer INGEST_TOKEN
Ingest  -> POST {KEPTO_WORKER_URL}/api/ingest-complete
           success: { item_id, r2_key, thumb_key?, content_type?,
                      duration?, width?, height?, file_size?, sha256? }
           failure: { item_id, error }
```

R2 layout: `users/me/items/<itemId>/original.<ext>` + `thumb.jpg`
(single-user MVP, `user_id` is `me` — same as the Worker).

## Resolution order

1. **Cobalt** — the user's own instance (`COBALT_URL`, default
   `https://cobalt.up.railway.app/`), `videoQuality: max`, `downloadMode:
   auto`. Tunnel/redirect URL is downloaded directly; picker takes the
   first video item. Age-restricted posts fail here with
   `error.api.content.post.age` — Cobalt runs anonymously, retrying
   will not help.
2. **yt-dlp fallback** — only if Cobalt fails. Downloads directly to a
   file, so HLS/m3u8 sources work. Needs the `yt-dlp` binary (installed
   in the Docker image).

## Run locally

```sh
cp .env.example .env   # fill in the secrets, never commit .env
npm install
npm run build
npm start
```

## Deploy (Railway)

1. Create a repo (suggested `sadboizx/kepto-ingest`) and push this folder.
   The Worker repo stays separate — the ingest contract says so.
2. Railway → New Project → Deploy from GitHub repo → pick the repo.
   Railway detects the `Dockerfile` automatically.
3. Set Variables (all of them; nothing secret goes in code):
   - `INGEST_TOKEN` — same value as the Worker's `INGEST_TOKEN` secret
     (`wrangler secret put INGEST_TOKEN` in the Kepto repo).
   - `KEPTO_WORKER_URL` — the deployed Worker URL, e.g.
     `https://kepto.<your-subdomain>.workers.dev` (no trailing slash).
   - `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` —
     from Cloudflare dashboard → R2 → Manage R2 API Tokens
     (needs Object Read & Write on the `kepto-media` bucket).
   - `R2_BUCKET=kepto-media`
   - `COBALT_URL=https://cobalt.up.railway.app/`
   - `COBALT_API_KEY` — from Railway → Cobalt service → Variables
     (the same key the Shortcuts use).
4. Railway gives the service a public domain — that URL plus `/ingest`
   **is** `INGEST_URL`. It is a plain var in the Worker's
   `wrangler.toml`, not a secret: set `INGEST_URL` to the Railway URL
   (with the `/ingest` path) and `npx wrangler deploy`.
5. Test end to end: `POST /api/save` with a public TikTok link, then
   `GET /api/items/:id` should go `pending` → `ready`.

## Endpoints

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/health` | No auth. Shows `active`, `cobalt_configured`, `r2_configured`. |
| POST | `/ingest` | Bearer `INGEST_TOKEN`. `{ item_id, url }` → 202. Runs async. |

A non-2xx from `/ingest` makes the Worker mark the item failed, so
this service only returns 4xx for bad auth/input — download failures
are reported through the `/api/ingest-complete` callback instead.

## Limits (MVP)

- Max 2 concurrent jobs; a 3rd save returns `ingest_busy_try_again`
  via the callback (the Worker marks it failed — retry the save).
- No auth on `/health`, no per-user isolation yet — same single-user
  scope as the Worker MVP.
- yt-dlp fallback needs cookies/session for login-walled posts and
  Stories; that is a later step, not wired here.
