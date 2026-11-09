/**
 * Kepto ingest service.
 *
 * The Worker (kepto) POSTs { item_id, url } here with Bearer INGEST_TOKEN.
 * This service resolves the link to real media, uploads it to R2, and calls
 * the Worker back at POST /api/ingest-complete — success or failure.
 *
 * Resolution order: Cobalt (user's own instance) first, yt-dlp fallback.
 * The Worker never downloads video and the iOS app never uploads; this
 * service is the only place that touches the media bytes.
 *
 * Per job, besides the media it also produces:
 * - a thumbnail (thumb.jpg, frame at 1s)
 * - a scrub-preview sprite (sprite.jpg, one frame every 5s tiled 5x4)
 * - author metadata: the creator's handle (from the resolved URL, e.g.
 *   vt.tiktok.com redirects to tiktok.com/@handle/video/..., or from
 *   yt-dlp's uploader_id) + display name / title / caption / posted date
 *   from yt-dlp --dump-json when it works. Metadata is best-effort:
 *   it never fails a job whose media already downloaded.
 *
 * Env (see .env.example — no secrets in code):
 *   INGEST_TOKEN, KEPTO_WORKER_URL, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID,
 *   R2_SECRET_ACCESS_KEY, R2_BUCKET, COBALT_URL, COBALT_API_KEY,
 *   YTDLP_BIN, PORT
 */
import express from "express";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveWithCobalt } from "./cobalt.js";
import { downloadToFile, extFor, makeSprite, makeThumbnail, probe } from "./media.js";
import { createR2Client, uploadFile } from "./r2.js";
import { downloadWithYtdlp, dumpJsonMetadata, ytdlpAvailable } from "./ytdlp.js";

const PORT = Number(process.env.PORT ?? 3000);
const INGEST_TOKEN = process.env.INGEST_TOKEN ?? "";
const KEPTO_WORKER_URL = (process.env.KEPTO_WORKER_URL ?? "").replace(/\/$/, "");
const R2_BUCKET = process.env.R2_BUCKET ?? "kepto-media";
const COBALT_URL = process.env.COBALT_URL ?? "https://cobalt.up.railway.app/";
const COBALT_API_KEY = process.env.COBALT_API_KEY ?? "";
const YTDLP_BIN = process.env.YTDLP_BIN ?? "yt-dlp";
const MAX_CONCURRENT = 2;

/** Scrub sprite geometry — must match what the iOS player expects. */
const SPRITE_INTERVAL_SECONDS = 5;
const SPRITE_COLS = 5;
const SPRITE_ROWS = 4;

const r2 =
  process.env.R2_ACCOUNT_ID &&
  process.env.R2_ACCESS_KEY_ID &&
  process.env.R2_SECRET_ACCESS_KEY
    ? createR2Client({
        accountId: process.env.R2_ACCOUNT_ID,
        accessKeyId: process.env.R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
        bucket: R2_BUCKET,
      })
    : null;

if (!INGEST_TOKEN) console.warn("[kepto-ingest] INGEST_TOKEN is empty — /ingest will reject everything.");
if (!KEPTO_WORKER_URL) console.warn("[kepto-ingest] KEPTO_WORKER_URL is empty — callbacks will fail.");
if (!r2) console.warn("[kepto-ingest] R2 credentials missing — uploads will fail.");

const app = express();
app.use(express.json({ limit: "16kb" }));

let active = 0;
const activeIds = new Set<string>();

function authed(req: express.Request): boolean {
  if (!INGEST_TOKEN) return false;
  return req.header("Authorization") === `Bearer ${INGEST_TOKEN}`;
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "kepto-ingest",
    active,
    cobalt_configured: Boolean(COBALT_URL),
    r2_configured: Boolean(r2),
  });
});

/**
 * Called by the Worker. Returns 202 immediately; the job runs in the
 * background and reports back via /api/ingest-complete. A non-2xx here
 * makes the Worker mark the item failed, so only return errors for
 * bad auth / bad input — never for a download failure (that goes
 * through the callback instead).
 */
app.post("/ingest", (req, res) => {
  if (!authed(req)) return res.status(401).json({ ok: false, error: "Unauthorized." });
  const { item_id, url } = (req.body ?? {}) as { item_id?: unknown; url?: unknown };
  if (typeof item_id !== "string" || !item_id) {
    return res.status(422).json({ ok: false, error: "item_id is required." });
  }
  if (typeof url !== "string" || !/^https?:\/\//i.test(url)) {
    return res.status(422).json({ ok: false, error: "url must be http(s)." });
  }
  if (activeIds.has(item_id)) {
    return res.json({ ok: true, item_id, status: "already_running" });
  }
  activeIds.add(item_id);
  void runJob(item_id, url).finally(() => activeIds.delete(item_id));
  return res.status(202).json({ ok: true, item_id, status: "accepted" });
});

async function callback(body: Record<string, unknown>): Promise<void> {
  if (!KEPTO_WORKER_URL) throw new Error("KEPTO_WORKER_URL not set");
  const res = await fetch(`${KEPTO_WORKER_URL}/api/ingest-complete`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${INGEST_TOKEN}`,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(`callback_http_${res.status}`);
}

interface GatheredMeta {
  authorUsername?: string;
  author?: string;
  title?: string;
  caption?: string;
  postedAt?: string | null;
}

/**
 * Creator handle from the URL itself: short links (vt.tiktok.com/...) redirect
 * to the canonical tiktok.com/@handle/video/... URL, and full TikTok links
 * already contain @handle. Follows up to 3 redirects without downloading
 * anything; any failure returns null.
 */
async function resolveUsernameFromUrl(pageUrl: string): Promise<string | null> {
  try {
    let current = pageUrl;
    for (let hop = 0; hop < 3; hop++) {
      const res = await fetch(current, {
        redirect: "manual",
        signal: AbortSignal.timeout(10_000),
      });
      const loc = res.headers.get("location");
      if (!loc) break;
      current = new URL(loc, current).toString();
    }
    const m = /@([A-Za-z0-9._]{1,30})/.exec(current);
    return m ? m[1] : null;
  } catch {
    return null;
  }
}

/**
 * Author + title metadata for a saved link. Runs the URL-redirect check and
 * yt-dlp --dump-json in parallel; both are best-effort (a platform that
 * blocks yt-dlp from a datacenter IP just yields no metadata). Never rejects.
 */
async function gatherMetadata(pageUrl: string): Promise<GatheredMeta> {
  const [username, yt] = await Promise.all([
    resolveUsernameFromUrl(pageUrl),
    dumpJsonMetadata(pageUrl, YTDLP_BIN),
  ]);
  let postedAt: string | null = null;
  if (yt.uploadDate && /^\d{8}$/.test(yt.uploadDate)) {
    postedAt = `${yt.uploadDate.slice(0, 4)}-${yt.uploadDate.slice(4, 6)}-${yt.uploadDate.slice(6, 8)}T00:00:00Z`;
  }
  return {
    authorUsername: username ?? yt.uploaderId,
    author: yt.uploader,
    title: yt.title,
    caption: yt.description?.slice(0, 500),
    postedAt,
  };
}

async function runJob(itemId: string, pageUrl: string): Promise<void> {
  active++;
  const dir = await mkdtemp(join(tmpdir(), "kepto-"));
  try {
    if (active > MAX_CONCURRENT) {
      throw new Error("ingest_busy_try_again");
    }
    if (!r2) throw new Error("r2_not_configured");
    console.log(JSON.stringify({ event: "ingest_start", item_id: itemId, url: pageUrl }));

    // Metadata runs in parallel with the download (started now, awaited
    // later) so it adds no latency in the common case.
    const metaPromise = gatherMetadata(pageUrl);

    // 1. Resolve to a direct media URL, then download it — or let yt-dlp
    //    download it directly (needed for HLS / login-walled sources).
    let mediaPath: string;
    let contentType: string | null;
    let fileSize: number;
    let sha256: string;
    let cobaltFilename: string | undefined;
    try {
      const resolved = await resolveWithCobalt(pageUrl, COBALT_URL, COBALT_API_KEY);
      cobaltFilename = resolved.filename;
      const ext = extFor(null, resolved.filename);
      const dl = await downloadToFile(resolved.url, join(dir, `media.${ext}`));
      mediaPath = dl.path;
      contentType = dl.contentType;
      fileSize = dl.fileSize;
      sha256 = dl.sha256;
    } catch (cobaltErr) {
      console.log(
        JSON.stringify({ event: "cobalt_failed", item_id: itemId, error: String(cobaltErr) }),
      );
      if (!(await ytdlpAvailable(YTDLP_BIN))) {
        throw new Error(`cobalt_failed_no_ytdlp: ${String(cobaltErr)}`);
      }
      mediaPath = await downloadWithYtdlp(pageUrl, dir, YTDLP_BIN);
      const st = await stat(mediaPath);
      fileSize = st.size;
      contentType = null;
      // Hash the yt-dlp output for dedupe/verification.
      sha256 = await new Promise<string>((resolve, reject) => {
        const h = createHash("sha256");
        const s = createReadStream(mediaPath);
        s.on("data", (c: Buffer | string) => h.update(c));
        s.on("end", () => resolve(h.digest("hex")));
        s.on("error", reject);
      });
    }

    // 2. Probe + thumbnail + scrub sprite (thumbnail/sprite failure is non-fatal).
    const meta = await probe(mediaPath);
    const gathered = await metaPromise;
    const ext = extFor(contentType, cobaltFilename) || mediaPath.split(".").pop() || "mp4";
    const thumbPath = join(dir, "thumb.jpg");
    const hasThumb = await makeThumbnail(mediaPath, thumbPath, 1);
    const r2Key = `users/me/items/${itemId}/original.${ext}`;
    const thumbKey = hasThumb ? `users/me/items/${itemId}/thumb.jpg` : null;
    const spritePath = join(dir, "sprite.jpg");
    const sprite = await makeSprite(
      mediaPath,
      spritePath,
      meta.duration ?? null,
      SPRITE_INTERVAL_SECONDS,
      SPRITE_COLS,
      SPRITE_ROWS,
    );
    const previewKey = sprite.ok ? `users/me/items/${itemId}/sprite.jpg` : null;

    // 3. Upload straight to R2, then tell the Worker the item is ready.
    await uploadFile(r2, R2_BUCKET, r2Key, mediaPath, contentType ?? "video/mp4");
    if (hasThumb && thumbKey) {
      await uploadFile(r2, R2_BUCKET, thumbKey, thumbPath, "image/jpeg");
    }
    if (sprite.ok && previewKey) {
      await uploadFile(r2, R2_BUCKET, previewKey, spritePath, "image/jpeg");
    }
    console.log(JSON.stringify({ event: "ingest_ready", item_id: itemId, r2_key: r2Key, file_size: fileSize, has_audio: meta.hasAudio ?? null, sprite_ok: sprite.ok, sprite_frames: sprite.count }));
    await callback({
      item_id: itemId,
      r2_key: r2Key,
      thumb_key: thumbKey,
      preview_key: previewKey,
      content_type: contentType ?? "video/mp4",
      title: gathered.title ?? null,
      caption: gathered.caption ?? null,
      author: gathered.author ?? null,
      author_username: gathered.authorUsername ?? null,
      posted_at: gathered.postedAt ?? null,
      duration: meta.duration ?? null,
      width: meta.width ?? null,
      height: meta.height ?? null,
      file_size: fileSize,
      sha256,
      preview_cols: sprite.ok ? SPRITE_COLS : null,
      preview_rows: sprite.ok ? SPRITE_ROWS : null,
      preview_interval: sprite.ok ? SPRITE_INTERVAL_SECONDS : null,
      preview_count: sprite.ok ? sprite.count : null,
    });
  } catch (e) {
    const message = String(e instanceof Error ? e.message : e).slice(0, 500);
    console.error(JSON.stringify({ event: "ingest_failed", item_id: itemId, error: message }));
    try {
      await callback({ item_id: itemId, error: message });
    } catch (cbErr) {
      console.error(
        JSON.stringify({ event: "callback_failed", item_id: itemId, error: String(cbErr) }),
      );
    }
  } finally {
    active--;
    await rm(dir, { recursive: true, force: true });
  }
}

app.listen(PORT, () => {
  console.log(`[kepto-ingest] listening on :${PORT}`);
});
