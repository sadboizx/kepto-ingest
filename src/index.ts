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
 *   TIKTOK_COOKIES — optional: the full Netscape cookies.txt content
 *   of a throwaway TikTok account. When set, yt-dlp runs with
 *   --cookies so it can list age-restricted / login-walled collections
 *   and download age-restricted videos. Never log its content.
 */
import express from "express";
import { createHash } from "node:crypto";
import { chmodSync, createReadStream, createWriteStream, writeFileSync } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveWithCobalt, type CobaltResult } from "./cobalt.js";
import { downloadToFile, extFor, makeSlideshow, makeSprite, makeThumbnail, normalizeImageToJpeg, probe } from "./media.js";
import { createR2Client, uploadFile } from "./r2.js";
import { resolveWithTikwm } from "./tikwm.js";
import { downloadWithYtdlp, dumpJsonMetadata, expandPlaylist, ytdlpAvailable, ytdlpVersion } from "./ytdlp.js";

const PORT = Number(process.env.PORT ?? 3000);
const INGEST_TOKEN = process.env.INGEST_TOKEN ?? "";
const KEPTO_WORKER_URL = (process.env.KEPTO_WORKER_URL ?? "").replace(/\/$/, "");
const R2_BUCKET = process.env.R2_BUCKET ?? "kepto-media";
const COBALT_URL = process.env.COBALT_URL ?? "https://cobalt.up.railway.app/";
const COBALT_API_KEY = process.env.COBALT_API_KEY ?? "";
// Second Cobalt instance, tried only when the own instance fails (e.g.
// TikTok serving the own instance's IP a challenge page). The public
// pybalt proxy routes to many instances on different IPs and needs no
// API key; leave empty to disable the fallback.
const COBALT_FALLBACK_URL =
  process.env.COBALT_FALLBACK_URL ?? "https://dwnld.nichind.dev/";

/**
 * Resolve a link with Cobalt, trying the own instance first and the
 * public fallback proxy second. Different instances run on different
 * IPs, so a TikTok challenge/block on one IP does not doom the job.
 * Logs each failure with its instance label; throws the last error.
 */
async function resolveWithCobaltChain(
  pageUrl: string,
  itemId: string,
): Promise<CobaltResult> {
  const instances: { url: string; key: string; label: string }[] = [
    { url: COBALT_URL, key: COBALT_API_KEY, label: "own" },
  ];
  if (COBALT_FALLBACK_URL) {
    instances.push({ url: COBALT_FALLBACK_URL, key: "", label: "fallback" });
  }
  let lastError: unknown = new Error("cobalt_no_instance");
  for (const inst of instances) {
    try {
      const result = await resolveWithCobalt(pageUrl, inst.url, inst.key);
      if (inst.label !== "own") {
        console.log(
          JSON.stringify({ event: "cobalt_fallback_used", item_id: itemId, instance: inst.label }),
        );
      }
      return result;
    } catch (e) {
      lastError = e;
      console.log(
        JSON.stringify({ event: "cobalt_failed", item_id: itemId, instance: inst.label, error: String(e) }),
      );
    }
  }
  throw lastError;
}

const YTDLP_BIN = process.env.YTDLP_BIN ?? "yt-dlp";
const MAX_CONCURRENT = 2;

// TikTok session cookies for a throwaway account (Netscape cookies.txt
// content, pasted into the TIKTOK_COOKIES env var). Written to a 0600
// file at startup and passed to yt-dlp as --cookies; Railway redeploys
// on env-var change, so a refreshed cookie takes effect on the next
// deploy. The content is a live session credential — it is never
// logged, and only its presence is reported (health + startup log).
// Without it, yt-dlp runs anonymously as before.
let COOKIES_FILE: string | null = null;
const cookiesContent = (process.env.TIKTOK_COOKIES ?? "").trim();
if (cookiesContent) {
  try {
    const p = join(tmpdir(), "tiktok-cookies.txt");
    writeFileSync(p, cookiesContent.endsWith("\n") ? cookiesContent : cookiesContent + "\n", {
      mode: 0o600,
    });
    chmodSync(p, 0o600);
    COOKIES_FILE = p;
  } catch (e) {
    console.log(JSON.stringify({ event: "cookies_write_failed", error: String(e).slice(0, 200) }));
  }
}

/**
 * Scrub sprite geometry. One frame per second, up to 100 frames
 * (10 columns, rows as needed), so the preview follows the finger in
 * real time. The old 5s interval gave a 10s video only 2 frames — the
 * popup barely changed while scrubbing. Longer videos stretch the
 * interval so the sprite never exceeds 100 frames.
 */
const SPRITE_MAX_FRAMES = 100;
const SPRITE_COLS = 10;

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
// Jobs waiting for a free slot. The Worker fires all of a collection's
// ingest calls at once (fire-and-forget); with MAX_CONCURRENT = 2 the
// 3rd+ job used to be rejected with "ingest_busy_try_again" and the
// item marked failed. Now it queues instead — a job is only started
// when a slot frees up, so no job is ever lost to capacity.
const queue: Array<{ itemId: string; pageUrl: string }> = [];

function pumpQueue(): void {
  while (active < MAX_CONCURRENT && queue.length > 0) {
    const job = queue.shift()!;
    void runJob(job.itemId, job.pageUrl).finally(() => {
      activeIds.delete(job.itemId);
      pumpQueue();
    });
  }
}

function authed(req: express.Request): boolean {
  if (!INGEST_TOKEN) return false;
  return req.header("Authorization") === `Bearer ${INGEST_TOKEN}`;
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "kepto-ingest",
    active,
    queued: queue.length,
    cobalt_configured: Boolean(COBALT_URL),
    r2_configured: Boolean(r2),
    cookies_configured: Boolean(COOKIES_FILE),
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
  if (active < MAX_CONCURRENT) {
    void runJob(item_id, url).finally(() => {
      activeIds.delete(item_id);
      pumpQueue();
    });
    return res.status(202).json({ ok: true, item_id, status: "accepted" });
  }
  // At capacity: queue it instead of rejecting. The job starts when a
  // slot frees up; the Worker still gets 202, so the item stays pending
  // (not failed) until the job runs and reports back.
  queue.push({ itemId: item_id, pageUrl: url });
  console.log(JSON.stringify({ event: "ingest_queued", item_id, queue_length: queue.length }));
  return res.status(202).json({ ok: true, item_id, status: "queued" });
});

/**
 * Expand a collection/playlist URL into its video URLs (for "save all
 * videos from a TikTok Collection"). Called by the Worker, which then
 * saves each URL as its own item. Synchronous: yt-dlp --flat-playlist
 * takes ~5-30s for a collection. Auth: Bearer INGEST_TOKEN (same as
 * /ingest). Returns { ok, title, urls, count }.
 */
app.post("/expand-collection", async (req, res) => {
  if (!authed(req)) return res.status(401).json({ ok: false, error: "Unauthorized." });
  const { url } = (req.body ?? {}) as { url?: unknown };
  if (typeof url !== "string" || !/^https?:\/\//i.test(url)) {
    return res.status(422).json({ ok: false, error: "url must be http(s)." });
  }
  try {
    const expanded = await expandPlaylist(url, YTDLP_BIN, 100, 120_000, COOKIES_FILE);
    console.log(
      JSON.stringify({
        event: "expand_collection",
        url,
        count: expanded.urls.length,
        title: expanded.title ?? null,
      }),
    );
    return res.json({
      ok: true,
      title: expanded.title ?? null,
      urls: expanded.urls,
      count: expanded.urls.length,
    });
  } catch (e) {
    const message = String(e instanceof Error ? e.message : e).slice(0, 300);
    console.log(JSON.stringify({ event: "expand_collection_failed", url, error: message }));
    return res.status(422).json({ ok: false, error: message });
  }
});

/**
 * POST /upload — receive a raw file upload from the device Albums.
 * The Worker (not the app) calls this: the app uploads to the Worker,
 * which streams the body through with Bearer <redacted> Query:
 * item_id (a pending "album" item the Worker created), filename.
 * The body is the raw file bytes (Content-Type = the file's MIME type).
 * The file is streamed to disk (never buffered in memory), probed,
 * thumbnailed (+ a scrub sprite for videos), uploaded to R2, and the
 * Worker is called back at /api/ingest-complete. Synchronous: the
 * Worker (and the app) wait for the whole process and get the
 * definitive result. Auth: Bearer <redacted> (same as /ingest).
 */
app.post("/upload", async (req, res) => {
  if (!authed(req)) return res.status(401).json({ ok: false, error: "Unauthorized." });
  const itemId = typeof req.query.item_id === "string" ? req.query.item_id : "";
  const filename = typeof req.query.filename === "string" ? req.query.filename : "upload";
  if (!itemId) return res.status(422).json({ ok: false, error: "item_id is required." });
  const contentType = req.headers["content-type"] ?? "application/octet-stream";
  const dir = await mkdtemp(join(tmpdir(), "kepto-upload-"));
  const ext = extFor(contentType, filename) || "mp4";
  const mediaPath = join(dir, `upload.${ext}`);
  try {
    // Stream the request body straight to disk — the file can be tens
    // of MB, so it must not be buffered in memory. express.json() only
    // parses application/json, so a file Content-Type reaches us as a
    // raw stream here.
    await new Promise<void>((resolve, reject) => {
      const ws = createWriteStream(mediaPath);
      req.pipe(ws);
      ws.on("finish", resolve);
      ws.on("error", reject);
      req.on("error", reject);
    });
    const st = await stat(mediaPath);
    if (st.size === 0) throw new Error("empty_upload");
    const sha256 = await new Promise<string>((resolve, reject) => {
      const h = createHash("sha256");
      const s = createReadStream(mediaPath);
      s.on("data", (c: Buffer | string) => h.update(c));
      s.on("end", () => resolve(h.digest("hex")));
      s.on("error", reject);
    });
    await runUpload(itemId, mediaPath, filename, contentType, st.size, sha256, dir);
    return res.json({ ok: true, item_id: itemId, status: "ready" });
  } catch (e) {
    const message = String(e instanceof Error ? e.message : e).slice(0, 500);
    console.error(JSON.stringify({ event: "upload_failed", item_id: itemId, error: message }));
    try {
      await callback({ item_id: itemId, error: message });
    } catch {
      // The Worker marks the item failed from the 422 below when the
      // callback itself fails; nothing more to do here.
    }
    return res.status(422).json({ ok: false, error: message });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
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
  viewCount?: number | null;
  duration?: number | null;
  images?: string[];
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
    dumpJsonMetadata(pageUrl, YTDLP_BIN, 15_000, COOKIES_FILE),
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
    viewCount: yt.viewCount ?? null,
    duration: yt.duration ?? null,
    images: yt.images,
  };
}

/**
 * TikWM only understands TikTok URLs (tiktok.com incl. vt/vm short
 * links, and Douyin). A Facebook/Instagram/YouTube link that reaches
 * the TikWM fallback (Cobalt + yt-dlp both failed) is NOT a TikTok
 * link — calling TikWM just produces the confusing
 * "tikwm_Url parsing is failed! Please check url." error. Callers check
 * this first and surface the real (Cobalt/yt-dlp) error instead.
 */
function isTikTokUrl(pageUrl: string): boolean {
  try {
    const host = new URL(pageUrl).hostname.toLowerCase();
    return host === "tiktok.com" || host.endsWith(".tiktok.com") || host.includes("douyin.com");
  } catch {
    return false;
  }
}

/**
 * Finish an uploaded file (from POST /upload): probe it, build a
 * thumbnail (+ a scrub sprite for videos), upload the original +
 * thumbnail (+ sprite) to R2, and call the Worker back so the item
 * becomes ready. Images keep their original file (content_type
 * image/*) and the thumbnail IS the image itself (normalized to JPEG,
 * so the feed's full-image view gets a real JPEG); videos get a 1s
 * thumbnail + a sprite exactly like a link ingest. Throws on failure
 * (the /upload handler reports it to the Worker).
 */
async function runUpload(
  itemId: string,
  mediaPath: string,
  filename: string,
  contentType: string,
  fileSize: number,
  sha256: string,
  dir: string,
): Promise<void> {
  if (!r2) throw new Error("r2_not_configured");
  const isImage =
    /^image\//i.test(contentType) ||
    /\.(jpe?g|png|webp|gif|heic|heif|avif)$/i.test(filename);
  const meta = await probe(mediaPath);
  if (!meta.width || !meta.height) {
    throw new Error("no_video_stream");
  }
  const ext = extFor(contentType, filename) || mediaPath.split(".").pop() || "mp4";
  const r2Key = `users/me/items/${itemId}/original.${ext}`;
  const thumbPath = join(dir, "thumb.jpg");
  let thumbKey: string | null = null;
  let previewKey: string | null = null;
  let previewCols: number | null = null;
  let previewRows: number | null = null;
  let previewInterval: number | null = null;
  let previewCount: number | null = null;
  if (isImage) {
    // The thumbnail is the image itself (normalized to JPEG). No sprite
    // for a still. The thumbnail is load-bearing here: the feed's
    // full-image view loads /api/thumb/:id, so a missing thumb means a
    // ready item that shows nothing — fail the job instead.
    const hasThumb = await normalizeImageToJpeg(mediaPath, thumbPath);
    if (!hasThumb) throw new Error("thumbnail_failed");
    thumbKey = `users/me/items/${itemId}/thumb.jpg`;
  } else {
    const hasThumb = await makeThumbnail(mediaPath, thumbPath, 1);
    if (hasThumb) thumbKey = `users/me/items/${itemId}/thumb.jpg`;
    const spritePath = join(dir, "sprite.jpg");
    const spriteInterval =
      meta.duration && meta.duration > 0
        ? Math.max(1, meta.duration / SPRITE_MAX_FRAMES)
        : 1;
    const spriteCount =
      meta.duration && meta.duration > 0
        ? Math.min(Math.ceil(meta.duration / spriteInterval), SPRITE_MAX_FRAMES)
        : SPRITE_MAX_FRAMES;
    const spriteRows = Math.ceil(spriteCount / SPRITE_COLS);
    const sprite = await makeSprite(
      mediaPath,
      spritePath,
      meta.duration ?? null,
      spriteInterval,
      SPRITE_COLS,
      spriteRows,
    );
    if (sprite.ok) {
      previewKey = `users/me/items/${itemId}/sprite.jpg`;
      previewCols = SPRITE_COLS;
      previewRows = spriteRows;
      previewInterval = spriteInterval;
      previewCount = sprite.count;
      await uploadFile(r2, R2_BUCKET, previewKey, spritePath, "image/jpeg");
    }
  }
  await uploadFile(r2, R2_BUCKET, r2Key, mediaPath, contentType);
  if (thumbKey) {
    await uploadFile(r2, R2_BUCKET, thumbKey, thumbPath, "image/jpeg");
  }
  console.log(
    JSON.stringify({
      event: "upload_ready",
      item_id: itemId,
      r2_key: r2Key,
      file_size: fileSize,
      is_image: isImage,
    }),
  );
  await callback({
    item_id: itemId,
    r2_key: r2Key,
    thumb_key: thumbKey,
    preview_key: previewKey,
    content_type: contentType,
    title: filename,
    caption: null,
    author: null,
    author_username: null,
    posted_at: null,
    view_count: null,
    duration: meta.duration ?? null,
    width: meta.width ?? null,
    height: meta.height ?? null,
    file_size: fileSize,
    sha256,
    preview_cols: previewCols,
    preview_rows: previewRows,
    preview_interval: previewInterval,
    preview_count: previewCount,
  });
}

async function runJob(itemId: string, pageUrl: string): Promise<void> {
  // runJob is only started by pumpQueue / the /ingest handler when
  // active < MAX_CONCURRENT, so no busy check is needed here.
  active++;
  const dir = await mkdtemp(join(tmpdir(), "kepto-"));
  try {
    if (!r2) throw new Error("r2_not_configured");
    console.log(JSON.stringify({ event: "ingest_start", item_id: itemId, url: pageUrl }));

    // Metadata runs in parallel with the download (started now, awaited
    // later) so it adds no latency in the common case.
    const metaPromise = gatherMetadata(pageUrl);

    // 1. Resolve to a direct media URL, then download it — or let yt-dlp
    //    download it directly (needed for HLS / login-walled sources).
    // Defaults are overwritten on every success path below; a failure
    // throws out of the try/catch (to the outer ingest_failed handler)
    // so the defaults are never uploaded.
    let mediaPath = "";
    let contentType: string | null = null;
    let fileSize = 0;
    let sha256 = "";
    let cobaltFilename: string | undefined;
    // TikTok photo post: photos 1..n become sibling items, created by
    // the Worker from the `photos` field of the callback below. Photo 0
    // is the main item (mediaPath, uploaded downstream as original.*).
    let cobaltPhotos: Array<{
      r2_key: string;
      thumb_key: string;
      content_type: string;
      width: number | null;
      height: number | null;
      file_size: number;
      sha256: string;
    }> = [];
    try {
      const resolved = await resolveWithCobaltChain(pageUrl, itemId);
      cobaltFilename = resolved.filename;
      if (resolved.photos && resolved.photos.length) {
        // TikTok photo post: save EVERY photo as a photo item — no
        // slideshow video, no audio. Each surviving photo is
        // normalized to a real JPEG and uploaded to R2 as
        // photo-<i>.jpg (i = position among survivors, picker order).
        // Photo 0 is the main item; photos 1..n are siblings for the
        // Worker (cobaltPhotos, sent in the callback below).
        const uploaded: Array<{
          path: string;
          r2Key: string;
          width: number | null;
          height: number | null;
          fileSize: number;
          sha256: string;
        }> = [];
        for (let i = 0; i < resolved.photos.length; i++) {
          try {
            const raw = await downloadToFile(resolved.photos[i], join(dir, `photo${i}.jpg`));
            const normPath = join(dir, `photo${i}.norm.jpg`);
            if (!(await normalizeImageToJpeg(raw.path, normPath))) continue;
            const pMeta = await probe(normPath);
            const st = await stat(normPath);
            const hash = await new Promise<string>((resolve, reject) => {
              const h = createHash("sha256");
              const s = createReadStream(normPath);
              s.on("data", (c: Buffer | string) => h.update(c));
              s.on("end", () => resolve(h.digest("hex")));
              s.on("error", reject);
            });
            const r2Key = `users/me/items/${itemId}/photo-${uploaded.length}.jpg`;
            // First survivor is the main item: it goes up downstream
            // as original.jpg — uploading it here too would leave an
            // orphaned photo-0.jpg in R2. Siblings upload here.
            if (uploaded.length > 0) {
              await uploadFile(r2, R2_BUCKET, r2Key, normPath, "image/jpeg");
            }
            uploaded.push({
              path: normPath,
              r2Key,
              width: pMeta.width ?? null,
              height: pMeta.height ?? null,
              fileSize: st.size,
              sha256: hash,
            });
          } catch {
            // One photo failing must not kill the rest.
          }
        }
        if (uploaded.length) {
          const first = uploaded[0];
          mediaPath = first.path;
          contentType = "image/jpeg";
          fileSize = first.fileSize;
          sha256 = first.sha256;
          cobaltPhotos = uploaded.slice(1).map((p) => ({
            r2_key: p.r2Key,
            thumb_key: p.r2Key,
            content_type: "image/jpeg",
            width: p.width,
            height: p.height,
            file_size: p.fileSize,
            sha256: p.sha256,
          }));
          console.log(
            JSON.stringify({ event: "cobalt_photos", item_id: itemId, photos: uploaded.length }),
          );
        } else {
          // No photo survived: fall back to the old single-file download.
          const dl = await downloadToFile(resolved.url, join(dir, `media.${extFor(null, resolved.filename)}`));
          mediaPath = dl.path;
          contentType = dl.contentType;
          fileSize = dl.fileSize;
          sha256 = dl.sha256;
        }
      } else {
        const ext = extFor(null, resolved.filename);
        const dl = await downloadToFile(resolved.url, join(dir, `media.${ext}`));
        mediaPath = dl.path;
        contentType = dl.contentType;
        fileSize = dl.fileSize;
        sha256 = dl.sha256;
      }
    } catch (cobaltErr) {
      // The chain already logged each cobalt_failed (per instance).
      const cobaltMsg = String(cobaltErr);
      // TikWM download (shared by both paths below): a plain HTTP API
      // returning a direct no-watermark mp4. It handles age-restricted
      // posts that anonymous Cobalt/yt-dlp cannot fetch (user
      // suggestion 2026-11-10), and photo posts come back as TikTok's
      // own pre-rendered slideshow video.
      const tryTikwm = async (reason: string): Promise<void> => {
        console.log(JSON.stringify({ event: "tikwm_try", item_id: itemId, reason }));
        const tik = await resolveWithTikwm(pageUrl);
        const dl = await downloadToFile(tik.playUrl, join(dir, `media.${extFor(null, tik.playUrl) || "mp4"}`));
        mediaPath = dl.path;
        contentType = dl.contentType;
        fileSize = dl.fileSize;
        sha256 = dl.sha256;
        console.log(JSON.stringify({ event: "tikwm_used", item_id: itemId }));
      };
      // Age-restricted (Cobalt reports error.api.content.post.age):
      // yt-dlp cannot fetch it anonymously either, so skip yt-dlp
      // entirely and go straight to TikWM (user request 2026-11-10).
      // Running yt-dlp on an age-gated post just burns 10-30s before
      // failing the same way.
      if (/post\.age|age[-_ ]restricted/i.test(cobaltMsg)) {
        // TikWM is TikTok-only. An age error on a non-TikTok link is
        // not something TikWM can fix — fail with the Cobalt error
        // instead of TikWM's confusing "Url parsing is failed".
        if (isTikTokUrl(pageUrl)) {
          await tryTikwm("age");
        } else {
          throw new Error(cobaltMsg);
        }
      } else {
        if (!(await ytdlpAvailable(YTDLP_BIN))) {
          throw new Error(`cobalt_failed_no_ytdlp: ${cobaltMsg}`);
        }
        try {
          mediaPath = await downloadWithYtdlp(pageUrl, dir, YTDLP_BIN, COOKIES_FILE);
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
          // yt-dlp sometimes returns audio only — TikTok photo posts
          // expose no video format, just an mp3 + the photos. Cobalt
          // already failed (error.api.fetch.empty / no_valid_content),
          // so try TikWM next: it returns TikTok's own pre-rendered
          // slideshow video for photo posts. If TikWM fails too, keep
          // the yt-dlp audio — the slideshow builder below will try to
          // rescue it from the photos, and only then fail with
          // audio_only_no_video. (User correction 2026-11-10: a Cobalt
          // failure must fall through to yt-dlp AND TikWM, not stop at
          // the audio-only download.)
          if (!(await probe(mediaPath)).width) {
            // TikWM only handles TikTok URLs. For a non-TikTok link,
            // skip it and go straight to the slideshow rescue below
            // (it builds from the photos yt-dlp found, if any).
            if (isTikTokUrl(pageUrl)) {
              try {
                await tryTikwm("audio_only");
              } catch {
                // Keep the yt-dlp audio file; the slideshow path handles it.
              }
            }
          }
        } catch (ytdlpErr) {
          // yt-dlp failed too: TikWM is the last resort. If it fails
          // as well the job fails with its error (the yt-dlp error is
          // in the log).
          console.log(
            JSON.stringify({
              event: "ytdlp_failed",
              item_id: itemId,
              error: String(ytdlpErr).slice(0, 200),
            }),
          );
          // TikWM only handles TikTok URLs. For a non-TikTok link,
          // TikWM would just fail with "Url parsing is failed" —
          // surface the real yt-dlp error instead.
          if (isTikTokUrl(pageUrl)) {
            await tryTikwm("ytdlp_failed");
          } else {
            throw ytdlpErr;
          }
        }
      }
    }

    // 2. Probe + thumbnail + scrub sprite (thumbnail/sprite failure is non-fatal).
    let meta = await probe(mediaPath);
    // No video stream: either a TikTok sound post (audio only) or a
    // photo post (images + optional background music — yt-dlp finds no
    // video format for these, only the mp3 + the photos). A photo post
    // is still visual content the user wants saved, so build a
    // slideshow video from its photos (+ the audio as soundtrack) and
    // save that instead. Only fail when there are no photos to build
    // from (a true audio-only post). A probe failure returns {} so it
    // never triggers this branch.
    if (!meta.width) {
      const gatheredEarly = await metaPromise;
      const images = gatheredEarly.images ?? [];
      let built = false;
      if (images.length) {
        const imgPaths: string[] = [];
        for (let i = 0; i < images.length; i++) {
          try {
            const dl = await downloadToFile(images[i], join(dir, `photo${i}.jpg`));
            imgPaths.push(dl.path);
          } catch {
            // One photo failing must not kill the slideshow.
          }
        }
        if (imgPaths.length) {
          const totalSeconds =
            gatheredEarly.duration && gatheredEarly.duration > 0
              ? gatheredEarly.duration
              : meta.duration && meta.duration > 0
                ? meta.duration
                : imgPaths.length * 5;
          const slideshowPath = join(dir, "slideshow.mp4");
          built = await makeSlideshow(
            imgPaths,
            meta.hasAudio ? mediaPath : null,
            totalSeconds,
            slideshowPath,
          );
          if (built) {
            mediaPath = slideshowPath;
            contentType = "video/mp4";
            const st = await stat(mediaPath);
            fileSize = st.size;
            sha256 = await new Promise<string>((resolve, reject) => {
              const h = createHash("sha256");
              const s = createReadStream(mediaPath);
              s.on("data", (c: Buffer | string) => h.update(c));
              s.on("end", () => resolve(h.digest("hex")));
              s.on("error", reject);
            });
            meta = await probe(mediaPath);
            console.log(
              JSON.stringify({
                event: "slideshow_built",
                item_id: itemId,
                photos: imgPaths.length,
                seconds: totalSeconds,
                has_audio: Boolean(meta.hasAudio),
              }),
            );
          }
        }
      }
      if (!built) {
        throw new Error(meta.hasAudio ? "audio_only_no_video" : "no_video_stream");
      }
    }
    const gathered = await metaPromise;
    const ext = extFor(contentType, cobaltFilename) || mediaPath.split(".").pop() || "mp4";
    const thumbPath = join(dir, "thumb.jpg");
    // A still image has no timeline to seek: ffmpeg -ss 1 exits 0 but
    // writes no frame, so makeThumbnail always fails on photos — the
    // file check then nulls the thumb, and grid + viewer both break
    // (the viewer loads the thumb endpoint too). For a still, the
    // photo itself IS the thumbnail — same as the /upload path and
    // the same as photo siblings (thumb_key = r2_key).
    const isStill = (contentType ?? "").startsWith("image/");
    const hasThumb = isStill
      ? await normalizeImageToJpeg(mediaPath, thumbPath)
      : await makeThumbnail(mediaPath, thumbPath, 1);
    const r2Key = `users/me/items/${itemId}/original.${ext}`;
    const thumbKey = hasThumb ? `users/me/items/${itemId}/thumb.jpg` : null;
    const spritePath = join(dir, "sprite.jpg");
    const spriteInterval =
      meta.duration && meta.duration > 0
        ? Math.max(1, meta.duration / SPRITE_MAX_FRAMES)
        : 1;
    const spriteCount =
      meta.duration && meta.duration > 0
        ? Math.min(Math.ceil(meta.duration / spriteInterval), SPRITE_MAX_FRAMES)
        : SPRITE_MAX_FRAMES;
    const spriteRows = Math.ceil(spriteCount / SPRITE_COLS);
    // No sprite for a still image: there is nothing to scrub, and the
    // tile filter never fills from a single frame (it exits 0 and
    // writes nothing). Matches the /upload path, which skips sprites
    // for stills. A slideshow video has contentType video/mp4, so it
    // still gets a sprite.
    const sprite = isStill
      ? { ok: false, count: 0 }
      : await makeSprite(
          mediaPath,
          spritePath,
          meta.duration ?? null,
          spriteInterval,
          SPRITE_COLS,
          spriteRows,
        );
    const previewKey = sprite.ok ? `users/me/items/${itemId}/sprite.jpg` : null;

    // 3. Upload straight to R2, then tell the Worker the item is ready.
    await uploadFile(r2, R2_BUCKET, r2Key, mediaPath, contentType ?? "video/mp4");
    // Thumb/sprite uploads are non-fatal: the original is already in
    // R2 and the item is viewable without them. A failed thumb upload
    // used to throw here, failing the whole job (and on 2026-11-17 the
    // missing thumb.jpg crashed the entire ingest process). Null the
    // key on failure so the Worker never points at a missing R2 object.
    let thumbKeyOut: string | null = thumbKey;
    if (hasThumb && thumbKey) {
      try {
        await uploadFile(r2, R2_BUCKET, thumbKey, thumbPath, "image/jpeg");
      } catch (e) {
        console.error(JSON.stringify({ event: "thumb_upload_failed", item_id: itemId, error: String(e).slice(0, 200) }));
        thumbKeyOut = null;
      }
    }
    let previewKeyOut: string | null = previewKey;
    if (sprite.ok && previewKey) {
      try {
        await uploadFile(r2, R2_BUCKET, previewKey, spritePath, "image/jpeg");
      } catch (e) {
        console.error(JSON.stringify({ event: "sprite_upload_failed", item_id: itemId, error: String(e).slice(0, 200) }));
        previewKeyOut = null;
      }
    }
    console.log(JSON.stringify({ event: "ingest_ready", item_id: itemId, r2_key: r2Key, file_size: fileSize, has_audio: meta.hasAudio ?? null, sprite_ok: sprite.ok, sprite_frames: sprite.count }));
    await callback({
      item_id: itemId,
      r2_key: r2Key,
      thumb_key: thumbKeyOut,
      preview_key: previewKeyOut,
      content_type: contentType ?? "video/mp4",
      title: gathered.title ?? null,
      caption: gathered.caption ?? null,
      author: gathered.author ?? null,
      author_username: gathered.authorUsername ?? null,
      posted_at: gathered.postedAt ?? null,
      view_count: gathered.viewCount ?? null,
      duration: meta.duration ?? null,
      width: meta.width ?? null,
      height: meta.height ?? null,
      file_size: fileSize,
      sha256,
      preview_cols: previewKeyOut ? SPRITE_COLS : null,
      preview_rows: previewKeyOut ? spriteRows : null,
      preview_interval: previewKeyOut ? spriteInterval : null,
      preview_count: previewKeyOut ? sprite.count : null,
      // TikTok photo post: photos 1..n — the Worker creates one sibling
      // item per entry. Omitted when there are no siblings.
      ...(cobaltPhotos.length ? { photos: cobaltPhotos } : {}),
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
  console.log(JSON.stringify({ event: "cookies_configured", cookies: Boolean(COOKIES_FILE) }));
  void ytdlpVersion(YTDLP_BIN).then((version) => {
    console.log(JSON.stringify({ event: "ytdlp_version", version }));
  });
});
