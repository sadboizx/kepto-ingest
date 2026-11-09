/**
 * yt-dlp fallback — used only when Cobalt cannot resolve the link
 * (private/login-walled posts, Stories, Douyin, platforms Cobalt lacks).
 * Downloads straight to a file so HLS/m3u8 sources are handled by yt-dlp
 * itself instead of by this service. Returns the downloaded file path.
 */
import { spawn } from "node:child_process";
import { readdir } from "node:fs/promises";
import { join } from "node:path";

export function ytdlpAvailable(bin: string): Promise<boolean> {
  return new Promise((resolve) => {
    const p = spawn(bin, ["--version"], { stdio: "ignore" });
    p.on("error", () => resolve(false));
    p.on("close", (code) => resolve(code === 0));
  });
}

/// The installed yt-dlp version (e.g. "2026.10.26"), or null when the
/// binary is missing. Logged once at startup: yt-dlp's TikTok error
/// text is version-dependent, so this pins down which build a log
/// line came from.
export function ytdlpVersion(bin: string): Promise<string | null> {
  return new Promise((resolve) => {
    const p = spawn(bin, ["--version"]);
    let out = "";
    p.stdout?.on("data", (d: Buffer) => {
      out += d.toString();
    });
    p.on("error", () => resolve(null));
    p.on("close", (code) => resolve(code === 0 ? out.trim() || null : null));
  });
}

export async function downloadWithYtdlp(
  pageUrl: string,
  outDir: string,
  bin: string,
): Promise<string> {
  const args = [
    "--no-playlist",
    "--no-warnings",
    // Impersonate Chrome so TikTok serves the media instead of a
    // challenge/empty page to a datacenter IP (needs curl-cffi,
    // installed in the Dockerfile).
    "--impersonate",
    "chrome",
    "-f",
    "bv*[height<=1080]+ba/b[height<=1080]/b",
    "--merge-output-format",
    "mp4",
    "-o",
    join(outDir, "media.%(ext)s"),
    pageUrl,
  ];
  await new Promise<void>((resolve, reject) => {
    const p = spawn(bin, args, { stdio: "pipe" });
    let stderr = "";
    p.stderr?.on("data", (d: Buffer) => {
      stderr += d.toString();
    });
    p.on("error", (e) => reject(new Error(`ytdlp_spawn: ${e.message}`)));
    p.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ytdlp_exit_${code}: ${stderr.slice(0, 300)}`));
    });
  });
  const files = (await readdir(outDir)).filter((f) => f.startsWith("media."));
  if (!files.length) throw new Error("ytdlp_no_output");
  return join(outDir, files[0]);
}

export interface ExpandedPlaylist {
  title?: string;
  urls: string[];
}

/**
 * Follow redirects (short links like vt.tiktok.com / vm.tiktok.com)
 * to the canonical URL, without downloading the page body — only
 * the `location` headers are read, so this is fast even when TikTok
 * is slow for the server's IP. Returns the original URL when the
 * chain cannot be followed (yt-dlp then resolves it itself).
 * A short link to a collection must resolve to the real
 * tiktok.com/.../collection/... URL for yt-dlp's tiktok:collection
 * extractor to recognise it — the vm.tiktok extractor timed out on
 * exactly this (2026-11-10, vt.tiktok.com short link).
 */
export async function resolveFinalUrl(pageUrl: string): Promise<string> {
  try {
    let current = pageUrl;
    for (let hop = 0; hop < 5; hop++) {
      const res = await fetch(current, {
        redirect: "manual",
        signal: AbortSignal.timeout(10_000),
      });
      const loc = res.headers.get("location");
      if (!loc) break;
      current = new URL(loc, current).toString();
    }
    return current;
  } catch {
    return pageUrl;
  }
}

/**
 * Expand a playlist/collection URL into its video URLs.
 * `yt-dlp --flat-playlist -J` returns one JSON object: `_type:
 * "playlist"` with `entries[]` (each with `webpage_url`), or a single
 * video object when the URL is one video. Used for "save all videos
 * from a TikTok Collection" — yt-dlp has a dedicated `tiktok:collection`
 * extractor, and a user page works the same way. Rejects when the URL
 * is not a playlist/collection, is private, or yt-dlp cannot read it
 * (e.g. a challenge page). Capped at maxEntries.
 *
 * Short links are resolved to the canonical URL first (see
 * resolveFinalUrl), the socket timeout is raised to 30s (TikTok is
 * slow for datacenter IPs), and a failed attempt is retried once —
 * a read timeout is often transient.
 */
export async function expandPlaylist(
  pageUrl: string,
  bin: string,
  maxEntries = 100,
  timeoutMs = 120_000,
): Promise<ExpandedPlaylist> {
  const resolved = await resolveFinalUrl(pageUrl);
  let lastError: Error = new Error("expand_failed: unknown");
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      return await expandOnce(resolved, bin, maxEntries, timeoutMs);
    } catch (e) {
      lastError = e instanceof Error ? e : new Error(String(e));
      if (attempt === 0) {
        console.log(
          JSON.stringify({ event: "expand_retry", url: resolved, error: lastError.message.slice(0, 200) }),
        );
        // Wait before retrying: TikTok rate-limits datacenter IPs,
        // and an immediate retry hits the same empty response.
        await new Promise((r) => setTimeout(r, 5_000));
      }
    }
  }
  throw lastError;
}

/**
 * The folder name for a saved TikTok Collection: the collection's own
 * TikTok title, NOT "<username>-<title>" (that is what yt-dlp puts in
 * the playlist title, e.g. "choayphoquenkodoxang-test"). Prefer the
 * collection slug from the URL (/collection/<name>-<numeric-id> ->
 * <name>); fall back to stripping the "<username>-" prefix from
 * yt-dlp's title (TikTok usernames never contain "-", so the first
 * "-" is the separator). Returns undefined when neither yields a name.
 */
function cleanCollectionTitle(pageUrl: string, ytTitle?: string): string | undefined {
  try {
    const u = new URL(pageUrl);
    const m = /\/collection\/([^/?#]+)/i.exec(u.pathname);
    if (m) {
      const slug = decodeURIComponent(m[1]);
      // Strip the trailing numeric id ("test-7706255745959856916" -> "test").
      const name = slug.replace(/-\d+$/, "");
      if (name) return name;
    }
  } catch {
    // Fall through to the title.
  }
  if (ytTitle) {
    const idx = ytTitle.indexOf("-");
    if (idx > 0 && idx < ytTitle.length - 1) {
      return ytTitle.slice(idx + 1);
    }
    return ytTitle;
  }
  return undefined;
}

function expandOnce(
  pageUrl: string,
  bin: string,
  maxEntries: number,
  timeoutMs: number,
): Promise<ExpandedPlaylist> {
  return new Promise((resolve, reject) => {
    let done = false;
    const finish = (fn: () => void) => {
      if (done) return;
      done = true;
      fn();
    };
    const p = spawn(
      bin,
      [
        "--flat-playlist",
        "--no-warnings",
        // Impersonate Chrome (needs curl-cffi, installed in the
        // Dockerfile): TikTok returns an empty JSON body to a bare
        // datacenter IP, which the tiktok:collection extractor
        // cannot parse ("Failed to parse JSON", 2026-11-10).
        "--impersonate",
        "chrome",
        "--socket-timeout",
        "30",
        "--playlist-end",
        String(maxEntries),
        "-J",
        pageUrl,
      ],
      { stdio: "pipe" },
    );
    let out = "";
    let stderr = "";
    const timer = setTimeout(() => {
      try {
        p.kill("SIGKILL");
      } catch {
        // Already gone.
      }
      finish(() => reject(new Error("expand_timeout")));
    }, timeoutMs);
    p.stdout?.on("data", (d: Buffer) => {
      out += d.toString();
    });
    p.stderr?.on("data", (d: Buffer) => {
      stderr += d.toString();
    });
    p.on("error", (e) => {
      clearTimeout(timer);
      finish(() => reject(new Error(`expand_spawn: ${e.message}`)));
    });
    p.on("close", (code) => {
      clearTimeout(timer);
      finish(() => {
        if (code !== 0) {
          reject(new Error(`expand_failed: ${stderr.slice(0, 300) || `exit ${code}`}`));
          return;
        }
        try {
          const j = JSON.parse(out) as Record<string, unknown>;
          const urls: string[] = [];
          const pushUrl = (u: unknown) => {
            if (typeof u === "string" && /^https?:\/\//i.test(u) && !urls.includes(u)) {
              urls.push(u);
            }
          };
          const entries = Array.isArray(j.entries) ? j.entries : [];
          for (const e of entries) {
            const rec = e as Record<string, unknown>;
            pushUrl(rec.webpage_url ?? rec.url);
          }
          // A single video URL (not a playlist) returns the video itself.
          if (!urls.length) pushUrl(j.webpage_url);
          if (!urls.length) {
            reject(new Error("expand_empty: no videos found in this collection"));
            return;
          }
          resolve({
            title: cleanCollectionTitle(pageUrl, typeof j.title === "string" ? j.title : undefined),
            urls: urls.slice(0, maxEntries),
          });
        } catch {
          reject(new Error("expand_bad_json"));
        }
      });
    });
  });
}

export interface YtDlpMetadata {
  uploaderId?: string;
  uploader?: string;
  title?: string;
  description?: string;
  /** Original upload date as YYYYMMDD, when the extractor provides it. */
  uploadDate?: string;
  /** View count from the extractor (TikTok: view_count), when available. */
  viewCount?: number;
}

/**
 * Metadata-only probe: `--dump-json --skip-download` returns the extractor's
 * title / description / uploader / uploader_id / upload_date without
 * downloading any bytes. Best-effort by design — many platforms block
 * yt-dlp from a datacenter IP, so ANY failure (spawn error, timeout,
 * non-zero exit, bad JSON) resolves to `{}` instead of rejecting.
 * Never let metadata kill a job whose media already downloaded.
 */
export function dumpJsonMetadata(
  pageUrl: string,
  bin: string,
  timeoutMs = 15_000,
): Promise<YtDlpMetadata> {
  return new Promise((resolve) => {
    let done = false;
    const finish = (v: YtDlpMetadata) => {
      if (done) return;
      done = true;
      resolve(v);
    };
    const p = spawn(
      bin,
      ["--no-playlist", "--no-warnings", "--impersonate", "chrome", "--skip-download", "--dump-json", pageUrl],
      { stdio: "pipe" },
    );
    let out = "";
    const timer = setTimeout(() => {
      try {
        p.kill("SIGKILL");
      } catch {
        // Already gone — finish() below handles it.
      }
      finish({});
    }, timeoutMs);
    p.stdout?.on("data", (d: Buffer) => {
      out += d.toString();
    });
    p.on("error", () => {
      clearTimeout(timer);
      finish({});
    });
    p.on("close", () => {
      clearTimeout(timer);
      try {
        const j = JSON.parse(out) as Record<string, unknown>;
        const pick = (k: string): string | undefined =>
          typeof j[k] === "string" ? (j[k] as string) : undefined;
        const pickNum = (k: string): number | undefined =>
          typeof j[k] === "number" ? (j[k] as number) : undefined;
        finish({
          uploaderId: pick("uploader_id"),
          uploader: pick("uploader"),
          title: pick("title"),
          description: pick("description"),
          uploadDate: pick("upload_date"),
          viewCount: pickNum("view_count"),
        });
      } catch {
        finish({});
      }
    });
  });
}
