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

export async function downloadWithYtdlp(
  pageUrl: string,
  outDir: string,
  bin: string,
): Promise<string> {
  const args = [
    "--no-playlist",
    "--no-warnings",
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

export interface YtDlpMetadata {
  uploaderId?: string;
  uploader?: string;
  title?: string;
  description?: string;
  /** Original upload date as YYYYMMDD, when the extractor provides it. */
  uploadDate?: string;
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
      ["--no-playlist", "--no-warnings", "--skip-download", "--dump-json", pageUrl],
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
        finish({
          uploaderId: pick("uploader_id"),
          uploader: pick("uploader"),
          title: pick("title"),
          description: pick("description"),
          uploadDate: pick("upload_date"),
        });
      } catch {
        finish({});
      }
    });
  });
}
