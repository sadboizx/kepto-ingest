/**
 * Media helpers: download a direct URL to a temp file (with sha256),
 * probe duration/dimensions with ffprobe, and grab a thumbnail with ffmpeg.
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";

export interface DownloadedFile {
  path: string;
  contentType: string | null;
  fileSize: number;
  sha256: string;
}

export async function downloadToFile(
  fileUrl: string,
  destPath: string,
): Promise<DownloadedFile> {
  const res = await fetch(fileUrl, {
    headers: { "User-Agent": "kepto-ingest/0.1" },
    signal: AbortSignal.timeout(600_000),
  });
  if (!res.ok || !res.body) {
    throw new Error(`download_http_${res.status}`);
  }
  const hash = createHash("sha256");
  let fileSize = 0;
  const writer = createWriteStream(destPath);
  try {
    for await (const chunk of res.body as AsyncIterable<Uint8Array>) {
      hash.update(chunk);
      fileSize += chunk.length;
      if (!writer.write(chunk)) {
        await new Promise<void>((resolve) => writer.once("drain", resolve));
      }
    }
  } finally {
    await new Promise<void>((resolve, reject) => {
      writer.end(() => resolve());
      writer.on("error", reject);
    });
  }
  return {
    path: destPath,
    contentType: res.headers.get("content-type"),
    fileSize,
    sha256: hash.digest("hex"),
  };
}

export interface ProbeResult {
  duration?: number;
  width?: number;
  height?: number;
}

export function probe(filePath: string): Promise<ProbeResult> {
  return new Promise((resolve) => {
    const p = spawn("ffprobe", [
      "-v",
      "quiet",
      "-print_format",
      "json",
      "-show_format",
      "-show_streams",
      filePath,
    ]);
    let out = "";
    p.stdout?.on("data", (d: Buffer) => {
      out += d.toString();
    });
    p.on("error", () => resolve({}));
    p.on("close", () => {
      try {
        const j = JSON.parse(out) as {
          format?: { duration?: string };
          streams?: { width?: number; height?: number; codec_type?: string }[];
        };
        const video = j.streams?.find((s) => s.codec_type === "video");
        resolve({
          duration: j.format?.duration ? Number(j.format.duration) : undefined,
          width: video?.width,
          height: video?.height,
        });
      } catch {
        resolve({});
      }
    });
  });
}

export function makeThumbnail(
  filePath: string,
  thumbPath: string,
  atSeconds = 1,
): Promise<boolean> {
  return new Promise((resolve) => {
    const p = spawn("ffmpeg", [
      "-y",
      "-v",
      "error",
      "-ss",
      String(atSeconds),
      "-i",
      filePath,
      "-vframes",
      "1",
      "-q:v",
      "4",
      thumbPath,
    ]);
    p.on("error", () => resolve(false));
    p.on("close", (code) => resolve(code === 0));
  });
}

/** Extension for an R2 key from a content type or a Cobalt filename. */
export function extFor(contentType: string | null, filename?: string): string {
  if (filename) {
    const m = /\.([a-z0-9]{2,5})$/i.exec(filename);
    if (m) return m[1].toLowerCase();
  }
  if (contentType?.includes("webm")) return "webm";
  if (contentType?.includes("quicktime")) return "mov";
  if (contentType?.includes("image/png")) return "png";
  if (contentType?.includes("image/jpeg")) return "jpg";
  return "mp4";
}
