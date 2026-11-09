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
  /** True when the file has an audio stream (diagnoses "video has no sound"). */
  hasAudio?: boolean;
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
          hasAudio: j.streams?.some((s) => s.codec_type === "audio"),
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

export interface SpriteResult {
  ok: boolean;
  /** Frames actually in the sprite (from duration; capped at cols*rows). */
  count: number;
}

/**
 * Slideshow video for a TikTok photo post: yt-dlp finds no video stream
 * for these (only the background-music audio + the photos), so a video
 * grid cannot display them. Build an mp4 that shows each photo for an
 * equal share of `totalSeconds` (letterboxed to 1080x1920, TikTok
 * portrait) with the audio track as soundtrack when present (silent
 * when not). Uses the concat demuxer + one scale/pad filter, so mixed
 * photo sizes are fine. Returns true when the mp4 was created.
 */
export async function makeSlideshow(
  imagePaths: string[],
  audioPath: string | null,
  totalSeconds: number,
  outPath: string,
): Promise<boolean> {
  if (!imagePaths.length || !(totalSeconds > 0)) return false;
  const per = totalSeconds / imagePaths.length;
  const listPath = `${outPath}.list.txt`;
  // The concat demuxer needs the last file repeated (without a duration)
  // or the final segment is dropped.
  const lines = imagePaths.flatMap((p) => [
    `file '${p.replace(/'/g, "'\\''")}'`,
    `duration ${per.toFixed(3)}`,
  ]);
  lines.push(`file '${imagePaths[imagePaths.length - 1].replace(/'/g, "'\\''")}'`);
  const { writeFile } = await import("node:fs/promises");
  await writeFile(listPath, lines.join("\n") + "\n");
  const args = [
    "-y",
    "-v",
    "error",
    "-f",
    "concat",
    "-safe",
    "0",
    "-i",
    listPath,
  ];
  if (audioPath) args.push("-i", audioPath);
  args.push(
    "-vf",
    "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "23",
  );
  if (audioPath) args.push("-c:a", "aac", "-shortest");
  args.push("-movflags", "+faststart", outPath);
  return new Promise((resolve) => {
    const p = spawn("ffmpeg", args);
    p.on("error", () => resolve(false));
    p.on("close", (code) => resolve(code === 0));
  });
}

/**
 * Scrub-preview sprite: one frame every `intervalSeconds`, scaled to
 * 160px wide, tiled into a `cols` x `rows` grid in a single JPEG.
 * The iOS player crops one cell per scrub timestamp (frame index =
 * floor(seconds / interval), col = index % cols, row = floor(index / cols)).
 * The interval is 1s for videos up to 100s (so a 10s video gets 10 frames
 * and the preview follows the finger in real time); longer videos stretch
 * the interval so the sprite never exceeds 100 frames. Failure is
 * non-fatal — the player just shows no preview for that item. When
 * duration is unknown the frame count is unknowable, so `count` falls
 * back to cols*rows (the player clamps to the last cell; trailing cells
 * may be blank for short videos — probe almost always returns a duration).
 */
export function makeSprite(
  filePath: string,
  spritePath: string,
  duration: number | null,
  intervalSeconds = 1,
  cols = 10,
  rows = 1,
): Promise<SpriteResult> {
  const count =
    duration && duration > 0
      ? Math.min(Math.ceil(duration / intervalSeconds), cols * rows)
      : cols * rows;
  // fps as a plain decimal (e.g. 0.5556 for a 1.8s interval) — the fps
  // filter's "1/1.8" rational form is not reliably parsed.
  const fps = (1 / intervalSeconds).toFixed(4);
  return new Promise((resolve) => {
    const p = spawn("ffmpeg", [
      "-y",
      "-v",
      "error",
      "-i",
      filePath,
      "-vf",
      `fps=${fps},scale=160:-1,tile=${cols}x${rows}`,
      "-frames:v",
      "1",
      "-q:v",
      "5",
      spritePath,
    ]);
    p.on("error", () => resolve({ ok: false, count: 0 }));
    p.on("close", (code) => resolve({ ok: code === 0, count: code === 0 ? count : 0 }));
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
