/**
 * TikWM fallback (https://www.tikwm.com/api/) — a plain HTTP API that
 * returns a direct no-watermark mp4 for a TikTok URL. Used as the LAST
 * resort in the ingest chain (after Cobalt and yt-dlp both fail): it
 * handles age-restricted posts that anonymous Cobalt/yt-dlp cannot
 * fetch (user suggestion 2026-11-10), and it also returns TikTok's own
 * pre-rendered slideshow video for photo posts (better than building
 * one from thumbnails). No API key; free. If it is down or rate-limits
 * us, the job just fails as it would have without it.
 */
export interface TikwmResult {
  /** Direct mp4 URL (no watermark). For a photo post this is TikTok's
   * own pre-rendered slideshow video, not a still image. */
  playUrl: string;
  /** Background-music URL (present for most posts). */
  musicUrl?: string;
  title?: string;
  author?: string;
  duration?: number;
}

export async function resolveWithTikwm(pageUrl: string): Promise<TikwmResult> {
  const api = `https://www.tikwm.com/api/?url=${encodeURIComponent(pageUrl)}`;
  const res = await fetch(api, {
    headers: { "User-Agent": "kepto-ingest/0.1" },
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(`tikwm_http_${res.status}`);
  const j = (await res.json()) as {
    code?: unknown;
    msg?: unknown;
    data?: Record<string, unknown>;
  };
  const data = j.data;
  if (j.code !== 0 || !data || typeof data.play !== "string" || !data.play) {
    throw new Error(`tikwm_${typeof j.msg === "string" ? j.msg : "failed"}`);
  }
  const author = data.author as Record<string, unknown> | undefined;
  return {
    playUrl: data.play,
    musicUrl: typeof data.music === "string" ? data.music : undefined,
    title: typeof data.title === "string" ? data.title : undefined,
    author:
      author && typeof author.unique_id === "string" ? author.unique_id : undefined,
    duration: typeof data.duration === "number" ? data.duration : undefined,
  };
}
