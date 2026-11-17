/**
 * Cobalt client — first resolver for a saved link.
 *
 * Uses the user's own instance (COBALT_URL, default the Railway instance).
 * POST / with { url, videoQuality: "max", downloadMode: "auto",
 * allowH265: true }. "max" is the highest quality; allowH265 lets
 * Cobalt return HEVC (H.265) when the source offers it — same quality
 * at a smaller file, or a higher-quality 1080p. When no HEVC version
 * exists it falls back to H.264 (verified: this video returns the
 * same H.264 576x1024 file with or without the flag). The flag must
 * be a JSON boolean — a string "true" is rejected (the old Shortcuts
 * bug).
 *   status "tunnel" | "redirect" -> body.url is the direct file URL.
 *   status "picker"              -> body.picker[]; a video item if there
 *     is one, else ALL photo items, so a TikTok photo post saves every
 *     photo (as separate photo items) instead of just its first photo.
 *     Photos only — no slideshow video, no background music.
 * Anything else is an error (e.g. error.api.content.post.age = age-restricted
 * post, which Cobalt cannot fetch anonymously — retrying will not help).
 */

export interface CobaltResult {
  url: string;
  filename?: string;
  /** TikTok photo post: every photo in the picker, in post order. */
  photos?: string[];
}

interface CobaltPickerItem {
  type?: string;
  url?: string;
}

interface CobaltResponse {
  status?: string;
  url?: string;
  filename?: string;
  picker?: CobaltPickerItem[];
  error?: { code?: string; context?: { service?: string } };
}

export async function resolveWithCobalt(
  pageUrl: string,
  cobaltUrl: string,
  apiKey: string,
): Promise<CobaltResult> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
  };
  if (apiKey) headers["Authorization"] = `Api-Key ${apiKey}`;
  const res = await fetch(cobaltUrl.replace(/\/$/, ""), {
    method: "POST",
    headers,
    body: JSON.stringify({
      url: pageUrl,
      videoQuality: "max",
      downloadMode: "auto",
      filenameStyle: "basic",
      allowH265: true,
    }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) {
    // Cobalt returns HTTP 400 for content errors and puts the real
    // reason in the JSON body (e.g. error.api.content.post.age for an
    // age-restricted post). Include that code — a bare http_400 tells
    // nothing about WHY the link failed.
    let code = `http_${res.status}`;
    try {
      const errBody = (await res.json()) as CobaltResponse;
      if (errBody.error?.code) code = errBody.error.code;
    } catch {
      // Body is not JSON; keep the HTTP status.
    }
    throw new Error(`cobalt_${code}`);
  }
  const data = (await res.json()) as CobaltResponse;
  if ((data.status === "tunnel" || data.status === "redirect") && data.url) {
    return { url: data.url, filename: data.filename };
  }
  if (data.status === "picker" && Array.isArray(data.picker)) {
    const video = data.picker.find((p) => p.type === "video" && p.url);
    if (video?.url) return { url: video.url, filename: data.filename };
    // Photo picker (TikTok photo post): return every photo. The caller
    // saves each as its own photo item. Returning only the first photo
    // would silently drop the rest of the post.
    const photos = data.picker
      .filter((p) => p.type === "photo" && p.url)
      .map((p) => p.url as string);
    if (photos.length) {
      return {
        url: photos[0],
        filename: data.filename,
        photos,
      };
    }
    const first = data.picker.find((p) => p.url);
    if (first?.url) return { url: first.url, filename: data.filename };
    throw new Error("cobalt_picker_empty");
  }
  const code = data.error?.code ?? data.status ?? "unknown";
  throw new Error(`cobalt_${code}`);
}
