/**
 * TypeScript interface for YouTube video items.
 *
 * Source: YouTube Data API via YouTubeVideo.model_dump().
 * NOT stored in a Redis index.
 *
 * Keep in sync with: src/api/youtube/models.py – YouTubeVideo
 */

import type { MCLink, MCImage } from "./mc_base_item";

// ---------------------------------------------------------------------------
// VideoItem — YouTube video
//
// Returned by:
//   GET  /api/search        → response.video[]
//   GET  /api/search/stream → SSE "result" event where source is "video"
//
// NOT returned by /api/autocomplete or /api/autocomplete/stream.
// ---------------------------------------------------------------------------

export interface VideoItem {
  // --- MCBaseItem fields ----------------------------------------------------
  mc_id: string;
  mc_type: "video";
  mc_subtype: string | null;
  source: "youtube";
  source_id: string | null;
  links: MCLink[];
  images: MCImage[];
  metrics: Record<string, unknown>;
  external_ids: Record<string, unknown>;
  error: string | null;
  status_code: number;
  sort_order: number;

  // --- YouTubeVideo fields --------------------------------------------------
  id: string;
  video_id: string;
  title: string;
  description: string;
  channel_title: string;
  channel_id: string;
  published_at: string;
  thumbnail_url: string | null;
  url: string;
  view_count: number;
  like_count: number;
  comment_count: number;
  /** ISO 8601 duration, e.g. "PT5M30S" */
  duration: string | null;
  tags: string[];
  category_id: string | null;
  category: string | null;
  default_language: string | null;
  is_live: boolean;
}
