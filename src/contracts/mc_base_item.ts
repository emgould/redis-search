/**
 * Shared nested types from the Pydantic MCBaseItem base class.
 *
 * These appear in model_dump() output of every brokered-API result item:
 *   NewsItem, VideoItem, RatingsItem, ArtistItem, AlbumItem
 *
 * They are returned wherever those items appear:
 *   GET  /api/search        → response.news[], video[], ratings[], artist[], album[]
 *   GET  /api/search/stream → SSE "result" events for those sources
 *
 * Keep in sync with: src/contracts/models.py – MCBaseItem, MCLink, MCImage
 */

// ---------------------------------------------------------------------------
// MCLink
// ---------------------------------------------------------------------------

export interface MCLink {
  url: string;
  key: string;
  description: string;
}

// ---------------------------------------------------------------------------
// MCImage
// ---------------------------------------------------------------------------

export type MCUrlType = "url" | "path" | "deep_link";

export interface MCImage {
  url: string;
  key: string;
  type: MCUrlType | null;
  description: string;
}
