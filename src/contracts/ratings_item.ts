/**
 * TypeScript interface for Rotten Tomatoes ratings items.
 *
 * Source: RottenTomatoes API via MCRottenTomatoesItem.model_dump().
 * NOT stored in a Redis index.
 *
 * Keep in sync with: src/api/rottentomatoes/models.py – MCRottenTomatoesItem
 */

import type { MCLink, MCImage } from "./mc_base_item";

// ---------------------------------------------------------------------------
// RatingsItem — Rotten Tomatoes rating
//
// Returned by:
//   GET  /api/search        → response.ratings[]
//   GET  /api/search/stream → SSE "result" event where source is "ratings"
//
// NOT returned by /api/autocomplete or /api/autocomplete/stream.
//
// When a search has filters but no text query, ratings are enriched from
// indexed tv/movie results via per-title RT lookups rather than a direct
// RT search.
// ---------------------------------------------------------------------------

export interface RatingsItem {
  // --- MCBaseItem fields ----------------------------------------------------
  mc_id: string;
  mc_type: "movie" | "tv" | "mixed";
  mc_subtype: string | null;
  source: "rottentomatoes";
  source_id: string | null;
  links: MCLink[];
  images: MCImage[];
  metrics: Record<string, unknown>;
  external_ids: Record<string, unknown>;
  error: string | null;
  status_code: number;
  sort_order: number;

  // --- MCRottenTomatoesItem fields ------------------------------------------
  rt_id: number | null;
  ems_id: string | null;
  tms_id: string | null;
  title: string | null;
  description: string | null;
  release_year: number | null;
  /** MPAA rating (PG, R, etc.) */
  rating: string | null;
  genres: string[];
  /** Runtime in minutes */
  runtime: number | null;
  /** URL slug */
  vanity: string | null;
  critics_score: number | null;
  audience_score: number | null;
  /** e.g. "fresh", "rotten", "certified_fresh" */
  critics_sentiment: string | null;
  audience_sentiment: string | null;
  certified_fresh: boolean;
  verified_hot: boolean;
  poster_url: string | null;
  rt_url: string | null;
  cast_names: string[];
  director: string | null;
  /** TV-specific: premiere date */
  series_premiere: string | null;
  /** TV-specific: season numbers */
  seasons: number[];
  popularity: number | null;
}
