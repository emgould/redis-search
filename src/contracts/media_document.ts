/**
 * TypeScript interfaces for media items (movie / tv).
 *
 * Keep in sync with:
 *   src/core/normalize.py          – SearchDocument / document_to_redis()
 *   src/services/search_service.py – parse_doc(), _get_media_details()
 */

// ---------------------------------------------------------------------------
// Shared enums / unions
// ---------------------------------------------------------------------------

export type MCSource = "tmdb";

export type MCMediaType = "movie" | "tv";

export type MCSubType =
  | "actor"
  | "musician"
  | "politician"
  | "athlete"
  | "author"
  | "podcaster"
  | "artist"
  | "music_artist"
  | "person"
  | "writer"
  | "director"
  | "producer"
  | "youtube_creator"
  | "character";

// ---------------------------------------------------------------------------
// Nested object interfaces
// ---------------------------------------------------------------------------

export interface MediaDirector {
  id: string;
  name: string;
  name_normalized: string;
}

export interface MediaWatchProviders {
  watch_region: string;
  primary_provider_type: string;
  streaming_platform_ids: string[];
  on_demand_platform_ids: string[];
  primary_provider_id: number;
}

// ---------------------------------------------------------------------------
// MediaItem — list-level media item
//
// Returned by:
//   GET  /api/autocomplete        → response.tv[] and response.movie[]
//   GET  /api/autocomplete/stream → SSE "result" events where source is "tv" or "movie"
//   GET  /api/search              → response.tv[] and response.movie[]
//   GET  /api/search/stream       → SSE "result" events where source is "tv" or "movie"
//
// Also used as the base for exact_match items (with cast restructured to
// ExactMatchCastEntry[] — see api_responses.ts).
//
// Shape: Redis JSON document + fields injected by parse_doc() (mc_id,
// search_title display swap).
// ---------------------------------------------------------------------------

export interface MediaItem {
  /** mc_id value, e.g. "tmdb_550" or "tmdb_1396" */
  id: string;
  /** Alias of id — always injected by parse_doc() */
  mc_id: string;
  /** Display title (parse_doc copies title → search_title for display) */
  search_title: string;
  /** Display title preserved from original */
  title?: string;
  mc_type: MCMediaType;
  mc_subtype: MCSubType | null;
  source: MCSource;
  /** Raw TMDB numeric ID as string */
  source_id: string;
  year: number | null;
  /** Normalized MC popularity score (0–100) */
  popularity: number;
  /** Rating score (0–10) */
  rating: number;
  /** Medium poster image URL */
  image: string | null;
  /** Truncated description */
  overview: string | null;

  /** TMDB genre IDs as strings, e.g. ["35","18"] */
  genre_ids: string[];
  /** Normalized genre names, e.g. ["comedy","drama"] */
  genres: string[];

  /** TMDB person IDs for top cast (strings) */
  cast_ids: string[];
  /** Normalized cast names for TAG filtering */
  cast_names: string[];
  /** Display cast names (not normalized) */
  cast: string[];

  director: MediaDirector | null;

  /** IPTC-expanded, normalized keyword tags */
  keywords: string[];
  /** Normalized ISO country codes, e.g. ["us"] */
  origin_country: string[];
  original_language: string | null;
  original_title: string | null;

  /** YYYY-MM-DD — populated for movies */
  release_date: string | null;
  /** YYYY-MM-DD — populated for TV */
  first_air_date: string | null;
  /** YYYY-MM-DD — populated for TV */
  last_air_date: string | null;

  /** e.g. "R", "PG-13", "TV-MA" */
  us_rating: string | null;
  watch_providers: MediaWatchProviders | null;

  status: string | null;
  /** TV series binge/weekly status */
  series_status: string | null;
  tagline: string | null;
  vote_count: number | null;
  vote_average: number | null;
  /** Raw TMDB popularity number */
  popularity_tmdb: number | null;
  /** Runtime in minutes (movies) */
  runtime: number | null;

  number_of_seasons: number | null;
  number_of_episodes: number | null;
  created_by: string[] | null;
  created_by_ids: number[] | null;
  networks: string[] | null;
  /** First network name, normalized */
  network: string | null;
  production_companies: string[] | null;
  production_countries: string[] | null;
  budget: number | null;
  revenue: number | null;
  spoken_languages: string[] | null;

  /** Unix seconds — set on first index write */
  created_at: number | null;
  /** Unix seconds — updated on every write */
  modified_at: number | null;
  /** Write provenance tag, e.g. "backfill" */
  _source: string | null;

  /** Pipe-separated alternate names */
  also_known_as?: string;
}

// ---------------------------------------------------------------------------
// Discriminated narrowing helpers for mc_type-specific guarantees
// ---------------------------------------------------------------------------

export interface MovieItem extends MediaItem {
  mc_type: "movie";
  first_air_date: null;
  last_air_date: null;
  number_of_seasons: null;
  number_of_episodes: null;
  created_by: null;
  created_by_ids: null;
  networks: null;
  network: null;
  series_status: null;
}

export interface TvItem extends MediaItem {
  mc_type: "tv";
  director: null;
  release_date: null;
  budget: null;
  revenue: null;
}

export type MediaItemUnion = MovieItem | TvItem;

// ---------------------------------------------------------------------------
// MediaDetailResponse — enriched media detail
//
// Returned by:
//   POST /api/details → when mc_type is "movie" or "tv"
//
// Extends MediaItem with live TMDB enrichment (watch providers, full cast,
// trailers, keywords).  watch_providers is widened to the full TMDB map.
// ---------------------------------------------------------------------------

export interface MediaDetailResponse extends Omit<MediaItem, "watch_providers"> {
  tmdb_id: number;
  /** Full TMDB watch-provider map (richer than indexed shape) */
  watch_providers: Record<string, unknown>;
  streaming_platform: string | null;
  /** Cast with profile images */
  main_cast: Record<string, unknown>[];
  /** Raw TMDB credits payload */
  tmdb_cast: Record<string, unknown>;
  /** Raw TMDB videos payload */
  tmdb_videos: Record<string, unknown>;
  /** YouTube trailer key URL */
  primary_trailer: string | null;
  trailers: Record<string, unknown>[];
  backdrop_path: string | null;
  /** Full (untruncated) overview from TMDB */
  full_overview?: string;
}
