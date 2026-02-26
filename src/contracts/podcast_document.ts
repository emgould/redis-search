/**
 * TypeScript interfaces for podcast items.
 *
 * Keep in sync with:
 *   src/core/normalize.py          – SearchDocument / document_to_redis()
 *   src/etl/pi_nightly_etl.py      – _add_display_fields()
 *   src/services/search_service.py – parse_doc(), _get_podcast_details()
 */

// ---------------------------------------------------------------------------
// Enum-like string unions
// ---------------------------------------------------------------------------

export type PodcastMCType = "podcast";
export type PodcastSource = "podcastindex";

// ---------------------------------------------------------------------------
// PodcastItem — list-level podcast item
//
// Returned by:
//   GET  /api/autocomplete        → response.podcast[]
//   GET  /api/autocomplete/stream → SSE "result" event where source is "podcast"
//   GET  /api/search              → response.podcast[]
//   GET  /api/search/stream       → SSE "result" event where source is "podcast"
//
// Also eligible as an exact_match item (see api_responses.ts).
//
// Shape: Redis JSON document + PodcastIndex display fields + parse_doc()
// injections (mc_id, search_title display swap).
// ---------------------------------------------------------------------------

export interface PodcastItem {
  /** Unique doc key, e.g. "podcastindex_podcast_920666" */
  id: string;
  /** Alias of id — always injected by parse_doc() */
  mc_id: string;
  /** Display title (parse_doc copies title → search_title) */
  search_title: string;
  /** Display title preserved from original */
  title?: string;
  mc_type: PodcastMCType;
  mc_subtype: null;
  source: PodcastSource;
  /** PodcastIndex feed ID as string */
  source_id: string;
  year: null;
  /** Normalized popularity score (0–100) */
  popularity: number;
  rating: number;
  /** Podcast artwork/cover image URL */
  image: string | null;
  /** Truncated podcast description */
  overview: string | null;

  // --- Genre / category fields ----------------------------------------------

  genre_ids: string[];
  genres: string[];

  // --- Podcast-specific display fields (from _add_display_fields) -----------

  /** RSS feed URL */
  url: string;
  /** Website link */
  site: string;
  /** iTunes author name */
  author: string | null;
  /** iTunes owner name */
  owner_name: string | null;
  /** Normalized language tag, e.g. "en", "es" */
  language: string | null;
  /** Normalized + IPTC-expanded categories */
  categories: string[];
  /** Normalized author for exact TAG matching */
  author_normalized: string | null;
  episode_count: number;
  /** iTunes numeric ID */
  itunes_id: number | null;
  podcast_guid: string | null;
  /** Raw PodcastIndex popularity score (0–29) */
  popularity_score: number;
  /** ISO timestamp string of last feed update */
  last_update_time: string | null;

  // --- Reserved fields (populated by search enrichment, null in ETL) --------

  artwork: string | null;
  trend_score: number | null;
  spotify_url: string | null;
  relevancy_score: number | null;

  // --- Lifecycle timestamps -------------------------------------------------

  /** Unix seconds — set on first index write */
  created_at: number | null;
  /** Unix seconds — updated on every write */
  modified_at: number | null;
  /** Write provenance tag, e.g. "backfill" */
  _source: string | null;
}

// ---------------------------------------------------------------------------
// PodcastDetailResponse — enriched podcast detail
//
// Returned by:
//   POST /api/details → when mc_type is "podcast"
//
// Extends PodcastItem with PodcastIndex API data.  When rss_details=true
// is passed in the request, RSS feed episodes are also included.
// ---------------------------------------------------------------------------

export interface PodcastRSSEpisode {
  title: string;
  description: string | null;
  published_at: string | null;
  duration: number | null;
  audio_url: string | null;
  episode_url: string | null;
  season: number | null;
  episode: number | null;
}

export interface PodcastDetailResponse extends PodcastItem {
  /** RSS episodes (only when rss_details=true) */
  rss_episodes?: PodcastRSSEpisode[];
  rss_total_episodes?: number;
  rss_feed_title?: string;
  rss_feed_description?: string;
  rss_error?: string;
}
