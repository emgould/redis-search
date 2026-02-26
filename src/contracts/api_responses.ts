/**
 * Top-level response envelopes and cross-cutting types for the
 * search-service API endpoints.
 *
 * Keep in sync with:
 *   web/app.py                     – endpoint definitions
 *   src/services/search_service.py – autocomplete(), search(), *_stream()
 */

import type { MediaItem } from "./media_document";
import type { PersonItem } from "./person_document";
import type { PodcastItem } from "./podcast_document";
import type { AuthorItem } from "./author_document";
import type { BookItem } from "./book_document";
import type { NewsItem } from "./news_item";
import type { VideoItem } from "./video_item";
import type { RatingsItem } from "./ratings_item";
import type { ArtistItem } from "./artist_item";
import type { AlbumItem } from "./album_item";

// ---------------------------------------------------------------------------
// ExactMatchCastEntry / ExactMatchItem
//
// Returned as:
//   GET  /api/autocomplete        → response.exact_match
//   GET  /api/autocomplete/stream → SSE "exact_match" event data
//   GET  /api/search              → response.exact_match
//   GET  /api/search/stream       → SSE "exact_match" event data
//
// When a query exactly matches an entity, that item is surfaced in a
// dedicated field/event.  For media items the `cast` array is restructured
// from string[] to ExactMatchCastEntry[] by _normalize_exact_match_cast().
// Non-media types (person, podcast, author, book) are unchanged.
//
// Priority order: movie > tv > person > podcast > book > author.
// In streaming mode, multiple exact_match events may fire (one per source).
// ---------------------------------------------------------------------------

export interface ExactMatchCastEntry {
  name: string;
  id: string | null;
}

export type ExactMatchItem =
  | (Omit<MediaItem, "cast"> & { cast: ExactMatchCastEntry[] })
  | PersonItem
  | PodcastItem
  | AuthorItem
  | BookItem;

// ---------------------------------------------------------------------------
// SearchResponse
//
// Returned by:
//   GET  /api/autocomplete → full envelope (brokered arrays always empty)
//   GET  /api/search       → full envelope (brokered arrays populated)
//
// Both endpoints share this shape.  /api/autocomplete excludes brokered
// API sources (news, video, ratings, artist, album) to avoid excessive
// API calls during typing, so those arrays are always [].
// ---------------------------------------------------------------------------

export interface SearchResponse {
  exact_match: ExactMatchItem | null;
  tv: MediaItem[];
  movie: MediaItem[];
  person: PersonItem[];
  podcast: PodcastItem[];
  author: AuthorItem[];
  book: BookItem[];
  news: NewsItem[];
  video: VideoItem[];
  ratings: RatingsItem[];
  artist: ArtistItem[];
  album: AlbumItem[];
  /** Present when a source-hint prefix was parsed from the query */
  source_hint?: string[];
}

/**
 * Autocomplete responses have the same shape as SearchResponse but the
 * brokered-API arrays (news, video, ratings, artist, album) are always
 * empty — those sources are excluded from autocomplete to avoid excessive
 * API calls during typing.
 */
export type AutocompleteResponse = SearchResponse;

// ---------------------------------------------------------------------------
// SSE event payloads
//
// Used by:
//   GET  /api/autocomplete/stream → emits "result", "exact_match", "done"
//   GET  /api/search/stream       → emits "result", "exact_match", "done"
//
// /api/autocomplete/stream only emits indexed sources (tv, movie, person,
// podcast, author, book).  /api/search/stream also emits brokered sources
// (news, video, ratings, artist, album).
// ---------------------------------------------------------------------------

/** SSE "result" event — one per source as it completes */
export interface StreamResultEvent {
  source: string;
  results:
    | MediaItem[]
    | PersonItem[]
    | PodcastItem[]
    | AuthorItem[]
    | BookItem[]
    | NewsItem[]
    | VideoItem[]
    | RatingsItem[]
    | ArtistItem[]
    | AlbumItem[];
  latency_ms: number;
}

/** SSE "exact_match" event — emitted as soon as a source yields an exact match */
export type StreamExactMatchEvent = ExactMatchItem;

/** SSE "done" event — signals all sources have completed */
export interface StreamDoneEvent {
  source_hint?: string[];
}

// ---------------------------------------------------------------------------
// Detail response re-exports
//
// Returned by:
//   POST /api/details → one of these depending on mc_type / mc_subtype:
//     mc_type="movie" or "tv"                    → MediaDetailResponse
//     mc_type="person", mc_subtype!="author"      → PersonDetailResponse
//     mc_type="podcast"                           → PodcastDetailResponse
//     mc_type="person", mc_subtype="author"       → AuthorDetailResponse
//     mc_type="book"                              → BookDetailResponse
//     (on error)                                  → DetailsErrorResponse
// ---------------------------------------------------------------------------

export type { MediaDetailResponse } from "./media_document";
export type { PersonDetailResponse } from "./person_document";
export type { PodcastDetailResponse } from "./podcast_document";
export type { AuthorDetailResponse } from "./author_document";
export type { BookDetailResponse } from "./book_document";

/** POST /api/details → returned when lookup or enrichment fails */
export interface DetailsErrorResponse {
  error: string;
  status_code: number;
}
