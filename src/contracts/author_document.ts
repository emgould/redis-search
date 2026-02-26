/**
 * TypeScript interfaces for author items (OpenLibrary).
 *
 * Keep in sync with:
 *   src/api/openlibrary/bulk/load_author_index.py – mc_author_to_redis_doc()
 *   src/services/search_service.py                – parse_doc(), _get_author_details()
 */

// ---------------------------------------------------------------------------
// Enum-like string unions
// ---------------------------------------------------------------------------

export type AuthorMCType = "person";
export type AuthorMCSubType = "author";
export type AuthorSource = "openlibrary";

// ---------------------------------------------------------------------------
// AuthorItem — list-level author item
//
// Returned by:
//   GET  /api/autocomplete        → response.author[]
//   GET  /api/autocomplete/stream → SSE "result" event where source is "author"
//   GET  /api/search              → response.author[]
//   GET  /api/search/stream       → SSE "result" event where source is "author"
//
// Also eligible as an exact_match item (see api_responses.ts).
//
// Shape: Redis JSON document + parse_doc() injections (mc_id).
// ---------------------------------------------------------------------------

export interface AuthorItem {
  /** Unique doc key, e.g. "author_OL23919A" */
  id: string;
  /** Alias of id — always injected by parse_doc() */
  mc_id: string;
  /** OpenLibrary key, e.g. "OL23919A" */
  key: string;
  source_id: string;

  /** Display name (parse_doc copies title → search_title) */
  search_title: string;
  /** Display name */
  name: string;
  /** Author biography */
  bio: string | null;

  mc_type: AuthorMCType;
  mc_subtype: AuthorMCSubType;
  source: AuthorSource;

  // --- Dates ----------------------------------------------------------------

  /** e.g. "24 September 1896" (OpenLibrary free-text format) */
  birth_date: string | null;
  death_date: string | null;

  // --- External identifiers -------------------------------------------------

  /** OpenLibrary remote_ids object (e.g. { wikidata: "Q36322", viaf: "..." }) */
  remote_ids: Record<string, string>;
  /** e.g. "OL23919A" */
  openlibrary_key: string | null;
  /** Full OpenLibrary URL */
  openlibrary_url: string | null;

  // --- Numeric / scoring fields ---------------------------------------------

  /** Number of works attributed to this author */
  work_count: number;
  /** Wikidata-derived quality score */
  quality_score: number;

  // --- Wikidata enrichment --------------------------------------------------

  wikidata_id: string | null;
  wikidata_name: string | null;
  wikidata_birth_year: number | null;

  // --- Images ---------------------------------------------------------------

  /** Primary author image URL (OpenLibrary or Wikidata) */
  image: string | null;
  images: unknown[];
  /** Keyed URLs, e.g. { openlibrary: "...", wikidata: "..." } */
  photo_urls: Record<string, string>;

  // --- Links ----------------------------------------------------------------

  author_links: unknown[];

  // --- Lifecycle timestamps -------------------------------------------------

  /** Unix seconds — set on first index write */
  created_at: number;
  /** Unix seconds — updated on every write */
  modified_at: number;
}

// ---------------------------------------------------------------------------
// AuthorDetailResponse — enriched author detail
//
// Returned by:
//   POST /api/details → when mc_type is "person" AND mc_subtype is "author"
//
// Extends AuthorItem with display-friendly aliases (overview, full_overview).
// All data comes from the Redis index (no external API call).
// ---------------------------------------------------------------------------

export interface AuthorDetailResponse extends AuthorItem {
  /** Full bio text from OpenLibrary */
  full_overview?: string;
  overview?: string;
}
