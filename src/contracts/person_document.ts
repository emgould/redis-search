/**
 * TypeScript interfaces for person items (actors, directors, etc.).
 *
 * Keep in sync with:
 *   src/core/normalize.py              – SearchDocument / document_to_redis()
 *   src/services/person_etl_service.py – person enrichment fields
 *   src/services/search_service.py     – parse_doc(), _get_person_details()
 */

// ---------------------------------------------------------------------------
// Enum-like string unions
// ---------------------------------------------------------------------------

export type PersonMCType = "person";

export type PersonMCSubType =
  | "actor"
  | "director"
  | "writer"
  | "producer"
  | "person";

export type PersonSource = "tmdb";

// ---------------------------------------------------------------------------
// PersonItem — list-level person item
//
// Returned by:
//   GET  /api/autocomplete        → response.person[]
//   GET  /api/autocomplete/stream → SSE "result" event where source is "person"
//   GET  /api/search              → response.person[]
//   GET  /api/search/stream       → SSE "result" event where source is "person"
//
// Also eligible as an exact_match item (see api_responses.ts).
//
// Shape: Redis JSON document + PersonETLService enrichment + parse_doc()
// injections (mc_id, search_title display swap).
// ---------------------------------------------------------------------------

export interface PersonItem {
  /** Unique doc key, e.g. "tmdb_person_17419" */
  id: string;
  /** Alias of id — always injected by parse_doc() */
  mc_id: string;
  /** Display name (parse_doc copies title → search_title) */
  search_title: string;
  /** Display name preserved from original */
  title?: string;
  mc_type: PersonMCType;
  mc_subtype: PersonMCSubType | null;
  source: PersonSource;
  /** Raw TMDB numeric person ID as string */
  source_id: string;
  year: null;
  /** Normalized popularity score (0–100) */
  popularity: number;
  rating: number;
  /** Medium profile image URL */
  image: string | null;
  /** Truncated biography */
  overview: string | null;

  // --- Person enrichment fields (added by PersonETLService) ----------------

  /** Pipe-separated alternate names, e.g. "Brad Pitt | William Bradley Pitt" */
  also_known_as: string;
  /** e.g. "Acting", "Directing", "Writing" */
  known_for_department: string;
  /** YYYY-MM-DD */
  birthday: string | null;
  /** YYYY-MM-DD */
  deathday: string | null;
  place_of_birth: string | null;
  age: number | null;
  is_deceased: boolean;
  /** Top 3 known-for work titles */
  known_for_titles: string[];

  // --- Lifecycle timestamps -------------------------------------------------

  /** Unix seconds — set on first index write */
  created_at: number | null;
  /** Unix seconds — updated on every write */
  modified_at: number | null;
  /** Write provenance tag, e.g. "backfill" */
  _source: string | null;
}

// ---------------------------------------------------------------------------
// PersonDetailResponse — enriched person detail
//
// Returned by:
//   POST /api/details → when mc_type is "person" AND mc_subtype is NOT "author"
//
// Extends PersonItem with live TMDB credits (movie + TV filmography) and
// multi-resolution profile images.
// ---------------------------------------------------------------------------

export interface PersonProfileImages {
  small: string;
  medium: string;
  large: string;
  original: string;
}

export interface PersonDetailResponse extends PersonItem {
  tmdb_id: number;
  name: string;
  known_for_department: string;
  birthday: string | null;
  deathday: string | null;
  place_of_birth: string | null;
  /** Full (untruncated) biography from TMDB */
  full_overview?: string;
  /** Alternate names as array (supplement to pipe-separated also_known_as) */
  also_known_as_list?: string[];
  profile_images?: PersonProfileImages;
  movie_credits: Record<string, unknown>[];
  tv_credits: Record<string, unknown>[];
  credits_metadata: Record<string, unknown>;
}
