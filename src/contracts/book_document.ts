/**
 * TypeScript interfaces for book items (OpenLibrary).
 *
 * Keep in sync with:
 *   src/api/openlibrary/bulk/load_book_index.py – mc_book_to_redis_doc()
 *   src/services/search_service.py              – parse_doc(), _get_book_details()
 */

// ---------------------------------------------------------------------------
// Enum-like string unions
// ---------------------------------------------------------------------------

export type BookMCType = "book";
export type BookSource = "openlibrary";

// ---------------------------------------------------------------------------
// Nested object interfaces
// ---------------------------------------------------------------------------

export interface BookCoverUrls {
  small?: string;
  medium?: string;
  large?: string;
}

// ---------------------------------------------------------------------------
// BookItem — list-level book item
//
// Returned by:
//   GET  /api/autocomplete        → response.book[]
//   GET  /api/autocomplete/stream → SSE "result" event where source is "book"
//   GET  /api/search              → response.book[]
//   GET  /api/search/stream       → SSE "result" event where source is "book"
//
// Also eligible as an exact_match item (see api_responses.ts).
//
// Shape: Redis JSON document + parse_doc() injections (mc_id).
// ---------------------------------------------------------------------------

export interface BookItem {
  /** Unique doc key, e.g. "book_OL45883W" */
  id: string;
  /** Alias of id — always injected by parse_doc() */
  mc_id: string;
  /** OpenLibrary work key, e.g. "OL45883W" */
  key: string;
  source_id: string;

  /** Display title (parse_doc copies title → search_title) */
  search_title: string;
  /** Display title */
  title: string;
  /** Book description / blurb */
  description: string | null;

  // --- Author fields --------------------------------------------------------

  /** Primary author name */
  author: string;
  /** All author names as array */
  author_name: string[];
  /** Space-joined author names for TEXT search */
  author_search: string;
  /** Author OpenLibrary IDs (legacy relational field) */
  matching_author_olids: string[];
  /** Author OpenLibrary IDs */
  author_olids: string[];
  /** Normalized author name for exact TAG matching */
  author_normalized: string | null;

  // --- Type tags ------------------------------------------------------------

  mc_type: BookMCType;
  source: BookSource;

  // --- OpenLibrary identifiers ----------------------------------------------

  /** e.g. "OL45883W" */
  openlibrary_key: string | null;
  /** Full OpenLibrary URL */
  openlibrary_url: string | null;

  // --- ISBNs ----------------------------------------------------------------

  /** All known ISBNs */
  isbn: string[];
  primary_isbn13: string | null;
  primary_isbn10: string | null;

  // --- Publication info -----------------------------------------------------

  first_publish_year: number | null;
  publisher: string | null;
  first_sentence: string[];

  // --- Subjects / categories ------------------------------------------------

  /** Raw subjects array (display) */
  subject: string[];
  /** Raw subjects array (display) */
  subjects: string[];
  /** Space-joined subjects for TEXT search */
  subjects_search: string;
  /** Normalized + IPTC-expanded subjects for TAG matching */
  subjects_normalized: string[];
  language: string | null;

  // --- Cover images ---------------------------------------------------------

  /** OpenLibrary cover ID */
  cover_i: number | null;
  /** String "true" or "false" (TagField requires string) */
  cover_available: string;
  cover_urls: BookCoverUrls;
  /** Primary book cover image URL */
  book_image: string | null;
  /** Standardized image field (same as book_image) */
  image: string | null;
  images: unknown[];

  // --- Ratings / popularity -------------------------------------------------

  ratings_average: number | null;
  ratings_count: number | null;
  readinglog_count: number;
  number_of_pages: number | null;
  edition_count: number | null;
  author_quality_score: number | null;
  /** Composite popularity score (0–100) */
  popularity_score: number;

  // --- Lifecycle timestamps -------------------------------------------------

  /** Unix seconds — set on first index write */
  created_at: number;
  /** Unix seconds — updated on every write */
  modified_at: number;
}

// ---------------------------------------------------------------------------
// BookDetailResponse — enriched book detail
//
// Returned by:
//   POST /api/details → when mc_type is "book"
//
// Extends BookItem with display-friendly aliases (name, overview,
// full_overview, cover_urls).  All data comes from the Redis index.
// ---------------------------------------------------------------------------

export interface BookDetailResponse extends BookItem {
  name: string;
  /** Full description from OpenLibrary */
  full_overview?: string;
  overview?: string;
}
