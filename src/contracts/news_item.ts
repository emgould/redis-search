/**
 * TypeScript interface for news article items.
 *
 * Source: NewsAI / Event Registry API via MCNewsItem.model_dump().
 * NOT stored in a Redis index.
 *
 * Keep in sync with: src/api/newsai/models.py – MCNewsItem
 */

import type { MCLink, MCImage } from "./mc_base_item";

// ---------------------------------------------------------------------------
// Nested
// ---------------------------------------------------------------------------

export interface NewsSource {
  id: string | null;
  name: string;
}

// ---------------------------------------------------------------------------
// NewsItem — news article
//
// Returned by:
//   GET  /api/search        → response.news[]
//   GET  /api/search/stream → SSE "result" event where source is "news"
//
// NOT returned by /api/autocomplete or /api/autocomplete/stream (brokered
// API sources are excluded from autocomplete to avoid excessive API calls).
// ---------------------------------------------------------------------------

export interface NewsItem {
  // --- MCBaseItem fields ----------------------------------------------------
  mc_id: string;
  mc_type: "news_article";
  mc_subtype: string | null;
  source: "newsai";
  source_id: string | null;
  links: MCLink[];
  images: MCImage[];
  metrics: Record<string, unknown>;
  external_ids: Record<string, unknown>;
  error: string | null;
  status_code: number;
  sort_order: number;

  // --- MCNewsItem fields ----------------------------------------------------
  id: string | null;
  title: string;
  description: string | null;
  content: string | null;
  url: string;
  url_to_image: string | null;
  published_at: string | null;
  author: string | null;
  news_source: NewsSource | null;

  /** Event Registry unique identifier */
  uri: string | null;
  lang: string | null;
  is_duplicate: boolean | null;
  /** Publishing date */
  date: string | null;
  /** Publishing time */
  time: string | null;
  /** Combined datetime */
  date_time: string | null;
  /** Similarity score */
  sim: number | null;
  /** Sentiment score (-1 to 1) */
  sentiment: number | null;
  /** Weight/importance score */
  wgt: number | null;
  /** Relevance score for search results */
  relevance: number | null;
}
