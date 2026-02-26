/**
 * TypeScript interface for Spotify album items.
 *
 * Source: Spotify Web API via SpotifyAlbum.model_dump().
 * NOT stored in a Redis index.
 *
 * Keep in sync with: src/api/spotify/models.py – SpotifyAlbum
 */

import type { MCLink, MCImage } from "./mc_base_item";

// ---------------------------------------------------------------------------
// AlbumItem — Spotify album
//
// Returned by:
//   GET  /api/search        → response.album[]
//   GET  /api/search/stream → SSE "result" event where source is "album"
//
// NOT returned by /api/autocomplete or /api/autocomplete/stream.
// ---------------------------------------------------------------------------

export interface AlbumItem {
  // --- MCBaseItem fields ----------------------------------------------------
  mc_id: string;
  mc_type: "music_album";
  mc_subtype: string | null;
  source: "spotify";
  source_id: string | null;
  links: MCLink[];
  images: MCImage[];
  metrics: Record<string, unknown>;
  external_ids: Record<string, unknown>;
  error: string | null;
  status_code: number;
  sort_order: number;

  // --- SpotifyAlbum fields --------------------------------------------------
  id: string;
  title: string;
  /** e.g. "album", "single", "compilation" */
  album_type: string | null;
  artist: string | null;
  artist_id: string | null;
  spotify_url: string | null;
  default_image: string | null;
  /** MusicBrainz ID */
  mbid: string;
  artist_url: string | null;
  /** YYYY-MM-DD or YYYY */
  release_date: string | null;
  /** "year", "month", or "day" */
  release_date_precision: string | null;
  total_tracks: number | null;

  // --- Cross-platform links -------------------------------------------------
  apple_music_url: string | null;
  youtube_music_url_ios: string | null;
  youtube_music_url_android: string | null;
  youtube_music_url_web: string | null;
}
