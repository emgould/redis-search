/**
 * TypeScript interface for Spotify artist items.
 *
 * Source: Spotify Web API via SpotifyArtist.model_dump().
 * NOT stored in a Redis index.
 *
 * Keep in sync with: src/api/spotify/models.py – SpotifyArtist
 */

import type { MCLink, MCImage } from "./mc_base_item";

// ---------------------------------------------------------------------------
// ArtistItem — Spotify artist
//
// Returned by:
//   GET  /api/search        → response.artist[]
//   GET  /api/search/stream → SSE "result" event where source is "artist"
//
// NOT returned by /api/autocomplete or /api/autocomplete/stream.
// ---------------------------------------------------------------------------

export interface ArtistItem {
  // --- MCBaseItem fields ----------------------------------------------------
  mc_id: string;
  mc_type: "person";
  mc_subtype: "music_artist";
  source: "spotify";
  source_id: string | null;
  links: MCLink[];
  images: MCImage[];
  metrics: Record<string, unknown>;
  external_ids: Record<string, unknown>;
  error: string | null;
  status_code: number;
  sort_order: number;

  // --- SpotifyArtist fields -------------------------------------------------
  id: string;
  name: string;
  spotify_url: string | null;
  artist_link: MCLink | null;
  popularity: number;
  followers: number;
  genres: string[];
  default_image: string | null;
  /** Compatibility alias for cards expecting "artist" */
  artist: string | null;
  /** Compatibility alias for cards expecting "title" */
  title: string | null;
  known_for: string | null;

  // --- Top-track metadata ---------------------------------------------------
  top_track_album: string | null;
  top_track_release_date: string | null;
  top_track_album_image: string | null;
  top_track_track: string | null;
}
