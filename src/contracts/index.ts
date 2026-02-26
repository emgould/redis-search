/**
 * Barrel export for all TypeScript API contracts.
 *
 * Usage:
 *   import type { MediaItem, SearchResponse, ... } from "@/contracts";
 */

// Shared base types
export type { MCLink, MCImage, MCUrlType } from "./mc_base_item";

// Indexed source items (from Redis via parse_doc)
export type {
  MCSource,
  MCMediaType,
  MCSubType,
  MediaDirector,
  MediaWatchProviders,
  MediaItem,
  MovieItem,
  TvItem,
  MediaItemUnion,
  MediaDetailResponse,
} from "./media_document";

export type {
  PersonMCType,
  PersonMCSubType,
  PersonSource,
  PersonItem,
  PersonProfileImages,
  PersonDetailResponse,
} from "./person_document";

export type {
  PodcastMCType,
  PodcastSource,
  PodcastItem,
  PodcastRSSEpisode,
  PodcastDetailResponse,
} from "./podcast_document";

export type {
  AuthorMCType,
  AuthorMCSubType,
  AuthorSource,
  AuthorItem,
  AuthorDetailResponse,
} from "./author_document";

export type {
  BookMCType,
  BookSource,
  BookCoverUrls,
  BookItem,
  BookDetailResponse,
} from "./book_document";

// Brokered API source items (from external APIs via model_dump)
export type { NewsSource, NewsItem } from "./news_item";
export type { VideoItem } from "./video_item";
export type { RatingsItem } from "./ratings_item";
export type { ArtistItem } from "./artist_item";
export type { AlbumItem } from "./album_item";

// Response envelopes
export type {
  ExactMatchCastEntry,
  ExactMatchItem,
  SearchResponse,
  AutocompleteResponse,
  StreamResultEvent,
  StreamExactMatchEvent,
  StreamDoneEvent,
  DetailsErrorResponse,
} from "./api_responses";
