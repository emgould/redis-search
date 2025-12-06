DEPRECATED ODESLI FIXTURES
==========================

Status: DEPRECATED

These fixture files are NO LONGER ACTIVELY USED in the codebase.
They are preserved for historical reference and potential future use.

MIGRATION
---------
We have migrated from Odesli API to direct Apple Music API integration:
- OLD: Odesli API for cross-platform link expansion
- NEW: api.subapi.apple.wrapper.AppleMusicAPI for Apple Music links
- NEW: YouTube scraping via AppleMusicAPI.get_youtube_video_id()

DEPRECATED FILES
----------------
- make_requests/mock_odesli_response.json
- enrichment/expand_with_odesli.json
- models/odesli_platform_links.json

CURRENT IMPLEMENTATION
----------------------
See: api.subapi.apple.wrapper.AppleMusicAPI
See: api.lastfm.search.LastFMSearchService.get_trending_albums()
See: api.lastfm._models.OdesliPlatformLinks (deprecated model)

Last Updated: 2025-11-06
Migration Completed: 2025-11-06
Reason: Odesli API limitations, moved to direct Apple Music API
