"""Offline tests of actual added methods with host dependencies stubbed.

The uploaded archive omits utils/api.tmdb/contracts; AST extraction loads only
new methods/models, unchanged, with a minimal Redis test double. This does not
claim to retest the real RedisCache or Firebase deployment.
"""
import ast
import asyncio
import copy
import functools
import inspect
import json
import logging
import os
from pathlib import Path

os.environ["ENVIRONMENT"] = "test"
import types
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

from pydantic import BaseModel, ConfigDict, Field, model_validator

ROOT = Path(__file__).resolve().parents[1]


class RedisCache:
    instances = []
    def __init__(self, **kwargs):
        self.config, self.values = kwargs, {}
        self.instances.append(self)
    @staticmethod
    def use_cache(cache, prefix):
        def decorate(fn):
            @functools.wraps(fn)
            async def cached(*args, **kwargs):
                bound = inspect.signature(fn).bind(*args, **kwargs)
                bound.apply_defaults()
                key = (prefix, repr([(k, v) for k, v in bound.arguments.items() if k != 'self']))
                if key not in cache.values:
                    cache.values[key] = copy.deepcopy(await fn(*args, **kwargs))
                return copy.deepcopy(cache.values[key])
            return cached
        return decorate


# Compile the real core with only BaseAPIClient/logger imports replaced.
core_tree = ast.parse((ROOT / 'core.py').read_text())
core_tree.body = [n for n in core_tree.body if not (isinstance(n, ast.ImportFrom) and n.module.startswith('utils.'))]
ns = {'RedisCache': RedisCache, 'BaseAPIClient': object, 'get_logger': logging.getLogger}
exec(compile(core_tree, 'core.py', 'exec'), ns)
Service = ns['WatchmodeService']

# Compile the real sports models with Pydantic, without unrelated catalog models.
model_tree = ast.parse((ROOT / 'models.py').read_text())
model_tree.body = [n for n in model_tree.body if isinstance(n, ast.ClassDef) and n.name.startswith('WatchmodeSports')]
models = {'BaseModelWithMethods': BaseModel, 'ConfigDict': ConfigDict, 'Field': Field,
          'model_validator': model_validator, 'Any': Any, '__name__': 'sports_test_models'}
exec(compile(ast.parse('from __future__ import annotations\n' + ast.unparse(model_tree)), 'models.py', 'exec'), models)
for name in ('WatchmodeSportsParticipant', 'WatchmodeSportsEvent', 'WatchmodeSportsResponse'):
    models[name].model_rebuild(_types_namespace=models)
Event, Response = models['WatchmodeSportsEvent'], models['WatchmodeSportsResponse']

wrapper_tree = ast.parse((ROOT / 'wrappers.py').read_text())
cls = next(n for n in wrapper_tree.body if isinstance(n, ast.ClassDef) and n.name == 'WatchmodeWrapper')
cls.body = [n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'get_sports']
wrapper_tree.body = [cls]
wns = {'datetime': datetime, 'UTC': UTC, 'os': os, 'asyncio': asyncio,
       'logger': logging.getLogger('sports'),
       'watchmode_auth': types.SimpleNamespace(watchmode_api_key='test-key'),
       'WatchmodeService': Service, 'WatchmodeSportsEvent': Event, 'WatchmodeSportsResponse': Response}
exec(compile(wrapper_tree, 'wrappers.py', 'exec'), wns)
Wrapper = wns['WatchmodeWrapper']


def event(n=1, league=10, teams=True):
    return {'id': n, 'name': 'Example tournament' if not teams else 'Home at Away', 'event_type': 'sport',
            'league': {'id': league, 'name': 'Example League'},
            'home_team': {'id': 101, 'name': 'Home', 'logo_url': 'not-licensed'} if teams else None,
            'away_team': {'id': 102, 'name': 'Away'} if teams else None,
            'start_time': datetime.now(UTC).replace(hour=20, minute=0, second=0, microsecond=0).isoformat(),
            'status': 'scheduled', 'sources_state': 'known_options',
            'sources': [{'provider_id': 1, 'freshness': 'current', 'links': [
                {'platform': 'web', 'type': 'event', 'url': 'https://example/event', 'requires_subscription': None}]}],
            'event_destinations': [{'provider_id': 2, 'links': [{'url': 'https://example/alternate', 'platform': 'web'}]}]}


class CoreTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for c in RedisCache.instances:
            c.values.clear()
        self.service = Service('test-key')
    async def test_header_auth_no_input_mutation_no_catalog_cache(self):
        self.service._core_async_request = AsyncMock(return_value=({'access_level': 'business'}, 200))
        params = {'region': 'US'}
        await self.service._make_live_request('/live-coverage/', params)
        call = self.service._core_async_request.call_args.kwargs
        self.assertEqual(call['headers'], {'X-API-Key': 'test-key'})
        self.assertEqual(params, {'region': 'US'})
        self.assertEqual(call['max_retries'], 1)
        self.assertTrue(call['return_status_code'])
    async def test_error_not_cached(self):
        self.service._core_async_request = AsyncMock(side_effect=[(None, 503), ({'access_level': 'business'}, 200)])
        with self.assertRaises(RuntimeError):
            await self.service.get_live_coverage()
        self.assertEqual((await self.service.get_live_coverage())['access_level'], 'business')
        self.assertEqual(self.service._core_async_request.call_count, 2)
    async def test_missing_event_reports_not_found(self):
        self.service._core_async_request = AsyncMock(return_value=(None, 404))
        with self.assertRaises(ns['WatchmodeLiveError']) as caught:
            await self.service.get_live_event(99, 'US')
        self.assertEqual(caught.exception.status_code, 404)
        self.assertFalse(ns['WatchmodeSportsScheduleCache'].values)
    async def test_directories_are_cached(self):
        self.service._core_async_request = AsyncMock(return_value=({'leagues': []}, 200))
        await self.service.get_live_leagues()
        await self.service.get_live_leagues()
        self.assertEqual(self.service._core_async_request.call_count, 1)
    async def test_bulk_continuation_only_and_complete_cache(self):
        self.service._core_async_request = AsyncMock(side_effect=[({'access_level': 'business'}, 200),
            ({'events': [event()], 'snapshot_id': 's', 'has_more': True, 'next_cursor': 'c'}, 200),
            ({'events': [event(2)], 'snapshot_id': 's', 'has_more': False, 'next_cursor': None}, 200)])
        r = await self.service.get_live_events_day('2026-10-29', 'US')
        self.assertEqual(len(r['events']), 2)
        self.assertEqual(r['timezone'], 'America/New_York')
        self.assertEqual(self.service._core_async_request.call_args.kwargs['params'], {'continuation': 'c'})
        await self.service.get_live_events_day('2026-10-29', 'US')
        self.assertEqual(self.service._core_async_request.call_count, 3)
    async def test_bulk_failed_page_never_caches_partial_result(self):
        self.service._core_async_request = AsyncMock(side_effect=[({'access_level': 'business'}, 200),
            ({'events': [event()], 'snapshot_id': 's', 'has_more': True, 'next_cursor': 'c'}, 200), (None, 500)])
        with self.assertRaises(RuntimeError):
            await self.service.get_live_events_day('2026-10-29', 'US')
        self.assertFalse(ns['WatchmodeSportsScheduleCache'].values)
    async def test_startup_omits_sources_and_paginates(self):
        self.service._core_async_request = AsyncMock(side_effect=[({'access_level': 'startup'}, 200),
            ({'events': [event()], 'total_pages': 2}, 200), ({'events': [event(2)], 'total_pages': 2}, 200)])
        r = await self.service.get_live_events_day('2026-10-29', 'US')
        self.assertEqual(len(r['events']), 2)
        self.assertEqual(self.service._core_async_request.call_args.kwargs['params']['include_sources'], 'false')
    async def test_dst_uses_local_midnights(self):
        self.service._core_async_request = AsyncMock(side_effect=[
            ({'access_level': 'startup'}, 200), ({'events': [], 'total_pages': 1}, 200),
        ])
        await self.service.get_live_events_day('2026-11-01', 'US')
        p = self.service._core_async_request.call_args.kwargs['params']
        start = datetime.fromisoformat(p['date_from']).astimezone(UTC)
        end = datetime.fromisoformat(p['date_to']).astimezone(UTC)
        self.assertEqual((end-start).total_seconds(), 25*3600)
    async def test_source_cache_is_per_country(self):
        self.service._core_async_request = AsyncMock(return_value=({'sources': []}, 200))
        await self.service.get_live_event_sources(1, 'US')
        await self.service.get_live_event_sources(1, 'US')
        await self.service.get_live_event_sources(1, 'CA')
        self.assertEqual(self.service._core_async_request.call_count, 2)
    async def test_ttl_configuration(self):
        self.assertEqual(ns['WatchmodeSportsScheduleCache'].config['defaultTTL'], 86400)
        self.assertEqual(ns['WatchmodeSportsSourceCache'].config['defaultTTL'], 86400)
        self.assertEqual(ns['WatchmodeSportsDirectoryCache'].config['defaultTTL'], 86400)
        self.assertEqual(
            ns['WatchmodeSportsTeamDirectoryCache'].config['defaultTTL'], 7 * 24 * 60 * 60
        )
        self.assertNotIn('WatchmodeSportsStatusCache', ns)


class ModelTests(unittest.TestCase):
    def test_non_team_event(self):
        self.assertEqual(Event(**event(teams=False)).display_kind, 'event')
    def test_no_team_logo(self):
        self.assertNotIn('logo', Event(**event()).model_dump_json())
    def test_expired_links_and_unknown_subscription(self):
        raw = event()
        raw['sources'][0]['links'][0]['refresh_after'] = '2000-01-01T00:00:00Z'
        r = Event(**raw)
        self.assertEqual(r.sources[0]['links'], [])
        self.assertTrue(r.event_destinations[0]['links'])
        self.assertTrue(r.viewing_links_need_refresh)
        self.assertTrue(raw['sources'][0]['links'])  # cached value untouched
    def test_invalid_and_naive_link_deadlines(self):
        for value in ('bad', '2099-01-01T00:00:00'):
            raw = event()
            raw['event_destinations'][0]['links'][0]['refresh_after'] = value
            self.assertEqual(Event(**raw).event_destinations[0]['links'], [])


class WrapperTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.day = datetime.now(UTC).date().isoformat()
        self.items = [event(), event(2, 20, False)]
        now = datetime.now(UTC).isoformat()
        self.snapshot = {
            'events': self.items,
            'fetched_at': now,
            'date_range_limited': False,
            'effective_date_from': self.day + 'T00:00:00+00:00',
            'effective_date_to': self.day + 'T23:59:59+00:00',
        }
        self.fake = types.SimpleNamespace(
            get_live_events_day=AsyncMock(return_value=self.snapshot),
            get_live_event_sources=AsyncMock(return_value={**event(), 'fetched_at': now}),
            get_live_event=AsyncMock(return_value=event()),
            get_live_coverage=AsyncMock(return_value={'access_level': 'business'}),
            get_live_leagues=AsyncMock(return_value={'leagues': [{'id': 10, 'name': 'Example'}]}),
            get_live_teams=AsyncMock(return_value={'teams': [{'id': 101, 'name': 'Home', 'logo_url': 'x'}]}),
            get_ncaam_scoreboard=AsyncMock(return_value={'events': []}),
            get_college_rankings=AsyncMock(return_value={'rankings': []}),
        )
        self.original = wns['WatchmodeService']
        wns['WatchmodeService'] = lambda key: self.fake
        self.wrapper = Wrapper()
    def tearDown(self):
        wns['WatchmodeService'] = self.original
    async def read(self, **kw):
        timezone_name = kw.pop('timezone_name', 'UTC')
        return await self.wrapper.get_sports(
            date=self.day, timezone_name=timezone_name, **kw
        )
    async def test_follow_union_and_empty_preferences(self):
        r = await self.read(mode='for_you', followed_team_ids='101', followed_league_ids='20')
        self.assertEqual(r.total_results, 2)
        r = await self.read(mode='for_you')
        self.assertEqual(r.total_results, 0)
        self.assertTrue(r.preferences_required)
    async def test_filters_not_sent_to_shared_core(self):
        await self.read(mode='for_you', followed_team_ids='101', league_ids='10')
        viewer_start = datetime.strptime(self.day, '%Y-%m-%d').replace(tzinfo=UTC)
        expected = [(ny_date, 'US') for ny_date in ns['new_york_dates_for_viewer_day'](viewer_start)]
        actual = [call.args for call in self.fake.get_live_events_day.call_args_list]
        self.assertEqual(actual, expected)
        self.assertTrue(all(len(args) == 2 for args in actual))
    async def test_missing_event_is_not_an_outage(self):
        from api.watchmode.core import WatchmodeLiveError
        self.fake.get_live_event = AsyncMock(side_effect=WatchmodeLiveError(404, 'NOT_FOUND'))
        response = await self.read(action='event', event_id=1)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.code, 'NOT_FOUND')
    async def test_sections_sort_live_ahead_of_final(self):
        completed = event(1)
        completed['status'] = 'final'
        live = event(2)
        live['status'] = 'live'
        live['start_time'] = completed['start_time']
        self.snapshot['events'] = [completed, live]
        self.fake.get_live_events_day.return_value = self.snapshot
        first = await self.read(limit=1)
        self.assertEqual(first.events[0].section, 'live')
        self.assertEqual(first.events[0].id, 2)
    async def test_schedule_snapshot_is_not_followed_by_a_status_download(self):
        r = await self.read()
        self.assertFalse(hasattr(self.fake, 'get_live_status_day'))
        self.assertEqual(r.events[0].status, 'scheduled')
        self.assertTrue(r.events[0].sources)
        self.assertEqual(r.status_fetched_at, self.snapshot['fetched_at'])
        self.assertFalse(r.status_refresh_failed)
    async def test_pagination_version(self):
        first = await self.read(limit=1)
        second = await self.read(limit=1, page=2, version=first.version)
        self.assertEqual(second.events[0].id, 2)
        self.assertEqual((await self.read(page=2)).status_code, 409)
        self.assertEqual((await self.read(version='old')).status_code, 409)
    async def test_bad_queries(self):
        for args in ({'limit': 101}, {'mode': 'bad'}, {'league_ids': '-1'}, {'dma': '501'}, {'action': 'bad'}):
            self.assertEqual((await self.read(**args)).status_code, 400)
    async def test_event_sources_and_directories(self):
        self.assertEqual((await self.read(action='event', event_id=1)).event.id, 1)
        self.assertTrue((await self.read(action='sources', event_id=1)).sources)
        self.assertEqual(len((await self.read(action='leagues')).leagues), 1)
        self.assertNotIn('logo', (await self.read(action='teams', league_id=10)).model_dump_json())
    async def test_multi_league_team_search(self):
        async def teams_for_league(lid: int) -> dict[str, list[dict[str, object]]]:
            if lid == 10:
                return {'teams': [{'id': 1, 'name': 'Patriots', 'short_name': 'NE'}]}
            if lid == 20:
                return {'teams': [{'id': 2, 'name': 'Lakers', 'short_name': 'LAL'}]}
            return {'teams': []}

        self.fake.get_live_teams = AsyncMock(side_effect=teams_for_league)
        response = await self.read(
            action='teams', league_ids='10,20', search='Lak', limit=100
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.total_results, 1)
        self.assertEqual(response.teams[0]['id'], 2)
        self.assertEqual(response.teams[0]['league_id'], 20)
    async def test_duplicate_team_names_stay_separate_records(self):
        self.fake.get_live_teams = AsyncMock(return_value={'teams': [
            {'id': 234220, 'name': 'Seattle Sounders', 'league_id': 470},
            {'id': 1093, 'name': 'Seattle Sounders FC', 'short_name': 'Seattle Sounders FC',
             'league_id': 470},
        ]})
        listed = await self.read(action='teams', league_id=470, search='seattle', limit=100)
        self.assertEqual(listed.total_results, 2)
        self.assertEqual(
            {team['name'] for team in listed.teams},
            {'Seattle Sounders', 'Seattle Sounders FC'},
        )
        kickoff = datetime.now(UTC).replace(hour=20, minute=0, second=0, microsecond=0).isoformat()
        game = event(9, 470)
        game['home_team'] = {'id': 1093, 'name': 'Seattle Sounders FC'}
        game['away_team'] = {'id': 50, 'name': 'Opponent'}
        game['start_time'] = kickoff
        self.fake.get_live_events_day = AsyncMock(return_value={**self.snapshot, 'events': [game]})
        self.fake.get_live_teams.reset_mock()
        other_record = await self.read(
            action='events', team_ids='234220', team_league_hints='234220:470', limit=100
        )
        self.assertEqual(other_record.events, [])
        selected = await self.read(
            action='events', team_ids='1093', team_league_hints='1093:470', limit=100
        )
        self.assertEqual([item.id for item in selected.events], [9])
        self.fake.get_live_teams.assert_not_called()
    async def test_multi_league_team_search_requires_query(self):
        bad = await self.read(action='teams', league_ids='10,20', search='L')
        self.assertEqual(bad.status_code, 400)
    async def test_ncaam_espn_start_time_override(self):
        game = event(n=99, league=472)
        game['start_time'] = '2026-11-02T05:00:00+00:00'
        game['home_team'] = {
            'id': 2138,
            'name': 'Syracuse Orange',
            'logo_url': 'https://a.espncdn.com/i/teamlogos/ncaa/500/183.png',
        }
        game['away_team'] = {
            'id': 2042,
            'name': 'New Haven Chargers',
            'logo_url': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2441.png',
        }
        self.snapshot['events'] = [game]
        self.fake.get_ncaam_scoreboard = AsyncMock(return_value={
            'events': [{
                'competitions': [{
                    'date': '2026-11-03T00:00Z',
                    'competitors': [
                        {'homeAway': 'away', 'team': {'id': '2441'}},
                        {'homeAway': 'home', 'team': {'id': '183'}},
                    ],
                }],
            }],
        })
        response = await self.read(timezone_name='America/New_York')
        self.assertEqual(response.total_results, 1)
        self.assertEqual(response.events[0].start_time, '2026-11-03T00:00:00+00:00')
        self.fake.get_ncaam_scoreboard.assert_awaited_once()
        self.assertEqual(
            self.fake.get_ncaam_scoreboard.call_args.args[0],
            self.day.replace('-', ''),
        )
    async def test_no_auth_returns_service_unavailable(self):
        wns['watchmode_auth'].watchmode_api_key = None
        try:
            self.assertEqual((await self.read()).status_code, 503)
        finally:
            wns['watchmode_auth'].watchmode_api_key = 'test-key'


# Load the actual new handler, retaining the existing payload serialization helper.
handler_tree = ast.parse((ROOT / 'handlers.py').read_text())
handler_class = next(n for n in handler_tree.body if isinstance(n, ast.ClassDef) and n.name == 'WatchmodeHandler')
handler_class.body = [n for n in handler_class.body if isinstance(n, ast.FunctionDef) and n.name == 'get_sports']
handler_tree.body = [n for n in handler_tree.body if isinstance(n, ast.FunctionDef) and n.name == '_safe_model_payload'] + [handler_class]
class HttpResponse:
    def __init__(self, body, status=200, headers=None):
        self.body, self.status, self.headers = body, status, headers
fake_wrapper = types.SimpleNamespace(get_sports=AsyncMock(return_value=Response()))
hns = {'https_fn': types.SimpleNamespace(Request=object, Response=HttpResponse), 'json': json,
       'logger': logging.getLogger('handler'), 'run_async': asyncio.run, 'watchmode_wrapper': fake_wrapper}
exec(compile(handler_tree, 'handlers.py', 'exec'), hns)

class HandlerTests(unittest.TestCase):
    def setUp(self):
        fake_wrapper.get_sports.reset_mock()
        fake_wrapper.get_sports.return_value = Response()
        self.handler = hns['WatchmodeHandler']()
    def test_routes_and_parses_queries(self):
        request = types.SimpleNamespace(method='GET', args={'action': 'sources', 'event_id': '123', 'timezone': 'UTC'})
        response = self.handler.get_sports(request)
        self.assertEqual(response.status, 200)
        self.assertEqual(fake_wrapper.get_sports.call_args.kwargs, {'action': 'sources', 'event_id': 123, 'timezone_name': 'UTC'})
    def test_rejects_cache_bypass(self):
        response = self.handler.get_sports(types.SimpleNamespace(method='GET', args={'force_refresh': 'true'}))
        self.assertEqual(response.status, 400)
        fake_wrapper.get_sports.assert_not_called()
    def test_propagates_wrapper_error(self):
        fake_wrapper.get_sports.return_value = Response(status_code=409, code='SNAPSHOT_CHANGED')
        response = self.handler.get_sports(types.SimpleNamespace(method='GET', args={}))
        self.assertEqual(response.status, 409)
        self.assertEqual(json.loads(response.body)['code'], 'SNAPSHOT_CHANGED')
    def test_accepts_team_league_hints(self):
        response = self.handler.get_sports(types.SimpleNamespace(
            method='GET', args={'action': 'events', 'team_league_hints': '1093:470'}
        ))
        self.assertEqual(response.status, 200)
        self.assertEqual(
            fake_wrapper.get_sports.call_args.kwargs['team_league_hints'], '1093:470'
        )
    def test_method_and_integer_validation(self):
        self.assertEqual(self.handler.get_sports(types.SimpleNamespace(method='POST', args={})).status, 405)
        self.assertEqual(self.handler.get_sports(types.SimpleNamespace(method='GET', args={'event_id':'bad'})).status, 400)

class WarmingTests(unittest.IsolatedAsyncioTestCase):
    async def test_warming_reuses_cached_methods_and_tolerates_failed_day(self):
        service = Service('key')
        service.get_live_leagues = AsyncMock(return_value={'leagues': []})
        service.get_live_events_day = AsyncMock(side_effect=[RuntimeError('offline'), {}, {}])
        result = await service.warm_sports_cache('US', days_before=1, days_after=1)
        self.assertEqual(len(result['failed_dates']), 1)
        self.assertEqual(len(result['completed_dates']), 2)
        self.assertEqual(service.get_live_events_day.call_count, 3)
        self.assertFalse(hasattr(service, 'get_live_status_day'))


if __name__ == '__main__':
    unittest.main()
