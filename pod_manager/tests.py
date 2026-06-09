"""
Security-focused test pass for the helpers and view changes added in the
recent hardening pass: signed OAuth state, SSRF guard, rate limiting, OTP
attempt accounting, and HTML sanitization. Plus a couple of view-level
integration tests for the Recurly login rate limits.

Transcription tests are appended at the bottom of this file, covering:
  - transcript_path() path helper and bucket math
  - Timestamp formatters (_vtt_timestamp, _srt_timestamp)
  - Format converters (_to_vtt, _to_srt, _to_html, _to_podcast_index_json,
    _to_words_json, _plain_text)
  - Response parser (_parse_whisper_response / _parse_srt)
  - Transcript model (get_url, auto_delete_transcript_files signal)
  - queue_transcription_on_episode_save signal
  - run_transcription() service (mocked whisper, option fallback chain)
  - apply_speaker_labels() (file rewrite, graceful no-op on missing data)
  - serve_transcript view (ETag, caching, Content-Disposition, 404)
  - backfill_transcripts_api (auth, eligibility filter, IDE vs Celery path)
  - retranscribe_episode_api (owner gate, state reset, IDE vs Celery path)
  - RSS feed transcript tags (_finalize_xml inserts podcast:transcript elements)
  - apply_approved_edit() speaker_mappings branch (early return, no metadata lock)
  - handle_update_network / handle_update_show whisper field persistence

Run with: python manage.py test pod_manager
"""
import json
import shutil
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from unittest import mock

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import Client, TestCase, RequestFactory, override_settings
from django.urls import reverse
from django.utils import timezone

from pod_manager import views
from pod_manager.models import (
    Network, PatronProfile, PatreonTier, Podcast, Episode, NetworkMembership,
    NetworkMix, UserMix, EpisodeEditSuggestion, Transcript,
)
from pod_manager.services.edits import (
    apply_approved_edit, parse_chapter_payload, snapshot_episode,
    update_contribution_stats,
)
from pod_manager.services.transcription import (
    _parse_srt,
    _parse_srt_timestamp,
    _parse_whisper_response,
    _plain_text,
    _srt_timestamp,
    _to_html,
    _to_podcast_index_json,
    _to_srt,
    _to_vtt,
    _to_words_json,
    _vtt_timestamp,
    apply_speaker_labels,
    transcript_path,
)


# Use locmem so tests don't depend on Redis being up.
TEST_CACHES = {'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'}}


@override_settings(CACHES=TEST_CACHES)
class ValidatePublicUrlTests(TestCase):
    """SSRF guard: only public, http(s) hosts are allowed through."""

    def test_rejects_empty_and_non_string(self):
        self.assertEqual(views.validate_public_url('')[0], False)
        self.assertEqual(views.validate_public_url(None)[0], False)
        self.assertEqual(views.validate_public_url(12345)[0], False)

    def test_rejects_non_http_schemes(self):
        for url in ['javascript:alert(1)', 'file:///etc/passwd', 'ftp://example.com', 'gopher://x']:
            ok, _ = views.validate_public_url(url)
            self.assertFalse(ok, f"Expected reject for {url}")

    def test_rejects_loopback(self):
        for url in ['http://127.0.0.1/x', 'http://localhost/x', 'http://[::1]/x']:
            ok, reason = views.validate_public_url(url)
            self.assertFalse(ok, f"Expected reject for {url}: {reason}")

    def test_rejects_aws_metadata(self):
        # 169.254.169.254 is link-local — primary cloud-metadata SSRF target.
        ok, reason = views.validate_public_url('http://169.254.169.254/latest/meta-data/')
        self.assertFalse(ok)
        self.assertIn('non-public', reason.lower())

    def test_rejects_private_ranges(self):
        for url in ['http://10.0.0.5/', 'http://192.168.1.1/', 'http://172.16.0.1/']:
            ok, _ = views.validate_public_url(url)
            self.assertFalse(ok, f"Expected reject for {url}")

    def test_rejects_unresolvable_host(self):
        ok, reason = views.validate_public_url('http://this-host-should-never-resolve.invalid/')
        self.assertFalse(ok)
        self.assertIn('resolved', reason.lower())

    def test_accepts_public_url(self):
        # Use mock to avoid real DNS during CI.
        with mock.patch('pod_manager.utils.socket.getaddrinfo',
                        return_value=[(2, 1, 6, '', ('93.184.216.34', 0))]):
            ok, _ = views.validate_public_url('https://example.com/path')
            self.assertTrue(ok)

    def test_dns_rebinding_defense(self):
        # If ANY resolved address is private, reject. Defends against a host
        # that resolves to a public address and a private address simultaneously.
        with mock.patch('pod_manager.utils.socket.getaddrinfo',
                        return_value=[
                            (2, 1, 6, '', ('93.184.216.34', 0)),
                            (2, 1, 6, '', ('10.0.0.1', 0)),
                        ]):
            ok, _ = views.validate_public_url('https://shady.example.com/')
            self.assertFalse(ok)


@override_settings(CACHES=TEST_CACHES)
class RateLimitTests(TestCase):

    def setUp(self):
        cache.clear()

    def test_under_limit_returns_false(self):
        for _ in range(3):
            self.assertFalse(views._is_rate_limited('test_bucket', limit=5, window_seconds=60))

    def test_over_limit_returns_true(self):
        for _ in range(5):
            views._is_rate_limited('test_bucket', limit=5, window_seconds=60)
        # 6th call should be over.
        self.assertTrue(views._is_rate_limited('test_bucket', limit=5, window_seconds=60))

    def test_buckets_are_independent(self):
        for _ in range(5):
            views._is_rate_limited('bucket_a', limit=5, window_seconds=60)
        # bucket_a is at limit, bucket_b is fresh.
        self.assertTrue(views._is_rate_limited('bucket_a', limit=5, window_seconds=60))
        self.assertFalse(views._is_rate_limited('bucket_b', limit=5, window_seconds=60))


@override_settings(CACHES=TEST_CACHES)
class OtpAttemptTests(TestCase):

    def setUp(self):
        cache.clear()

    def test_record_failure_increments(self):
        self.assertEqual(views._record_otp_failure('a@b.com'), 1)
        self.assertEqual(views._record_otp_failure('a@b.com'), 2)
        self.assertEqual(views._record_otp_failure('a@b.com'), 3)

    def test_clear_state_burns_all_keys(self):
        cache.set('recurly_otp_a@b.com', 'ABC123|acct_1', timeout=600)
        cache.set('recurly_account_a@b.com', 'acct_1', timeout=600)
        views._record_otp_failure('a@b.com')
        views._record_otp_failure('a@b.com')

        views._clear_otp_state('a@b.com')

        self.assertIsNone(cache.get('recurly_otp_a@b.com'))
        self.assertIsNone(cache.get('recurly_account_a@b.com'))
        self.assertIsNone(cache.get('recurly_otp_attempts:a@b.com'))


class OauthStateSigningTests(TestCase):

    def test_round_trip(self):
        signed = views._sign_oauth_state('link:42:7')
        self.assertEqual(views._unsign_oauth_state(signed), 'link:42:7')

    def test_returns_none_on_missing(self):
        self.assertIsNone(views._unsign_oauth_state(''))
        self.assertIsNone(views._unsign_oauth_state(None))

    def test_returns_none_on_tampered(self):
        signed = views._sign_oauth_state('link:42:7')
        # Flip a payload byte: signature no longer matches.
        tampered = signed.replace('42', '99', 1)
        self.assertIsNone(views._unsign_oauth_state(tampered))

    def test_returns_none_on_expired(self):
        signed = views._sign_oauth_state('link:42:7')
        # max_age=0 means anything older than now is expired.
        # Sleep a hair to ensure the timestamp is "in the past".
        time.sleep(1.01)
        self.assertIsNone(views._unsign_oauth_state(signed, max_age_seconds=1))


class SanitizeUserHtmlTests(TestCase):

    def test_strips_script(self):
        out = views.sanitize_user_html('<p>hi</p><script>alert(1)</script>')
        self.assertNotIn('<script>', out)
        self.assertIn('<p>hi</p>', out)

    def test_strips_event_handlers(self):
        out = views.sanitize_user_html('<img src="x" onerror="alert(1)">')
        self.assertNotIn('onerror', out)

    def test_allows_safe_formatting(self):
        out = views.sanitize_user_html('<p><strong>bold</strong> <em>italic</em></p>')
        self.assertIn('<strong>bold</strong>', out)
        self.assertIn('<em>italic</em>', out)

    def test_links_get_safe_rel(self):
        out = views.sanitize_user_html('<a href="https://example.com">x</a>')
        self.assertIn('rel=', out)
        self.assertIn('noopener', out)

    def test_empty_input_safe(self):
        self.assertEqual(views.sanitize_user_html(''), '')
        self.assertEqual(views.sanitize_user_html(None), '')


@override_settings(CACHES=TEST_CACHES,
                   RECURLY_API_KEY='test-key',
                   PATREON_CLIENT_ID='test-id',
                   PATREON_CLIENT_SECRET='test-secret')
class RecurlyLoginRateLimitTests(TestCase):
    """
    Integration coverage for the rate limits added to /login/legacy/.
    Stubs out the Recurly client and the OTP-email Celery task so the test
    exercises only the rate-limit logic.
    """

    def setUp(self):
        cache.clear()
        # Networks are required because views resolve `request.network` via
        # middleware. We bypass middleware by calling the view directly with
        # a stub request that already has `.network` attached.
        self.network = Network.objects.create(name='Test Net', slug='test')
        self.factory = RequestFactory()

    def _post(self, data, ip='1.2.3.4'):
        req = self.factory.post(reverse('recurly_login'), data=data,
                                HTTP_X_FORWARDED_FOR=ip)
        # Attach what the middlewares would normally provide.
        req.network = self.network
        # Manually wire session + messages so the view doesn't blow up.
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        from django.contrib.messages.storage.fallback import FallbackStorage
        setattr(req, '_messages', FallbackStorage(req))
        return req

    @mock.patch('pod_manager.views.recurly.Client')
    @mock.patch('pod_manager.tasks.task_send_otp_email')
    def test_per_ip_rate_limit_blocks_after_10(self, mock_task, mock_client_cls):
        # Recurly returns "no account found" so we don't accidentally trigger
        # an OTP send on every request — the rate limit should fire either way.
        mock_client_cls.return_value.list_accounts.return_value.items.return_value = iter([])

        for i in range(10):
            req = self._post({'email': f'user{i}@example.com'}, ip='9.9.9.9')
            resp = views.recurly_login(req)
            self.assertEqual(resp.status_code, 302)

        # 11th request from same IP, fresh email: rate-limited before the
        # Recurly call. Verify by checking the call count didn't go up.
        before_calls = mock_client_cls.return_value.list_accounts.call_count
        req = self._post({'email': 'user-final@example.com'}, ip='9.9.9.9')
        resp = views.recurly_login(req)
        self.assertEqual(resp.status_code, 302)
        after_calls = mock_client_cls.return_value.list_accounts.call_count
        self.assertEqual(after_calls, before_calls,
                         "Expected the 11th request to be blocked before Recurly was called.")

    @mock.patch('pod_manager.views.recurly.Client')
    def test_per_email_rate_limit_blocks_after_5(self, mock_client_cls):
        mock_client_cls.return_value.list_accounts.return_value.items.return_value = iter([])

        # 5 attempts on the same email from different IPs (so the IP limit
        # never triggers) — limit is 5 so the 6th must be blocked.
        for i in range(5):
            req = self._post({'email': 'victim@example.com'}, ip=f'10.0.0.{i+1}')
            views.recurly_login(req)

        before_calls = mock_client_cls.return_value.list_accounts.call_count
        req = self._post({'email': 'victim@example.com'}, ip='10.0.0.99')
        views.recurly_login(req)
        after_calls = mock_client_cls.return_value.list_accounts.call_count
        self.assertEqual(after_calls, before_calls,
                         "Expected the 6th per-email request to short-circuit before Recurly.")


@override_settings(CACHES=TEST_CACHES, RECURLY_API_KEY='test-key')
class RecurlyOtpAttemptTests(TestCase):
    """The OTP cache entry is burned after MAX_OTP_ATTEMPTS bad guesses."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(name='Test Net', slug='test')
        self.user = User.objects.create_user(username='victim@example.com',
                                             email='victim@example.com')
        # Pre-seed the OTP into the cache as if it were just sent.
        cache.set('recurly_otp_victim@example.com', '111111|acct_1', timeout=600)
        self.factory = RequestFactory()

    def _post(self, otp):
        req = self.factory.post(reverse('recurly_login'), data={'otp': otp})
        req.network = self.network
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        req.session['recurly_login'] = {
            'state': 'awaiting_email',
            'email': 'victim@example.com',
            'account_id': 'acct_1',
            'is_second_factor': False,
        }
        req.session.save()
        from django.contrib.messages.storage.fallback import FallbackStorage
        setattr(req, '_messages', FallbackStorage(req))
        return req

    def test_otp_burned_after_max_failures(self):
        # 5 wrong guesses must burn the cache entry.
        for _ in range(views.MAX_OTP_ATTEMPTS):
            req = self._post('000000')
            views.recurly_login(req)
        self.assertIsNone(cache.get('recurly_otp_victim@example.com'),
                          "OTP should be cleared after MAX_OTP_ATTEMPTS failures.")

    def test_correct_otp_still_works_within_attempts(self):
        # Two wrong guesses, then the right one — should still succeed.
        with mock.patch('pod_manager.services.recurly.Client') as mock_client_cls:
            # Stub list_account_subscriptions so the success path doesn't blow up.
            mock_client_cls.return_value.list_account_subscriptions.return_value.items.return_value = iter([])

            req = self._post('999999')
            views.recurly_login(req)
            req = self._post('888888')
            views.recurly_login(req)
            # Cache should still have the OTP at this point.
            self.assertIsNotNone(cache.get('recurly_otp_victim@example.com'))

            # Now the correct code: should clear cache + create profile.
            req = self._post('111111')
            views.recurly_login(req)
            self.assertIsNone(cache.get('recurly_otp_victim@example.com'))
            self.assertTrue(PatronProfile.objects.filter(user=self.user).exists())


# ---------------------------------------------------------------------------
# Helpers used by multiple integration test classes below
# ---------------------------------------------------------------------------

def _make_tenant_request(factory, network, *, method='get', path='/feed/',
                        data=None, user=None):
    """Build a request that already has request.network attached, bypassing
    NetworkMiddleware so view-level tests don't need to install hosts."""
    if method == 'post':
        req = factory.post(path, data=data or {})
    else:
        req = factory.get(path, data=data or {})
    req.network = network
    req.tenant_profile = None
    from django.contrib.sessions.backends.cache import SessionStore
    req.session = SessionStore()
    req.session.create()
    from django.contrib.messages.storage.fallback import FallbackStorage
    setattr(req, '_messages', FallbackStorage(req))
    if user is not None:
        req.user = user
    else:
        from django.contrib.auth.models import AnonymousUser
        req.user = AnonymousUser()
    return req


@override_settings(CACHES=TEST_CACHES)
class FeedEtagStabilityTests(TestCase):
    """generate_custom_feed and generate_public_feed must produce the same
    ETag across successive calls, even when the shell cache is rebuilt
    between them. Otherwise every podcast app downloads the full feed body
    on every poll."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Net', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        # Two episodes so there's at least one item to hash.
        for i in range(2):
            Episode.objects.create(
                podcast=self.podcast,
                title=f"Ep {i}",
                pub_date=timezone.now() - timedelta(days=i),
                raw_description='hello',
                clean_description='hello',
                audio_url_public='https://cdn.example.com/audio.mp3',
            )

    def _get_etag(self, view, **kwargs):
        req = _make_tenant_request(self.factory, self.network, path='/feed/')
        resp = view(req, **kwargs)
        return resp.get('ETag')

    def test_public_feed_etag_stable_across_shell_cache_rebuild(self):
        first = self._get_etag(views.generate_public_feed, podcast_slug='show')
        # Wipe the shell cache to simulate a podcast-update rebuild or
        # autoreload. Episode fragments stay so we're really testing that
        # lastBuildDate is pinned to episode pub_date, not "now".
        cache.delete(f"feed_shell_public_{self.podcast.id}")
        second = self._get_etag(views.generate_public_feed, podcast_slug='show')
        self.assertIsNotNone(first)
        self.assertEqual(first, second,
                         "ETag must not change when the shell cache is rebuilt.")

    def test_custom_feed_etag_stable_across_shell_cache_rebuild(self):
        user = User.objects.create_user(username='listener')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)

        def get_etag():
            req = self.factory.get('/feed/', {
                'auth': str(profile.feed_token),
                'show': 'show',
            })
            req.network = self.network
            return views.generate_custom_feed(req).get('ETag')

        first = get_etag()
        cache.delete(f"feed_shell_public_{self.podcast.id}")
        cache.delete(f"feed_shell_private_{self.podcast.id}")
        second = get_etag()
        self.assertEqual(first, second)

    def test_public_feed_returns_304_on_etag_match(self):
        req = _make_tenant_request(self.factory, self.network, path='/feed/')
        resp = views.generate_public_feed(req, podcast_slug='show')
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        req2 = _make_tenant_request(self.factory, self.network, path='/feed/')
        req2.META['HTTP_IF_NONE_MATCH'] = etag
        resp2 = views.generate_public_feed(req2, podcast_slug='show')
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)

    def test_custom_feed_returns_304_on_etag_match(self):
        user = User.objects.create_user(username='listener')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)

        def make_req(etag=None):
            req = self.factory.get('/feed/', {
                'auth': str(profile.feed_token),
                'show': 'show',
            })
            req.network = self.network
            if etag:
                req.META['HTTP_IF_NONE_MATCH'] = etag
            return req

        resp = views.generate_custom_feed(make_req())
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        resp2 = views.generate_custom_feed(make_req(etag))
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)

    # ── UserMix feed ──────────────────────────────────────────────────────

    def _make_user_mix(self):
        user = User.objects.create_user(username='mixowner')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)
        mix = UserMix.objects.create(
            user=user, network=self.network, name='My Mix', is_active=True
        )
        mix.selected_podcasts.add(self.podcast)
        return mix, profile

    def _mix_req(self, mix, profile, etag=None):
        req = self.factory.get(
            f'/feed/mix/{mix.unique_id}',
            {'auth': str(profile.feed_token)},
        )
        if etag:
            req.META['HTTP_IF_NONE_MATCH'] = etag
        return req

    def test_user_mix_feed_etag_stable_across_shell_cache_rebuild(self):
        mix, profile = self._make_user_mix()
        first = views.generate_mix_feed(self._mix_req(mix, profile), unique_id=mix.unique_id).get('ETag')
        cache.delete(f"shell_user_mix_{mix.id}")
        second = views.generate_mix_feed(self._mix_req(mix, profile), unique_id=mix.unique_id).get('ETag')
        self.assertIsNotNone(first)
        self.assertEqual(first, second, "UserMix ETag must not change when the shell cache is rebuilt.")

    def test_user_mix_feed_returns_304_on_etag_match(self):
        mix, profile = self._make_user_mix()
        resp = views.generate_mix_feed(self._mix_req(mix, profile), unique_id=mix.unique_id)
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        resp2 = views.generate_mix_feed(self._mix_req(mix, profile, etag=etag), unique_id=mix.unique_id)
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)

    # ── NetworkMix feed ───────────────────────────────────────────────────

    def _make_network_mix(self):
        mix = NetworkMix.objects.create(
            network=self.network, name='Net Mix', slug='netmix', required_tier=None
        )
        mix.selected_podcasts.add(self.podcast)
        return mix

    def _net_mix_req(self, etag=None):
        req = _make_tenant_request(self.factory, self.network, path='/feed/n/mix/netmix/')
        if etag:
            req.META['HTTP_IF_NONE_MATCH'] = etag
        return req

    def test_network_mix_feed_etag_stable_across_shell_cache_rebuild(self):
        mix = self._make_network_mix()
        first = views.generate_network_mix_feed(self._net_mix_req(), network_slug='n', mix_slug='netmix').get('ETag')
        cache.delete(f"shell_net_mix_{mix.id}")
        second = views.generate_network_mix_feed(self._net_mix_req(), network_slug='n', mix_slug='netmix').get('ETag')
        self.assertIsNotNone(first)
        self.assertEqual(first, second, "NetworkMix ETag must not change when the shell cache is rebuilt.")

    def test_network_mix_feed_returns_304_on_etag_match(self):
        self._make_network_mix()
        resp = views.generate_network_mix_feed(self._net_mix_req(), network_slug='n', mix_slug='netmix')
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        resp2 = views.generate_network_mix_feed(self._net_mix_req(etag=etag), network_slug='n', mix_slug='netmix')
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)


@override_settings(CACHES=TEST_CACHES)
class NetworkMixCrudTests(TestCase):
    """The creator_settings dispatcher gained add/edit/delete handlers for
    NetworkMix; verify each branch persists the right state."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner', email='owner@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)
        self.p1 = Podcast.objects.create(network=self.network, title='P1', slug='p1')
        self.p2 = Podcast.objects.create(network=self.network, title='P2', slug='p2')

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_add_network_mix_creates_row(self):
        self._post({
            'action': 'add_network_mix',
            'network_id': self.network.id,
            'name': 'My Net Mix',
            'slug': 'my-net-mix',
            'mix_image': '',
            'tier_id': '',
            'podcasts': [str(self.p1.id), str(self.p2.id)],
        })
        mix = NetworkMix.objects.get(network=self.network, slug='my-net-mix')
        self.assertEqual(mix.name, 'My Net Mix')
        self.assertEqual(set(mix.selected_podcasts.values_list('id', flat=True)),
                         {self.p1.id, self.p2.id})

    def test_edit_network_mix_persists_name_and_podcast_set(self):
        mix = NetworkMix.objects.create(network=self.network, name='Old', slug='old')
        mix.selected_podcasts.set([self.p1])

        self._post({
            'action': 'edit_network_mix',
            'mix_id': mix.id,
            'name': 'New Name',
            'slug': 'old',
            'mix_image': '',
            'tier_id': '',
            'podcasts': [str(self.p2.id)],   # swap p1 -> p2
        })
        mix.refresh_from_db()
        self.assertEqual(mix.name, 'New Name')
        self.assertEqual(list(mix.selected_podcasts.values_list('id', flat=True)),
                         [self.p2.id])

    def test_delete_network_mix_removes_row(self):
        mix = NetworkMix.objects.create(network=self.network, name='X', slug='x')
        self._post({'action': 'delete_network_mix', 'mix_id': mix.id})
        self.assertFalse(NetworkMix.objects.filter(id=mix.id).exists())


class EpisodeChapterMirrorTests(TestCase):
    """submit_episode_edit must mirror chapter writes to both the public
    and private columns so the private feed is never left null after an
    edit."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='trusted', email='trusted@example.com')
        self.network = Network.objects.create(name='Net', slug='n', auto_approve_trust_threshold=0)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast,
            title='Ep',
            pub_date=timezone.now(),
            raw_description='hi',
            clean_description='hi',
            audio_url_public='https://x/audio.mp3',
        )
        # Trust score >= threshold (0) → auto-approval path
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=10)

    def test_chapters_mirror_to_both_columns(self):
        import json
        payload = json.dumps({
            'title': 'Ep',
            'description': '<p>still hi</p>',
            'tags': [],
            'chapters': [{'startTime': 0.0, 'title': 'Intro'}],
        })
        req = self.factory.post(
            reverse('submit_episode_edit', args=[self.episode.id]),
            data={'payload': payload},
        )
        req.user = self.user
        req.network = self.network
        from django.contrib.messages.storage.fallback import FallbackStorage
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        setattr(req, '_messages', FallbackStorage(req))

        views.submit_episode_edit(req, self.episode.id)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.chapters_public, self.episode.chapters_private,
                         "Chapters must be written to both columns.")
        # Sanity: chapters actually contain our entry.
        chap_list = (self.episode.chapters_public or {}).get('chapters', [])
        self.assertEqual(len(chap_list), 1)
        self.assertEqual(chap_list[0]['title'], 'Intro')


@override_settings(WHISPER_ENABLED=False)
class EpisodeDetailTemplateTests(TestCase):
    """The 'Show Ad-Supported Version' button must NOT render when there's
    no public audio URL — otherwise it loads an empty <audio> element."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='listener', email='listener@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        # Membership grants tier-free access.
        NetworkMembership.objects.create(user=self.user, network=self.network)

    def _render(self, public_url):
        ep = Episode.objects.create(
            podcast=self.podcast,
            title='Ep',
            pub_date=timezone.now(),
            raw_description='hi',
            clean_description='hi',
            audio_url_public=public_url,
            audio_url_subscriber='https://cdn.example.com/private.mp3',
        )
        # Bypass NetworkMiddleware by calling the view directly with a
        # request that already has request.network attached. Avoids the
        # ALLOWED_HOSTS dance that the test client requires.
        req = _make_tenant_request(RequestFactory(), self.network,
                                   path=f'/episode/{ep.id}/', user=self.user)
        resp = views.episode_detail(req, ep.id)
        return resp.content.decode('utf-8')

    def test_button_hidden_when_no_public_audio(self):
        body = self._render(public_url='')
        self.assertNotIn('Show Ad-Supported Version', body)

    def test_button_shown_when_public_audio_present(self):
        body = self._render(public_url='https://cdn.example.com/public.mp3')
        self.assertIn('Show Ad-Supported Version', body)


class AnalyticsSweepTests(TestCase):
    """Drives sweep_analytics_buffer end-to-end against an in-memory
    fakeredis. Verifies that:
      - billing:active and analytics:rss keys both update last_active_date
      - the latest date wins when multiple keys exist for one membership
      - keys are deleted after processing
      - the function still runs the presence sweep when there are no
        analytics:play keys (the bug we fixed)
    """

    def setUp(self):
        from django.conf import settings as django_settings
        self.user = User.objects.create_user(username='listener', email='listener@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.membership = NetworkMembership.objects.create(user=self.user, network=self.network)

        import os
        if django_settings.IS_IDE:
            import fakeredis
            self.fake = fakeredis.FakeRedis()
            redis_location = 'redis://fake:6379/0'
        else:
            import redis as redis_lib
            redis_location = os.getenv('REDIS_URL', 'redis://redis:6379/0')
            self.fake = redis_lib.from_url(redis_location)
            self._clean_test_keys()

        # override_settings(CACHES=...) is required in both branches: the task reads
        # settings.CACHES['default']['BACKEND'] directly and bails early on locmem.
        self._patches = [
            mock.patch('pod_manager.tasks.redis.from_url', return_value=self.fake),
            override_settings(CACHES={
                'default': {
                    'BACKEND': 'django.core.cache.backends.redis.RedisCache',
                    'LOCATION': redis_location,
                }
            }),
        ]

        for p in self._patches:
            p.enable() if hasattr(p, 'enable') else p.__enter__()

    def tearDown(self):
        for p in reversed(self._patches):
            p.disable() if hasattr(p, 'disable') else p.__exit__(None, None, None)
        from django.conf import settings as django_settings
        if not django_settings.IS_IDE:
            self._clean_test_keys()

    def _clean_test_keys(self):
        for pattern in [b'billing:active:*', b'analytics:rss:*']:
            keys = self.fake.keys(pattern)
            if keys:
                self.fake.delete(*keys)

    def test_billing_active_updates_last_active_date(self):
        from pod_manager.tasks import sweep_analytics_buffer
        self.fake.set(f"billing:active:{self.network.id}:{self.user.id}:2026-05-02", "1")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-05-02')
        # And the key was drained.
        self.assertEqual(self.fake.keys('billing:active:*'), [])

    def test_analytics_rss_updates_last_active_date(self):
        """RSS-only listener (no session, no billing middleware) still counts
        as active because the feed views write analytics:rss directly."""
        from pod_manager.tasks import sweep_analytics_buffer
        self.fake.set(f"analytics:rss:{self.network.id}:{self.user.id}:2026-04-30", "3")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-04-30')
        self.assertEqual(self.fake.keys('analytics:rss:*'), [])

    def test_latest_date_wins_for_one_membership(self):
        from pod_manager.tasks import sweep_analytics_buffer
        # Two keys for the same (network, user) — older billing, newer rss.
        self.fake.set(f"billing:active:{self.network.id}:{self.user.id}:2026-04-01", "1")
        self.fake.set(f"analytics:rss:{self.network.id}:{self.user.id}:2026-05-02", "5")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-05-02')

    def test_runs_when_no_play_keys(self):
        """The early-return that used to skip billing when there were no
        analytics:play:* keys is gone. With only billing keys, the sweep
        must still update last_active_date."""
        from pod_manager.tasks import sweep_analytics_buffer
        # No analytics:play keys at all.
        self.fake.set(f"billing:active:{self.network.id}:{self.user.id}:2026-05-02", "1")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-05-02')


class PatreonStateForgeryTests(TestCase):
    """patreon_callback must NOT call _link_creator_campaign when the state
    parameter isn't a valid signed token, even when the rest of the OAuth
    round-trip succeeds. The negative manual test (raw network_id as state)
    couldn't actually prove this because Patreon rejects the fake code first.
    Mock the token exchange so we reach the state validation."""

    def setUp(self):
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Victim', slug='victim')

    def _callback(self, state):
        req = self.factory.get(
            reverse('patreon_callback'),
            {'code': 'fake-code', 'state': state},
        )
        req.network = self.network
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        from django.contrib.messages.storage.fallback import FallbackStorage
        setattr(req, '_messages', FallbackStorage(req))
        from django.contrib.auth.models import AnonymousUser
        req.user = AnonymousUser()
        return req

    @mock.patch('pod_manager.views._link_creator_campaign')
    @mock.patch('pod_manager.views._fetch_patreon_identity')
    @mock.patch('pod_manager.views._exchange_patreon_token')
    def test_raw_network_id_state_does_not_link_campaign(self, mock_exchange,
                                                         mock_identity, mock_link):
        # Token exchange succeeds.
        mock_exchange.return_value = ({'access_token': 'a', 'refresh_token': 'r'}, None)
        # Identity returns the minimum needed to fall through to listener path.
        mock_identity.return_value = ({
            'data': {'id': '1', 'attributes': {'email': 'x@example.com',
                                                'first_name': 'X',
                                                'last_name': 'Y'}},
            'included': [],
        }, None)
        # Forged state: just the raw network id.
        req = self._callback(state=str(self.network.id))
        views.patreon_callback(req)
        # Critical assertion: link path was NOT taken.
        mock_link.assert_not_called()

    @mock.patch('pod_manager.views._link_creator_campaign')
    @mock.patch('pod_manager.views._exchange_patreon_token')
    def test_signed_state_with_wrong_user_id_is_rejected(self, mock_exchange, mock_link):
        # Even a properly-signed state must include the linking user's id;
        # if request.user.id doesn't match, _link_creator_campaign should
        # not run.
        mock_exchange.return_value = ({'access_token': 'a', 'refresh_token': 'r'}, None)
        # Sign state for user 999, but the callback runs anonymously (user.id=None).
        forged = views._sign_oauth_state(f"link:999:{self.network.id}")
        req = self._callback(state=forged)
        views.patreon_callback(req)
        mock_link.assert_not_called()


# ---------------------------------------------------------------------------
# Creator settings: inbox action handlers (approve / reject)
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class CreatorInboxActionTests(TestCase):
    """_handle_inbox_action: approve_edit and reject_edit branches."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.submitter = User.objects.create_user(username='submitter')
        # High threshold so submitter never auto-approves
        self.network = Network.objects.create(name='Net', slug='n', auto_approve_trust_threshold=999)
        self.network.owners.add(self.owner)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original Title', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/a.mp3',
            tags=['tag1'], chapters_public=[],
        )
        self.membership = NetworkMembership.objects.create(
            user=self.submitter, network=self.network, trust_score=5,
        )

    def _make_pending_edit(self):
        return EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter, status=EpisodeEditSuggestion.Status.PENDING,
            original_data={
                'title': self.episode.title,
                'description': self.episode.clean_description,
                'tags': list(self.episode.tags or []),
                'chapters': self.episode.chapters_public or [],
            },
            suggested_data={
                'title': 'New Title',
                'description': '<p>new desc</p>',
                'tags': ['tag2'],
                'chapters': [],
            },
        )

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_approve_title_grants_trust_locks_episode(self):
        edit = self._make_pending_edit()
        self._post({
            'action': 'approve_edit',
            'edit_id': edit.id,
            'approve_title': 'on',
            'edited_title': 'New Title',
        })
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'New Title')
        self.assertTrue(self.episode.is_metadata_locked)
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'approved')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 6)  # +1

    def test_approve_zero_fields_converts_to_rejection_and_penalizes(self):
        edit = self._make_pending_edit()
        # No approve_* checkboxes → zero points
        self._post({'action': 'approve_edit', 'edit_id': edit.id})
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'rejected')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 3)  # -2

    def test_approve_three_fields_applies_perfect_sweep_bonus(self):
        import json as _json
        edit = self._make_pending_edit()
        self._post({
            'action': 'approve_edit',
            'edit_id': edit.id,
            'approve_title': 'on',
            'edited_title': 'New Title',
            'approve_description': 'on',
            'edited_description': '<p>new desc</p>',
            'approve_tags': 'on',
            'edited_tags': _json.dumps(['tag2']),
        })
        self.membership.refresh_from_db()
        # 3 fields approved → 3 points + 2 perfect-sweep bonus = 5
        self.assertEqual(self.membership.trust_score, 10)

    def test_reject_edit_penalizes_trust_and_marks_rejected(self):
        edit = self._make_pending_edit()
        self._post({'action': 'reject_edit', 'edit_id': edit.id})
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'rejected')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 3)  # -2


# ---------------------------------------------------------------------------
# Creator settings: rollback handlers
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class CreatorRollbackTests(TestCase):
    """rollback_single_edit and bulk_rollback branches of _handle_rollback."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.spammer = User.objects.create_user(username='spammer')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Vandalized', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/a.mp3',
        )
        self.membership = NetworkMembership.objects.create(
            user=self.spammer, network=self.network, trust_score=20, edits_title=1,
        )

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def _approved_edit(self, episode, original_title, resolved_at=None):
        return EpisodeEditSuggestion.objects.create(
            episode=episode, user=self.spammer, status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': original_title, 'description': 'x', 'tags': [], 'chapters': []},
            suggested_data={'title': 'Vandalized', 'description': 'x', 'tags': [], 'chapters': []},
            resolved_at=resolved_at or timezone.now(),
        )

    def test_rollback_blocked_when_newer_approved_edit_exists(self):
        base = timezone.now()
        older = self._approved_edit(self.episode, 'Original', resolved_at=base)
        self._approved_edit(self.episode, 'Middle',
                            resolved_at=base + timedelta(seconds=30))

        self._post({'action': 'rollback_single_edit', 'edit_id': older.id})

        older.refresh_from_db()
        self.assertEqual(older.status, 'approved')  # Must not change

    def test_rollback_single_restores_episode_and_penalizes_trust(self):
        edit = self._approved_edit(self.episode, 'Original Title')

        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})

        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Original Title')
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'rolled_back')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 15)  # -5

    def test_bulk_rollback_reverts_all_edits_and_zeros_stats(self):
        for i in range(3):
            ep = Episode.objects.create(
                podcast=self.podcast, title=f'Ep{i}', pub_date=timezone.now(),
                raw_description='x', clean_description='x',
                audio_url_public='https://cdn.example.com/a.mp3',
            )
            self._approved_edit(ep, f'Clean {i}')

        self._post({'action': 'bulk_rollback', 'spammer_id': self.spammer.id})

        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 0)
        self.assertEqual(self.membership.edits_chapters, 0)
        reverted = EpisodeEditSuggestion.objects.filter(
            user=self.spammer, status=EpisodeEditSuggestion.Status.ROLLED_BACK
        ).count()
        self.assertEqual(reverted, 3)


# ---------------------------------------------------------------------------
# Creator settings: network and show management
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class CreatorNetworkAndShowTests(TestCase):
    """update_network, add_show, update_show action handlers."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_update_network_invalid_json_does_not_save(self):
        self.network.website_url = 'https://before.example.com'
        self.network.save()
        self._post({
            'action': 'update_network',
            'theme_config': '{invalid json}',
            'website_url': 'https://after.example.com',
            'patreon_campaign_id': '', 'default_image_url': '',
            'ignored_title_tags': '', 'description_cut_triggers': '',
            'footer_public': '', 'footer_private': '',
        })
        self.network.refresh_from_db()
        self.assertEqual(self.network.website_url, 'https://before.example.com')

    def test_update_network_valid_persists_fields(self):
        import json as _json
        self._post({
            'action': 'update_network',
            'theme_config': _json.dumps({'primary_color': '#abc'}),
            'website_url': 'https://example.com',
            'patreon_campaign_id': 'camp999',
            'default_image_url': '', 'ignored_title_tags': '',
            'description_cut_triggers': '',
            'footer_public': 'Pub footer', 'footer_private': 'Priv footer',
        })
        self.network.refresh_from_db()
        self.assertEqual(self.network.website_url, 'https://example.com')
        self.assertEqual(self.network.patreon_campaign_id, 'camp999')
        self.assertEqual(self.network.global_footer_public, 'Pub footer')
        self.assertEqual(self.network.theme_config, {'primary_color': '#abc'})

    def test_add_show_creates_podcast_and_redirects_to_auto_import(self):
        resp = self._post({
            'action': 'add_show',
            'title': 'Brand New Show', 'slug': 'brand-new',
            'tier_id': '',
            'public_feed_url': 'https://feeds.example.com/show.rss',
            'subscriber_feed_url': '',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIn('auto_import=', resp['Location'])
        self.assertTrue(Podcast.objects.filter(network=self.network, slug='brand-new').exists())

    def test_update_show_persists_feed_url(self):
        show = Podcast.objects.create(
            network=self.network, title='S', slug='s',
            public_feed_url='https://old.example.com/feed.rss',
        )
        self._post({
            'action': 'update_show',
            'show_id': show.id,
            'public_feed_url': 'https://new.example.com/feed.rss',
            'subscriber_feed_url': '',
            'tier_id': '', 'show_footer_public': '', 'show_footer_private': '',
        })
        show.refresh_from_db()
        self.assertEqual(show.public_feed_url, 'https://new.example.com/feed.rss')


# ---------------------------------------------------------------------------
# Creator settings: episode merge, split, move
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES, WHISPER_ENABLED=False)
class CreatorMergeAndMoveTests(TestCase):
    """merge_episodes, split_episode, move_episodes handlers."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)
        self.podcast = Podcast.objects.create(network=self.network, title='P1', slug='p1')

    def _ep(self, **kwargs):
        defaults = dict(
            podcast=self.podcast, pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
        )
        defaults.update(kwargs)
        return Episode.objects.create(**defaults)

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_merge_transfers_private_data_and_deletes_orphan(self):
        pub_ep = self._ep(title='Public', guid_public='pub-guid')
        priv_ep = self._ep(
            title='Private', guid_private='priv-guid',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )
        self._post({
            'action': 'merge_episodes',
            'public_episode_id': pub_ep.id,
            'private_episode_id': priv_ep.id,
        })
        pub_ep.refresh_from_db()
        self.assertEqual(pub_ep.guid_private, 'priv-guid')
        self.assertEqual(pub_ep.audio_url_subscriber, 'https://cdn.example.com/priv.mp3')
        self.assertFalse(Episode.objects.filter(id=priv_ep.id).exists())

    def test_split_creates_new_episode_and_clears_private_data(self):
        ep = self._ep(
            title='Paired', guid_private='priv-guid',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )
        self._post({'action': 'split_episode', 'episode_id': ep.id})

        ep.refresh_from_db()
        self.assertIsNone(ep.guid_private)
        self.assertEqual(ep.audio_url_subscriber, '')
        self.assertEqual(ep.match_reason, 'Manually Unpaired')
        # A new episode with the same title must exist
        self.assertEqual(Episode.objects.filter(podcast=self.podcast, title='Paired').count(), 2)

    def test_move_episodes_to_existing_podcast(self):
        target = Podcast.objects.create(network=self.network, title='Target', slug='target')
        ep = self._ep(title='Traveller')
        self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': target.id,
            'new_podcast_title': '', 'new_podcast_slug': '', 'new_podcast_tier_id': '',
        })
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, target.id)

    def test_move_episodes_creates_new_podcast(self):
        ep = self._ep(title='Traveller')
        self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': '',
            'new_podcast_title': 'Brand New', 'new_podcast_slug': 'brand-new',
            'new_podcast_tier_id': '',
        })
        ep.refresh_from_db()
        new_pod = Podcast.objects.get(network=self.network, slug='brand-new')
        self.assertEqual(ep.podcast_id, new_pod.id)

    def test_move_episodes_missing_slug_redirects_with_error(self):
        ep = self._ep(title='Traveller')
        resp = self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': '',
            'new_podcast_title': 'Show Without Slug', 'new_podcast_slug': '',
            'new_podcast_tier_id': '',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIn('tab=move', resp['Location'])
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, self.podcast.id)  # Unchanged

    def test_move_episodes_duplicate_slug_redirects_with_error(self):
        Podcast.objects.create(network=self.network, title='Existing', slug='exists')
        ep = self._ep(title='Traveller')
        resp = self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': '',
            'new_podcast_title': 'New Show', 'new_podcast_slug': 'exists',
            'new_podcast_tier_id': '',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIn('tab=move', resp['Location'])
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, self.podcast.id)  # Unchanged


# ---------------------------------------------------------------------------
# gather_inbox conflict flags (tested directly, not through the full view)
# ---------------------------------------------------------------------------

class GatherInboxConflictFlagTests(TestCase):
    """gather_inbox annotates each pending edit with conflict flags when the
    live episode has changed since the user submitted their snapshot."""

    def setUp(self):
        from pod_manager.views.creator.data import gather_inbox as _gather_inbox
        self._gather_inbox = _gather_inbox
        self.submitter = User.objects.create_user(username='submitter')
        self.network = Network.objects.create(name='Net', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Current Title', pub_date=timezone.now(),
            raw_description='x', clean_description='<p>current</p>',
            audio_url_public='https://cdn.example.com/a.mp3',
            tags=['tag-live'], chapters_public=[],
        )

    def _make_pending_edit(self, original_data):
        return EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter, status=EpisodeEditSuggestion.Status.PENDING,
            original_data=original_data,
            suggested_data={'title': 'X', 'description': 'X', 'tags': [], 'chapters': []},
        )

    def _pending_list(self):
        return list(self._gather_inbox(self.network)['pending_edits'])

    def test_no_conflict_when_episode_matches_snapshot(self):
        self._make_pending_edit({
            'title': 'Current Title', 'description': '<p>current</p>',
            'tags': ['tag-live'], 'chapters': [],
        })
        edit = self._pending_list()[0]
        self.assertFalse(edit.title_conflict)
        self.assertFalse(edit.desc_conflict)
        self.assertFalse(edit.tags_conflict)
        self.assertFalse(edit.chapters_conflict)

    def test_title_conflict_when_episode_changed_since_submission(self):
        self._make_pending_edit({
            'title': 'Old Title',  # episode is now 'Current Title'
            'description': '<p>current</p>', 'tags': ['tag-live'], 'chapters': [],
        })
        edit = self._pending_list()[0]
        self.assertTrue(edit.title_conflict)
        self.assertFalse(edit.desc_conflict)

    def test_tags_conflict_when_live_tags_differ_from_snapshot(self):
        self._make_pending_edit({
            'title': 'Current Title', 'description': '<p>current</p>',
            'tags': ['tag-old'],  # episode has ['tag-live']
            'chapters': [],
        })
        edit = self._pending_list()[0]
        self.assertTrue(edit.tags_conflict)
        self.assertFalse(edit.title_conflict)


# ---------------------------------------------------------------------------
# submit_episode_edit: untrusted (pending) path
# ---------------------------------------------------------------------------

class SubmitEpisodeEditPendingPathTests(TestCase):
    """Untrusted users (trust_score < threshold) get 'pending' status.
    The episode must NOT be mutated immediately."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='untrusted')
        # threshold=100, user has score=0 → always pending
        self.network = Network.objects.create(name='Net', slug='n', auto_approve_trust_threshold=100)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/audio.mp3',
        )
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=0)

    def _submit(self, payload):
        import json as _json
        from django.contrib.messages.storage.fallback import FallbackStorage
        from django.contrib.sessions.backends.cache import SessionStore
        req = self.factory.post(
            reverse('submit_episode_edit', args=[self.episode.id]),
            data={'payload': _json.dumps(payload)},
        )
        req.user = self.user
        req.network = self.network
        req.session = SessionStore()
        req.session.create()
        setattr(req, '_messages', FallbackStorage(req))
        return views.submit_episode_edit(req, self.episode.id)

    def test_untrusted_edit_is_pending_and_episode_unchanged(self):
        self._submit({
            'title': 'Vandalized', 'description': '<p>bad</p>',
            'tags': [], 'chapters': [],
        })
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertEqual(suggestion.status, 'pending')
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Original')  # NOT mutated

    def test_chapter_sanitization_skips_invalid_start_time(self):
        """Chapters with non-numeric startTime must be silently dropped."""
        self._submit({
            'title': 'Ep', 'description': '<p>x</p>', 'tags': [],
            'chapters': [
                {'startTime': 0.0, 'title': 'Valid'},
                {'startTime': 'not-a-number', 'title': 'Bad'},
            ],
        })
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        chaps = suggestion.suggested_data.get('chapters', {}).get('chapters', [])
        self.assertEqual(len(chaps), 1)
        self.assertEqual(chaps[0]['title'], 'Valid')


# ---------------------------------------------------------------------------
# services/edits.py unit tests
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# RSSFeedBuilder._finalize_xml unit tests
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class FinalizeXmlTests(TestCase):
    """_finalize_xml handles namespace injection, lxml pollution stripping,
    category attachment, and chapter URL insertion independently of podgen."""

    PODCAST_NS = "https://podcastindex.org/namespace/1.0"

    def setUp(self):
        # _finalize_xml now queries Transcript for completed transcripts.
        # These unit tests use mock episodes (no real DB rows), so suppress
        # the query to keep tests focused on the XML manipulation behaviour.
        # Transcript is locally imported inside _finalize_xml, so patch at models level.
        patcher = mock.patch('pod_manager.models.Transcript')
        self._mock_transcript = patcher.start()
        self._mock_transcript.objects.filter.return_value = []
        self._mock_transcript.Status.COMPLETED = 'completed'
        self.addCleanup(patcher.stop)

    # Minimal RSS skeleton with one item whose <guid> we can control.
    RSS_TMPL = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        '<rss version="2.0"{ns_attr}>'
        "<channel><title>T</title>"
        "<item><guid>{guid}</guid><title>Ep</title></item>"
        "</channel></rss>"
    )

    def _builder(self):
        from pod_manager.views.feeds import RSSFeedBuilder
        from unittest.mock import MagicMock
        net = MagicMock()
        net.name = "TestNet"
        net.summary = ""
        net.website_url = "https://example.com"
        net.default_image_url = ""
        net.contact_email = "test@example.com"
        return RSSFeedBuilder("https://example.com", "T", "D", "", net, feed_type='public')

    def _rss(self, guid="ep-1", with_ns=False):
        ns_attr = f' xmlns:podcast="{self.PODCAST_NS}"' if with_ns else ""
        return self.RSS_TMPL.format(guid=guid, ns_attr=ns_attr)

    def test_namespace_injected_when_absent(self):
        builder = self._builder()
        result = builder._finalize_xml(self._rss(), {}, None)
        self.assertIn(f'xmlns:podcast="{self.PODCAST_NS}"', result)

    def test_namespace_not_duplicated_when_already_present(self):
        builder = self._builder()
        result = builder._finalize_xml(self._rss(with_ns=True), {}, None)
        self.assertEqual(result.count(f'xmlns:podcast="{self.PODCAST_NS}"'), 1)

    def test_empty_tag_map_returns_without_lxml_parse(self):
        builder = self._builder()
        # Deliberately malformed inner XML that lxml would choke on —
        # empty tag_map must short-circuit before any parsing.
        raw = self._rss()
        result = builder._finalize_xml(raw, {}, None)
        self.assertIn('<rss', result)

    def test_inline_namespace_pollution_stripped(self):
        # Simulate what lxml produces when a child element uses the namespace:
        # it re-declares xmlns:podcast on the child. We must strip those.
        # A non-empty tag_map is required so that the lxml roundtrip runs.
        from unittest.mock import MagicMock
        raw = (
            "<?xml version='1.0' encoding='UTF-8'?>"
            f'<rss version="2.0" xmlns:podcast="{self.PODCAST_NS}">'
            "<channel><title>T</title>"
            f'<item xmlns:podcast="{self.PODCAST_NS}"><guid>ep-1</guid></item>'
            "</channel></rss>"
        )
        ep = MagicMock()
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'
        builder = self._builder()
        result = builder._finalize_xml(raw, {'ep-1': ep}, None)
        # The declaration should appear exactly once, on the root <rss> tag.
        self.assertEqual(result.count(f'xmlns:podcast="{self.PODCAST_NS}"'), 1)

    def test_category_tags_added_for_episode(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = ['comedy', 'tech']
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertEqual(result.count('<category>'), 2)
        self.assertIn('comedy', result)
        self.assertIn('tech', result)

    def test_no_category_tags_when_episode_has_none(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertNotIn('<category>', result)

    def test_chapter_url_added_when_chapters_exist(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.id = 42
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = {'version': '1.2.0', 'chapters': []}
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertIn('podcast:chapters', result)
        self.assertIn('/42/', result)
        self.assertIn('application/json+chapters', result)

    def test_no_chapter_elem_when_no_chapters(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertNotIn('podcast:chapters', result)

    def test_access_map_controls_chapter_feed_type(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.id = 99
        ep.tags = []
        ep.podcast_id = 7
        ep.chapters_public = [{'startTime': 0, 'title': 'A'}]
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(
            self._rss(guid='ep-1'), {'ep-1': ep},
            access_map={7: True},
        )
        self.assertIn('/chapters/private.json', result)

    def test_unknown_guid_item_left_untouched(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = ['x']
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        # tag_map has 'other-guid', XML has 'ep-1' — no match, no mutation
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'other-guid': ep}, None)
        self.assertNotIn('<category>', result)


class ParseChapterPayloadTests(TestCase):
    """parse_chapter_payload normalises list and dict inputs."""

    def test_empty_list_returns_empty_chapters(self):
        result = parse_chapter_payload([])
        self.assertEqual(result, {'version': '1.2.0', 'chapters': []})

    def test_empty_dict_returns_empty_chapters(self):
        result = parse_chapter_payload({'chapters': [], 'version': '1.2.0'})
        self.assertEqual(result, {'version': '1.2.0', 'chapters': []})

    def test_valid_list_chapter_parsed(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'Intro'}])
        self.assertEqual(result['chapters'][0], {'startTime': 0, 'title': 'Intro'})

    def test_float_start_time_preserved_when_not_integer(self):
        result = parse_chapter_payload([{'startTime': 1.5, 'title': 'Mid'}])
        self.assertEqual(result['chapters'][0]['startTime'], 1.5)

    def test_integer_start_time_stored_as_int(self):
        result = parse_chapter_payload([{'startTime': 60.0, 'title': 'A'}])
        self.assertIsInstance(result['chapters'][0]['startTime'], int)

    def test_invalid_start_time_chapter_dropped(self):
        result = parse_chapter_payload([
            {'startTime': 0.0, 'title': 'Good'},
            {'startTime': 'bad', 'title': 'Drop me'},
        ])
        self.assertEqual(len(result['chapters']), 1)
        self.assertEqual(result['chapters'][0]['title'], 'Good')

    def test_chapter_missing_title_dropped(self):
        result = parse_chapter_payload([{'startTime': 0.0}])
        self.assertEqual(result['chapters'], [])

    def test_chapter_missing_start_time_dropped(self):
        result = parse_chapter_payload([{'title': 'No time'}])
        self.assertEqual(result['chapters'], [])

    def test_url_accepted_when_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'url': 'https://example.com'}])
        self.assertEqual(result['chapters'][0]['url'], 'https://example.com')

    def test_url_rejected_when_non_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'url': 'javascript:alert(1)'}])
        self.assertNotIn('url', result['chapters'][0])

    def test_img_accepted_when_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'img': 'https://cdn.example.com/art.jpg'}])
        self.assertEqual(result['chapters'][0]['img'], 'https://cdn.example.com/art.jpg')

    def test_img_rejected_when_non_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'img': 'data:image/png;base64,abc'}])
        self.assertNotIn('img', result['chapters'][0])

    def test_toc_false_preserved(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'toc': False}])
        self.assertFalse(result['chapters'][0]['toc'])

    def test_toc_true_not_included(self):
        # Only toc=False is meaningful per the spec; toc=True is the default.
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'toc': True}])
        self.assertNotIn('toc', result['chapters'][0])

    def test_location_included_when_name_and_geo_present(self):
        chap = {'startTime': 0.0, 'title': 'A', 'location': {'name': 'Berlin', 'geo': 'geo:52,13', 'osm': 'R62422'}}
        result = parse_chapter_payload([chap])
        loc = result['chapters'][0]['location']
        self.assertEqual(loc['name'], 'Berlin')
        self.assertEqual(loc['geo'], 'geo:52,13')
        self.assertEqual(loc['osm'], 'R62422')

    def test_location_omitted_when_geo_missing(self):
        chap = {'startTime': 0.0, 'title': 'A', 'location': {'name': 'Berlin'}}
        result = parse_chapter_payload([chap])
        self.assertNotIn('location', result['chapters'][0])

    def test_dict_format_waypoints_flag_propagated(self):
        payload = {'version': '1.2.0', 'chapters': [{'startTime': 0.0, 'title': 'A'}], 'waypoints': True}
        result = parse_chapter_payload(payload)
        self.assertTrue(result.get('waypoints'))

    def test_dict_format_without_waypoints_flag_omits_key(self):
        payload = {'version': '1.2.0', 'chapters': [{'startTime': 0.0, 'title': 'A'}]}
        result = parse_chapter_payload(payload)
        self.assertNotIn('waypoints', result)

    def test_end_time_included_when_valid(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'endTime': 30.0}])
        self.assertEqual(result['chapters'][0]['endTime'], 30)

    def test_end_time_omitted_when_none(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'endTime': None}])
        self.assertNotIn('endTime', result['chapters'][0])


@override_settings(CACHES=TEST_CACHES)
class ApplyApprovedEditTests(TestCase):
    """apply_approved_edit writes all fields and locks metadata."""

    def setUp(self):
        self.network = Network.objects.create(name='N', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='s')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/audio.mp3',
            tags=['old'], chapters_public=[], chapters_private=[],
        )

    def test_fields_updated_and_saved(self):
        new_chapters = {'version': '1.2.0', 'chapters': [{'startTime': 0, 'title': 'Intro'}]}
        apply_approved_edit(self.episode, {
            'title': 'New Title',
            'description': '<p>new</p>',
            'tags': ['new'],
            'chapters': new_chapters,
        })
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'New Title')
        self.assertEqual(self.episode.clean_description, '<p>new</p>')
        self.assertEqual(self.episode.tags, ['new'])
        self.assertEqual(self.episode.chapters_public, new_chapters)
        self.assertEqual(self.episode.chapters_private, new_chapters)
        self.assertTrue(self.episode.is_metadata_locked)

    def test_missing_key_leaves_field_unchanged(self):
        apply_approved_edit(self.episode, {})
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Original')


@override_settings(CACHES=TEST_CACHES)
class UpdateContributionStatsTests(TestCase):
    """update_contribution_stats increments counters and awards trust correctly."""

    def setUp(self):
        self.user = User.objects.create_user(username='contrib')
        self.network = Network.objects.create(name='N', slug='n2')
        self.membership = NetworkMembership.objects.create(
            user=self.user, network=self.network,
            trust_score=10, edits_title=0, edits_tags=0,
            edits_chapters=0, edits_descriptions=0, first_responder_count=0,
        )
        self.original = {'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []}

    def _call(self, suggested, *, is_first=False):
        update_contribution_stats(self.membership, suggested, self.original, is_first=is_first)
        self.membership.refresh_from_db()

    def test_trust_always_incremented_by_five(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.trust_score, 15)

    def test_title_change_increments_edits_title(self):
        self._call({'title': 'New', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.edits_title, 1)

    def test_no_title_change_leaves_edits_title_unchanged(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.edits_title, 0)

    def test_tag_added_counts_one(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a', 'b'], 'chapters': []})
        self.assertEqual(self.membership.edits_tags, 1)

    def test_tag_removed_counts_one(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': [], 'chapters': []})
        self.assertEqual(self.membership.edits_tags, 1)

    def test_two_tags_added_one_removed_counts_three(self):
        # original=['a'], new=['b','c'] → removed={'a'}, added={'b','c'} → delta=3
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['b', 'c'], 'chapters': []})
        self.assertEqual(self.membership.edits_tags, 3)

    def test_chapter_change_counts_chapter_items(self):
        new_chaps = {'version': '1.2.0', 'chapters': [{'startTime': 0, 'title': 'A'}, {'startTime': 60, 'title': 'B'}]}
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': new_chaps})
        self.assertEqual(self.membership.edits_chapters, 2)

    def test_description_change_increments_edits_descriptions(self):
        self._call({'title': 'Old', 'description': '<p>new</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.edits_descriptions, 1)

    def test_first_responder_flag_increments_count(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []}, is_first=True)
        self.assertEqual(self.membership.first_responder_count, 1)

    def test_not_first_responder_leaves_count_unchanged(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []}, is_first=False)
        self.assertEqual(self.membership.first_responder_count, 0)


@override_settings(CACHES=TEST_CACHES)
class SubmitEpisodeEditTrustedPathTests(TestCase):
    """Trusted users (trust_score >= threshold) get instant approval, episode
    mutation, +5 trust, and correct edit counters."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='trusted')
        self.network = Network.objects.create(name='Net', slug='nt', auto_approve_trust_threshold=10)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='st')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/audio.mp3',
            tags=['old'],
        )
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=50)

    def _submit(self, payload):
        import json as _json
        from django.contrib.messages.storage.fallback import FallbackStorage
        from django.contrib.sessions.backends.cache import SessionStore
        req = self.factory.post(
            reverse('submit_episode_edit', args=[self.episode.id]),
            data={'payload': _json.dumps(payload)},
        )
        req.user = self.user
        req.network = self.network
        req.session = SessionStore()
        req.session.create()
        setattr(req, '_messages', FallbackStorage(req))
        with mock.patch('pod_manager.views.creator.main.task_rebuild_episode_fragments') as m:
            resp = views.submit_episode_edit(req, self.episode.id)
        return resp, m

    def test_trusted_edit_approved_and_episode_mutated(self):
        resp, _ = self._submit({'title': 'Updated', 'description': '<p>new</p>', 'tags': ['new'], 'chapters': []})
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Updated')
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertEqual(suggestion.status, 'approved')

    def test_trusted_edit_awards_trust_and_increments_title_counter(self):
        self._submit({'title': 'Updated', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        mem = NetworkMembership.objects.get(user=self.user, network=self.network)
        self.assertEqual(mem.trust_score, 55)
        self.assertEqual(mem.edits_title, 1)

    def test_trusted_edit_counts_tag_delta_not_just_one(self):
        # original=['old'], new=['old','extra'] → 1 tag added
        self._submit({'title': 'Original', 'description': '<p>hi</p>', 'tags': ['old', 'extra'], 'chapters': []})
        mem = NetworkMembership.objects.get(user=self.user, network=self.network)
        self.assertEqual(mem.edits_tags, 1)

    def test_trusted_edit_triggers_fragment_rebuild(self):
        _, mock_task = self._submit({'title': 'X', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        mock_task.delay.assert_called_once_with(self.episode.id, mock.ANY)

    def test_first_responder_flag_set_on_suggestion(self):
        resp, _ = self._submit({'title': 'X', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertTrue(suggestion.is_first_responder)

    def test_second_edit_is_not_first_responder(self):
        self._submit({'title': 'First', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        self._submit({'title': 'Second', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        suggestions = list(EpisodeEditSuggestion.objects.filter(episode=self.episode).order_by('id'))
        self.assertTrue(suggestions[0].is_first_responder)
        self.assertFalse(suggestions[1].is_first_responder)


# ─────────────────────────────────────────────────────────────────────────────
# TRANSCRIPTION TESTS
# ─────────────────────────────────────────────────────────────────────────────

# Common settings override applied to all transcription tests.
# WHISPER_ENABLED=False suppresses the post_save signal during fixture creation
# so individual tests can opt into it deliberately.
TRANSCRIPTION_SETTINGS = dict(
    CACHES=TEST_CACHES,
    WHISPER_ENABLED=False,
    WHISPER_URL='http://whisper-test:9000',
    WHISPER_MODEL='medium.en',
    WHISPER_LANGUAGE='en',
    WHISPER_TIMEOUT=30,
    WHISPER_KEEP_SOURCE_AUDIO=False,
    IS_IDE=False,
    SITE_URL='http://testserver',
)

# Sample ASR response (JSON format, two speakers).
_MOCK_ASR_JSON = json.dumps({
    'language': 'en',
    'segments': [
        {'start': 0.0, 'end': 2.0, 'text': 'Hello', 'speaker': 'SPEAKER_00', 'words': []},
        {'start': 2.0, 'end': 4.0, 'text': 'World', 'speaker': 'SPEAKER_01', 'words': []},
    ],
})

_SAMPLE_SEGMENTS = [
    {'start': 0.0, 'end': 2.5, 'text': 'Hello world', 'speaker': 'SPEAKER_00'},
    {'start': 2.5, 'end': 5.0, 'text': 'Goodbye',     'speaker': None},
]

_SAMPLE_SEGMENTS_WITH_WORDS = [
    {
        'start': 0.0, 'end': 2.5, 'text': 'Hello world', 'speaker': 'SPEAKER_00',
        'words': [
            {'word': 'Hello', 'start': 0.0, 'end': 1.0, 'score': 0.9,  'speaker': 'SPEAKER_00'},
            {'word': 'world', 'start': 1.0, 'end': 2.5, 'score': 0.85, 'speaker': 'SPEAKER_00'},
        ],
    },
]


_fixture_counter = 0


def _make_fixture(*, subscriber=True):
    """Return (network, podcast, episode) with minimal required fields.
    Uses a module counter to keep slugs unique across repeated calls."""
    global _fixture_counter
    _fixture_counter += 1
    n = _fixture_counter
    net = Network.objects.create(name=f'TestNet{n}', slug=f'testnet-tx{n}')
    pod = Podcast.objects.create(network=net, title=f'Show{n}', slug=f'show-tx{n}')
    ep = Episode.objects.create(
        podcast=pod,
        title='Episode 1',
        pub_date=timezone.now(),
        raw_description='desc',
        clean_description='<p>desc</p>',
        audio_url_public='https://cdn.example.com/pub.mp3',
        audio_url_subscriber='https://cdn.example.com/sub.mp3' if subscriber else '',
    )
    return net, pod, ep


# ── 1. transcript_path() ─────────────────────────────────────────────────────

class TranscriptPathTests(TestCase):

    def test_bucket_and_filename(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            p = transcript_path(6354, 'vtt')
        self.assertEqual(p, Path('/tmp/media/transcriptions/6/6354.vtt'))

    def test_bucket_boundaries(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            self.assertEqual(transcript_path(0,    'srt').parent.name, '0')
            self.assertEqual(transcript_path(999,  'srt').parent.name, '0')
            self.assertEqual(transcript_path(1000, 'srt').parent.name, '1')
            self.assertEqual(transcript_path(5999, 'srt').parent.name, '5')

    def test_invalid_extension_raises(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            with self.assertRaises(ValueError):
                transcript_path(1, 'exe')

    def test_all_valid_extensions_accepted(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            for ext in ('vtt', 'json', 'srt', 'html', 'words'):
                self.assertIsNotNone(transcript_path(1, ext))


# ── 2. Timestamp formatters ──────────────────────────────────────────────────

class VttTimestampTests(TestCase):

    def test_zero(self):
        self.assertEqual(_vtt_timestamp(0), '00:00:00.000')

    def test_sub_second(self):
        self.assertEqual(_vtt_timestamp(0.5), '00:00:00.500')

    def test_minutes(self):
        self.assertEqual(_vtt_timestamp(90.0), '00:01:30.000')

    def test_hours(self):
        self.assertEqual(_vtt_timestamp(3661.25), '01:01:01.250')


class SrtTimestampTests(TestCase):

    def test_zero(self):
        self.assertEqual(_srt_timestamp(0), '00:00:00,000')

    def test_milliseconds(self):
        self.assertEqual(_srt_timestamp(1.5), '00:00:01,500')

    def test_hours(self):
        self.assertEqual(_srt_timestamp(3661.25), '01:01:01,250')


# ── 3. Format converters ─────────────────────────────────────────────────────

class ToVttTests(TestCase):

    def test_starts_with_webvtt(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertTrue(out.startswith('WEBVTT'))

    def test_speaker_voice_tag(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('<v SPEAKER_00>', out)

    def test_no_voice_tag_when_speaker_is_none(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertNotIn('<v None>', out)
        self.assertIn('Goodbye', out)

    def test_timestamp_format(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('00:00:00.000 --> 00:00:02.500', out)


class ToSrtTests(TestCase):

    def test_index_starts_at_one(self):
        out = _to_srt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertTrue(out.startswith('1\n'))

    def test_speaker_in_brackets(self):
        out = _to_srt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('[SPEAKER_00]:', out)

    def test_no_bracket_label_when_none(self):
        out = _to_srt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertNotIn('[None]:', out)
        self.assertIn('Goodbye', out)


class ToHtmlTests(TestCase):

    def test_article_wrapper(self):
        out = _to_html(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('<article class="transcript">', out)

    def test_data_attributes(self):
        out = _to_html(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('data-start="0.0"', out)
        self.assertIn('data-end="2.5"', out)

    def test_speaker_attribute(self):
        out = _to_html(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('data-speaker="SPEAKER_00"', out)

    def test_no_speaker_attribute_when_none(self):
        out = _to_html([{'start': 0, 'end': 1, 'text': 'Hi'}]).decode('utf-8')
        self.assertNotIn('data-speaker', out)


class ToPodcastIndexJsonTests(TestCase):

    def test_structure(self):
        doc = json.loads(_to_podcast_index_json(_SAMPLE_SEGMENTS).decode('utf-8'))
        self.assertEqual(doc['version'], '1.0.0')
        seg = doc['segments'][0]
        self.assertEqual(seg['startTime'], 0.0)
        self.assertEqual(seg['body'], 'Hello world')
        self.assertEqual(seg['speaker'], 'SPEAKER_00')

    def test_no_speaker_key_when_absent(self):
        doc = json.loads(_to_podcast_index_json([{'start': 0, 'end': 1, 'text': 'Hi'}]).decode('utf-8'))
        self.assertNotIn('speaker', doc['segments'][0])


class ToWordsJsonTests(TestCase):

    def test_metadata_embedded(self):
        meta = {'episode_id': 42, 'audio_url': 'https://ex.com/ep.mp3', 'language': 'en',
                'model': 'medium.en', 'transcribed_at': '2026-01-01T00:00:00'}
        doc = json.loads(_to_words_json(_SAMPLE_SEGMENTS_WITH_WORDS, metadata=meta).decode('utf-8'))
        self.assertEqual(doc['episode_id'], 42)
        self.assertEqual(doc['audio_url'], 'https://ex.com/ep.mp3')

    def test_word_level_data_included(self):
        meta = {'episode_id': 1}
        doc = json.loads(_to_words_json(_SAMPLE_SEGMENTS_WITH_WORDS, metadata=meta).decode('utf-8'))
        words = doc['segments'][0]['words']
        self.assertEqual(len(words), 2)
        self.assertEqual(words[0]['word'], 'Hello')

    def test_no_metadata_still_valid(self):
        doc = json.loads(_to_words_json(_SAMPLE_SEGMENTS).decode('utf-8'))
        self.assertIn('segments', doc)


class PlainTextTests(TestCase):

    def test_joins_segment_text(self):
        self.assertEqual(_plain_text(_SAMPLE_SEGMENTS), 'Hello world Goodbye')

    def test_empty_list(self):
        self.assertEqual(_plain_text([]), '')


# ── 4. Response parser ───────────────────────────────────────────────────────

class ParseWhisperResponseTests(TestCase):

    def test_json_format_parsed(self):
        payload = json.dumps({'segments': [{'start': 0, 'end': 1, 'text': 'hi'}], 'language': 'en'})
        segs, lang = _parse_whisper_response(payload, 'en')
        self.assertEqual(len(segs), 1)
        self.assertEqual(lang, 'en')

    def test_json_language_field_overrides_fallback(self):
        payload = json.dumps({'segments': [], 'language': 'fr'})
        _, lang = _parse_whisper_response(payload, 'en')
        self.assertEqual(lang, 'fr')

    def test_srt_fallback(self):
        srt = "1\n00:00:00,000 --> 00:00:02,000\nHello world\n\n2\n00:00:02,000 --> 00:00:04,000\nGoodbye\n"
        segs, lang = _parse_whisper_response(srt, 'en')
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0]['text'], 'Hello world')
        self.assertAlmostEqual(segs[0]['start'], 0.0)
        self.assertAlmostEqual(segs[0]['end'], 2.0)
        self.assertEqual(lang, 'en')

    def test_prefixed_json_skips_leading_line(self):
        prefix = "transcribing...\n"
        payload = prefix + json.dumps({'segments': [{'start': 0, 'end': 1, 'text': 'x'}], 'language': 'en'})
        segs, _ = _parse_whisper_response(payload, 'en')
        self.assertEqual(len(segs), 1)

    def test_completely_unparseable_raises_value_error(self):
        with self.assertRaises(ValueError):
            _parse_whisper_response('not json not srt at all', 'en')


class ParseSrtTests(TestCase):

    def test_basic_block(self):
        srt = "1\n00:00:01,000 --> 00:00:02,500\nLine one\n\n"
        segs = _parse_srt(srt)
        self.assertEqual(len(segs), 1)
        self.assertAlmostEqual(segs[0]['start'], 1.0)
        self.assertAlmostEqual(segs[0]['end'], 2.5)
        self.assertEqual(segs[0]['text'], 'Line one')

    def test_srt_timestamp_parsing(self):
        self.assertAlmostEqual(_parse_srt_timestamp('01:02:03,456'), 3723.456)

    def test_empty_input_returns_empty(self):
        self.assertEqual(_parse_srt(''), [])


# ── 5. Transcript model ──────────────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class TranscriptModelTests(TestCase):

    def setUp(self):
        _, _, self.ep = _make_fixture()
        self.transcript = Transcript.objects.create(
            episode=self.ep,
            status=Transcript.Status.COMPLETED,
            vtt_file='transcriptions/0/1.vtt',
            json_file='transcriptions/0/1.json',
            srt_file='transcriptions/0/1.srt',
            html_file='transcriptions/0/1.html',
            words_json_file='transcriptions/0/1.words',
        )

    def test_get_url_returns_url_with_episode_id(self):
        url = self.transcript.get_url('vtt')
        self.assertIsNotNone(url)
        self.assertIn(str(self.ep.id), url)

    def test_get_url_words_maps_to_words_json_file(self):
        url = self.transcript.get_url('words')
        self.assertIsNotNone(url)

    def test_get_url_returns_none_when_file_field_empty(self):
        self.transcript.vtt_file = None
        self.transcript.save()
        self.assertIsNone(self.transcript.get_url('vtt'))


# ── 6. queue_transcription_on_episode_save signal ────────────────────────────

@override_settings(**{**TRANSCRIPTION_SETTINGS, 'WHISPER_ENABLED': True})
class QueueTranscriptionSignalTests(TestCase):

    def setUp(self):
        cache.clear()
        self.net = Network.objects.create(name='SigNet', slug='signet')
        self.pod = Podcast.objects.create(network=self.net, title='SigPod', slug='sigpod')

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_queues_new_episode_with_subscriber_audio(self, mock_task):
        ep = Episode.objects.create(
            podcast=self.pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep.mp3',
        )
        mock_task.delay.assert_called_once_with(ep.id)
        self.assertTrue(Transcript.objects.filter(episode=ep, status=Transcript.Status.PENDING).exists())

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_no_queue_without_subscriber_audio(self, mock_task):
        Episode.objects.create(
            podcast=self.pod, title='Ep2', pub_date=timezone.now(),
            raw_description='x', audio_url_public='https://cdn.example.com/pub.mp3',
        )
        mock_task.delay.assert_not_called()

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_no_requeue_when_already_pending(self, mock_task):
        ep = Episode.objects.create(
            podcast=self.pod, title='Ep3', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep3.mp3',
        )
        mock_task.reset_mock()
        ep.title = 'Updated'
        ep.save()
        mock_task.delay.assert_not_called()

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_requeues_after_failure(self, mock_task):
        ep = Episode.objects.create(
            podcast=self.pod, title='Ep4', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep4.mp3',
        )
        Transcript.objects.filter(episode=ep).update(status=Transcript.Status.FAILED)
        mock_task.reset_mock()
        ep.title = 'Retried'
        ep.save()
        mock_task.delay.assert_called_once_with(ep.id)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_no_queue_when_whisper_disabled(self, mock_task):
        with override_settings(WHISPER_ENABLED=False):
            Episode.objects.create(
                podcast=self.pod, title='Ep5', pub_date=timezone.now(),
                raw_description='x', audio_url_subscriber='https://cdn.example.com/ep5.mp3',
            )
        mock_task.delay.assert_not_called()


# ── 7. run_transcription() service ───────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class RunTranscriptionTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.net, self.pod, self.ep = _make_fixture()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _settings(self, **extra):
        return {**TRANSCRIPTION_SETTINGS, 'MEDIA_ROOT': self.tmp, **extra}

    def _mock_dl(self, mock_get):
        mock_get.return_value.raise_for_status = mock.MagicMock()
        mock_get.return_value.iter_content.return_value = [b'mp3data']

    def _mock_asr(self, mock_post, text=_MOCK_ASR_JSON):
        mock_post.return_value.raise_for_status = mock.MagicMock()
        mock_post.return_value.text = text

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.get')
    def test_happy_path_status_and_fields(self, mock_get, mock_post, mock_rebuild):
        self._mock_dl(mock_get)
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.COMPLETED)
        self.assertEqual(t.transcript_text, 'Hello World')
        self.assertEqual(t.whisper_model_used, 'medium.en')
        self.assertEqual(t.language, 'en')
        mock_rebuild.delay.assert_called_once()

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.get')
    def test_all_five_files_written_to_disk(self, mock_get, mock_post, mock_rebuild):
        self._mock_dl(mock_get)
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        for field in ('vtt_file', 'srt_file', 'json_file', 'html_file', 'words_json_file'):
            p = Path(self.tmp) / getattr(t, field)
            self.assertTrue(p.exists(), f"{field} not written to disk")

    def test_skips_when_whisper_disabled(self):
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=False)):
            run_transcription(self.ep.id)
        self.assertFalse(Transcript.objects.filter(episode=self.ep).exists())

    def test_skips_episode_without_subscriber_audio(self):
        _, _, ep_pub = _make_fixture(subscriber=False)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(ep_pub.id)
        self.assertFalse(Transcript.objects.filter(episode=ep_pub).exists())

    def test_missing_episode_returns_gracefully(self):
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(99999)
        self.assertFalse(Transcript.objects.filter(episode_id=99999).exists())

    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.get')
    def test_failure_marks_failed_and_increments_retry(self, mock_get, mock_post):
        self._mock_dl(mock_get)
        mock_post.side_effect = Exception('ASR down')
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            with self.assertRaises(Exception):
                run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.FAILED)
        self.assertEqual(t.retry_count, 1)
        self.assertIn('ASR down', t.error_message)

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.get')
    def test_podcast_override_wins_over_network(self, mock_get, mock_post, mock_rebuild):
        """Podcast-level whisper_model takes precedence over network default."""
        self.net.whisper_model = 'large'
        self.net.save()
        self.pod.whisper_model = 'base'
        self.pod.save()
        self._mock_dl(mock_get)
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.whisper_model_used, 'base')

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.get')
    def test_call_kwarg_wins_over_podcast_and_network(self, mock_get, mock_post, mock_rebuild):
        """Per-call model kwarg overrides all other levels."""
        self.pod.whisper_model = 'base'
        self.pod.save()
        self._mock_dl(mock_get)
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id, model='small')
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.whisper_model_used, 'small')

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.get')
    def test_global_settings_used_when_no_overrides(self, mock_get, mock_post, mock_rebuild):
        """Falls back to settings.WHISPER_MODEL when podcast and network have no overrides."""
        self._mock_dl(mock_get)
        self._mock_asr(mock_post)
        # Clear all model overrides so the settings fallback is actually reached.
        self.net.whisper_model = ''
        self.net.save()
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True, WHISPER_MODEL='tiny')):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.whisper_model_used, 'tiny')


# ── 8. apply_speaker_labels() ────────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class ApplySpeakerLabelsTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        _, _, self.ep = _make_fixture()
        self.transcript = Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_files(self, segments):
        """Write a .words file (plus stub files for other formats) into tmp."""
        with override_settings(MEDIA_ROOT=self.tmp):
            for ext in ('vtt', 'srt', 'json', 'html'):
                p = transcript_path(self.ep.id, ext)
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(b'stub')
            meta = {
                'episode_id': self.ep.id,
                'audio_url': 'https://x.com/a.mp3',
                'language': 'en',
                'model': 'medium.en',
                'transcribed_at': '2026-01-01T00:00:00',
            }
            words_p = transcript_path(self.ep.id, 'words')
            words_p.write_bytes(_to_words_json(segments, metadata=meta))
            rel_words = str(words_p.relative_to(self.tmp))
            rel_vtt   = str(transcript_path(self.ep.id, 'vtt').relative_to(self.tmp))
        self.transcript.words_json_file = rel_words
        self.transcript.vtt_file        = rel_vtt
        self.transcript.save()

    def test_speaker_label_applied_in_vtt(self):
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_files(segs)
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id, {'SPEAKER_00': 'Jim'})
            vtt = transcript_path(self.ep.id, 'vtt').read_bytes().decode('utf-8')
        self.assertIn('Jim', vtt)
        self.assertNotIn('SPEAKER_00', vtt)

    def test_words_json_updated_with_mapping_record(self):
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_files(segs)
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id, {'SPEAKER_00': 'Jim'})
            doc = json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))
        self.assertEqual(doc['speaker_mappings'], {'SPEAKER_00': 'Jim'})
        self.assertEqual(doc['segments'][0]['speaker'], 'Jim')

    def test_unmapped_speaker_preserved_unchanged(self):
        segs = [
            {'start': 0, 'end': 1, 'text': 'Hi',    'speaker': 'SPEAKER_00', 'words': []},
            {'start': 1, 'end': 2, 'text': 'There',  'speaker': 'SPEAKER_01', 'words': []},
        ]
        self._write_files(segs)
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id, {'SPEAKER_00': 'Jim'})
            doc = json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))
        self.assertEqual(doc['segments'][1]['speaker'], 'SPEAKER_01')

    def test_missing_words_file_returns_gracefully(self):
        # No files written — should not raise
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id, {'SPEAKER_00': 'Jim'})

    def test_non_completed_transcript_returns_gracefully(self):
        self.transcript.status = Transcript.Status.PENDING
        self.transcript.save()
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id, {'SPEAKER_00': 'Jim'})


# ── 9. serve_transcript view ─────────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class ServeTranscriptTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        # Keep MEDIA_ROOT pointing at tmp for the entire test, including when
        # the view runs. enable()/disable() span setUp → tearDown.
        self._media = override_settings(MEDIA_ROOT=self.tmp)
        self._media.enable()
        self.client = Client()
        _, _, self.ep = _make_fixture()
        p = transcript_path(self.ep.id, 'vtt')
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b'WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHello')
        self.transcript = Transcript.objects.create(
            episode=self.ep,
            status=Transcript.Status.COMPLETED,
            vtt_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.vtt',
            source_audio_url='https://cdn.example.com/my-episode-title.mp3',
        )

    def tearDown(self):
        self._media.disable()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _get(self, ext='vtt', **kwargs):
        url = reverse('serve_transcript', kwargs={'episode_id': self.ep.id, 'ext': ext})
        return self.client.get(url, **kwargs)

    def test_serves_file_with_correct_content_type(self):
        resp = self._get()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp['Content-Type'], 'text/vtt')

    def test_long_cache_and_cors_headers(self):
        resp = self._get()
        self.assertIn('max-age=31536000', resp['Cache-Control'])
        self.assertIn('immutable', resp['Cache-Control'])
        self.assertEqual(resp['Access-Control-Allow-Origin'], '*')

    def test_etag_present(self):
        resp = self._get()
        self.assertIn('ETag', resp)

    def test_304_on_etag_match(self):
        r1 = self._get()
        etag = r1['ETag']
        r2 = self._get(HTTP_IF_NONE_MATCH=etag)
        self.assertEqual(r2.status_code, 304)

    def test_404_for_nonexistent_episode(self):
        url = reverse('serve_transcript', kwargs={'episode_id': 99999, 'ext': 'vtt'})
        with override_settings(MEDIA_ROOT=self.tmp):
            resp = self.client.get(url)
        self.assertEqual(resp.status_code, 404)

    def test_content_disposition_uses_source_audio_stem(self):
        resp = self._get()
        disposition = resp.get('Content-Disposition', '')
        self.assertIn('my-episode-title', disposition)


# ── 10. backfill_transcripts_api ─────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class BackfillTranscriptsApiTests(TestCase):

    def setUp(self):
        cache.clear()
        self.staff   = User.objects.create_user(username='staff_bf',   is_staff=True)
        self.regular = User.objects.create_user(username='regular_bf', is_staff=False)
        self.net, self.pod, self.ep = _make_fixture()
        self.url = reverse('backfill_transcripts_api')

    def _post(self, user, body=None):
        self.client.force_login(user)
        return self.client.post(
            self.url,
            data=json.dumps(body or {}),
            content_type='application/json',
        )

    def test_non_staff_gets_403(self):
        resp = self._post(self.regular)
        self.assertEqual(resp.status_code, 403)

    def test_invalid_json_gets_400(self):
        self.client.force_login(self.staff)
        resp = self.client.post(self.url, data='!!notjson', content_type='application/json')
        self.assertEqual(resp.status_code, 400)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_queues_episode_with_no_transcript(self, mock_task):
        resp = self._post(self.staff, {'stagger': 0})
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.content)
        self.assertEqual(data['queued'], 1)
        mock_task.apply_async.assert_called_once()

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_skips_completed_transcript(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.COMPLETED)
        resp = self._post(self.staff)
        self.assertEqual(json.loads(resp.content)['queued'], 0)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_skips_pending_transcript(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.PENDING)
        resp = self._post(self.staff)
        self.assertEqual(json.loads(resp.content)['queued'], 0)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_requeues_failed_transcript(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.FAILED)
        resp = self._post(self.staff, {'stagger': 0})
        self.assertEqual(json.loads(resp.content)['queued'], 1)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_podcast_slug_filter(self, mock_task):
        pod2 = Podcast.objects.create(network=self.net, title='P2', slug='p2-tx')
        Episode.objects.create(
            podcast=pod2, title='Ep2', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep2.mp3',
        )
        resp = self._post(self.staff, {'podcast_slug': self.pod.slug, 'stagger': 0})
        self.assertEqual(json.loads(resp.content)['queued'], 1)

    @mock.patch('pod_manager.services.transcription.run_transcription')
    def test_ide_path_calls_run_transcription_synchronously(self, mock_run):
        with override_settings(**{**TRANSCRIPTION_SETTINGS, 'IS_IDE': True}):
            resp = self._post(self.staff, {'stagger': 0})
        mock_run.assert_called_once_with(self.ep.pk)
        self.assertEqual(json.loads(resp.content)['queued'], 1)

    def test_unknown_podcast_slug_returns_404(self):
        resp = self._post(self.staff, {'podcast_slug': 'no-such-show'})
        self.assertEqual(resp.status_code, 404)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_transcription_kwargs_forwarded(self, mock_task):
        self._post(self.staff, {'stagger': 0, 'model': 'large', 'language': 'fr', 'num_speakers': 3})
        call_kwargs = mock_task.apply_async.call_args[1].get('kwargs', {})
        self.assertEqual(call_kwargs.get('model'), 'large')
        self.assertEqual(call_kwargs.get('language'), 'fr')
        self.assertEqual(call_kwargs.get('num_speakers'), 3)


# ── 11. retranscribe_episode_api ─────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class RetranscribeEpisodeApiTests(TestCase):

    def setUp(self):
        cache.clear()
        self.net, self.pod, self.ep = _make_fixture()
        self.owner = User.objects.create_user(username='owner_rt')
        self.other = User.objects.create_user(username='other_rt')
        self.net.owners.add(self.owner)

    def _post(self, user, ep_id=None, body=None):
        self.client.force_login(user)
        url = reverse('retranscribe_episode_api', kwargs={'episode_id': ep_id or self.ep.id})
        return self.client.post(url, data=json.dumps(body or {}), content_type='application/json')

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_owner_gets_200_and_queued_status(self, mock_task):
        resp = self._post(self.owner)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)['status'], 'queued')

    def test_non_owner_gets_403(self):
        resp = self._post(self.other)
        self.assertEqual(resp.status_code, 403)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_resets_existing_transcript_to_pending(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.COMPLETED)
        self._post(self.owner)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.PENDING)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_clears_error_message(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.FAILED,
                                  error_message='old error')
        self._post(self.owner)
        t = Transcript.objects.get(episode=self.ep)
        self.assertIsNone(t.error_message)

    def test_episode_without_subscriber_audio_returns_400(self):
        net2, _, ep_pub = _make_fixture(subscriber=False)
        net2.owners.add(self.owner)
        resp = self._post(self.owner, ep_id=ep_pub.id)
        self.assertEqual(resp.status_code, 400)

    @mock.patch('pod_manager.services.transcription.run_transcription')
    def test_ide_path_calls_run_transcription_synchronously(self, mock_run):
        with override_settings(**{**TRANSCRIPTION_SETTINGS, 'IS_IDE': True}):
            resp = self._post(self.owner)
        mock_run.assert_called_once_with(self.ep.pk)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_transcription_kwargs_forwarded(self, mock_task):
        self._post(self.owner, body={'model': 'large', 'language': 'es'})
        call_kwargs = mock_task.apply_async.call_args[1].get('kwargs', {})
        self.assertEqual(call_kwargs.get('model'), 'large')
        self.assertEqual(call_kwargs.get('language'), 'es')


# ── 12. RSS feed podcast:transcript tags ─────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class FeedTranscriptTagTests(TestCase):

    def setUp(self):
        self.net, self.pod, self.ep = _make_fixture()

    def _render(self, *, completed_transcript=False, has_access=False):
        from pod_manager.views.feeds import RSSFeedBuilder
        feed_type = 'private' if has_access else 'public'
        builder = RSSFeedBuilder(
            base_url='http://testserver',
            title=self.pod.title,
            description='Test feed',
            image_url='http://testserver/img.jpg',
            network=self.net,
            feed_type=feed_type,
        )
        builder.add_episode(self.ep, has_access=has_access)
        if completed_transcript:
            Transcript.objects.create(
                episode=self.ep,
                status=Transcript.Status.COMPLETED,
                vtt_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.vtt',
                json_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.json',
                srt_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.srt',
                html_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.html',
            )
        return builder.render()

    def test_completed_transcript_adds_podcast_transcript_elements(self):
        xml = self._render(completed_transcript=True)
        self.assertIn('podcast:transcript', xml)

    def test_all_four_mime_types_present(self):
        xml = self._render(completed_transcript=True)
        self.assertIn('text/vtt', xml)
        self.assertIn('application/json', xml)
        self.assertIn('application/x-subrip', xml)
        self.assertIn('text/html', xml)

    def test_no_transcript_means_no_tags(self):
        xml = self._render(completed_transcript=False)
        self.assertNotIn('podcast:transcript', xml)

    def test_podcast_namespace_always_declared(self):
        xml = self._render(completed_transcript=True)
        self.assertIn('podcastindex.org/namespace/1.0', xml)

    def test_transcript_url_contains_episode_id_and_ext(self):
        xml = self._render(completed_transcript=True)
        self.assertIn(f'/transcripts/{self.ep.id}.vtt', xml)

    def test_pending_transcript_does_not_generate_tags(self):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.PENDING)
        xml = self._render(completed_transcript=False)
        self.assertNotIn('podcast:transcript', xml)


# ── 13. apply_approved_edit — speaker_mappings branch ────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class ApplyApprovedEditSpeakerMappingsTests(TestCase):
    """Speaker-label edits must not lock episode metadata.
    is_metadata_locked guards feed-ingested fields against being overwritten
    by the ingestor on re-import; speaker names are transcript-only and have
    no corresponding feed field, so they must not trigger the lock."""

    def setUp(self):
        _, _, self.ep = _make_fixture()
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.COMPLETED)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_calls_apply_speaker_labels(self, mock_apply):
        mappings = {'SPEAKER_00': 'Jim', 'SPEAKER_01': 'A.Ron'}
        apply_approved_edit(self.ep, {'speaker_mappings': mappings})
        mock_apply.assert_called_once_with(self.ep.id, mappings)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_speaker_only_edit_does_not_set_metadata_locked(self, _):
        """The early return in apply_approved_edit means is_metadata_locked
        is never touched for speaker-only edits."""
        self.ep.is_metadata_locked = False
        self.ep.save()
        apply_approved_edit(self.ep, {'speaker_mappings': {'SPEAKER_00': 'Jim'}})
        self.ep.refresh_from_db()
        self.assertFalse(self.ep.is_metadata_locked)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_speaker_only_edit_does_not_alter_title(self, _):
        original_title = self.ep.title
        apply_approved_edit(self.ep, {'speaker_mappings': {'SPEAKER_00': 'Jim'}})
        self.ep.refresh_from_db()
        self.assertEqual(self.ep.title, original_title)

    def test_normal_field_edit_does_set_metadata_locked(self):
        self.ep.is_metadata_locked = False
        self.ep.save()
        apply_approved_edit(self.ep, {'title': 'New Title'})
        self.ep.refresh_from_db()
        self.assertTrue(self.ep.is_metadata_locked)


# ── 14. auto_delete_transcript_files signal ──────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class AutoDeleteTranscriptFilesTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        _, _, self.ep = _make_fixture()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _create_transcript_with_file(self):
        with override_settings(MEDIA_ROOT=self.tmp):
            p = transcript_path(self.ep.id, 'vtt')
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b'WEBVTT\n')
            rel = str(p.relative_to(self.tmp))
        return Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, vtt_file=rel,
        ), p

    def test_file_deleted_on_transcript_delete(self):
        t, p = self._create_transcript_with_file()
        with override_settings(MEDIA_ROOT=self.tmp):
            t.delete()
        self.assertFalse(p.exists())

    def test_empty_bucket_directory_removed(self):
        t, p = self._create_transcript_with_file()
        bucket_dir = p.parent
        with override_settings(MEDIA_ROOT=self.tmp):
            t.delete()
        self.assertFalse(bucket_dir.exists())


# ── 15. Creator settings — whisper field persistence ─────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class HandleUpdateNetworkWhisperTests(TestCase):

    def setUp(self):
        self.factory = RequestFactory()
        self.user    = User.objects.create_user(username='owner_hun')
        self.net     = Network.objects.create(name='WN', slug='wn-tx')

    def _call(self, post_data):
        from django.contrib.messages.storage.fallback import FallbackStorage
        from pod_manager.views.creator.actions import handle_update_network
        req = self.factory.post('/creator/', data={
            'theme_config': '{}',
            'patreon_campaign_id': '', 'website_url': '', 'default_image_url': '',
            'ignored_title_tags': '', 'description_cut_triggers': '',
            'footer_public': '', 'footer_private': '',
            **post_data,
        })
        req.user = self.user
        req.session = {}  # FallbackStorage only needs dict-like session
        req._messages = FallbackStorage(req)
        with mock.patch('pod_manager.views.creator.actions.task_rebuild_podcast_fragments'):
            handle_update_network(req, self.net)
        self.net.refresh_from_db()

    def test_saves_whisper_model(self):
        self._call({'whisper_model': 'large', 'whisper_language': 'en'})
        self.assertEqual(self.net.whisper_model, 'large')

    def test_saves_whisper_language(self):
        self._call({'whisper_model': 'medium.en', 'whisper_language': 'es'})
        self.assertEqual(self.net.whisper_language, 'es')

    def test_blank_model_falls_back_to_default(self):
        self._call({'whisper_model': '', 'whisper_language': 'en'})
        self.assertEqual(self.net.whisper_model, 'medium.en')

    def test_blank_language_falls_back_to_en(self):
        self._call({'whisper_model': 'medium.en', 'whisper_language': ''})
        self.assertEqual(self.net.whisper_language, 'en')

    def test_saves_speaker_counts(self):
        self._call({
            'whisper_model': 'medium.en', 'whisper_language': 'en',
            'whisper_min_speakers': '1',
            'whisper_num_speakers': '3',
            'whisper_max_speakers': '6',
        })
        self.assertEqual(self.net.whisper_min_speakers, 1)
        self.assertEqual(self.net.whisper_num_speakers, 3)
        self.assertEqual(self.net.whisper_max_speakers, 6)

    def test_saves_initial_prompt(self):
        self._call({
            'whisper_model': 'medium.en', 'whisper_language': 'en',
            'whisper_initial_prompt': 'Hosts: Jim and A.Ron.',
        })
        self.assertEqual(self.net.whisper_initial_prompt, 'Hosts: Jim and A.Ron.')


@override_settings(**TRANSCRIPTION_SETTINGS)
class HandleUpdateShowWhisperTests(TestCase):

    def setUp(self):
        self.factory = RequestFactory()
        self.user    = User.objects.create_user(username='owner_hus')
        self.net     = Network.objects.create(name='SWN', slug='swn-tx')
        self.pod     = Podcast.objects.create(network=self.net, title='SP2', slug='sp2-tx')

    def _call(self, post_data):
        from django.contrib.messages.storage.fallback import FallbackStorage
        from pod_manager.views.creator.actions import handle_update_show
        req = self.factory.post('/creator/', data={
            'show_id': str(self.pod.id),
            'public_feed_url': '', 'subscriber_feed_url': '',
            'show_footer_public': '', 'show_footer_private': '',
            **post_data,
        })
        req.user = self.user
        req.session = {}  # FallbackStorage only needs dict-like session
        req._messages = FallbackStorage(req)
        with mock.patch('pod_manager.views.creator.actions.task_rebuild_podcast_fragments'):
            handle_update_show(req, self.net)
        self.pod.refresh_from_db()

    def test_sets_whisper_model_override(self):
        self._call({'whisper_model': 'small'})
        self.assertEqual(self.pod.whisper_model, 'small')

    def test_blank_model_stores_none_to_inherit_from_network(self):
        self.pod.whisper_model = 'large'
        self.pod.save()
        self._call({'whisper_model': ''})
        self.assertIsNone(self.pod.whisper_model)

    def test_sets_speaker_overrides(self):
        self._call({'whisper_min_speakers': '1', 'whisper_num_speakers': '2', 'whisper_max_speakers': '4'})
        self.assertEqual(self.pod.whisper_min_speakers, 1)
        self.assertEqual(self.pod.whisper_num_speakers, 2)
        self.assertEqual(self.pod.whisper_max_speakers, 4)

    def test_blank_speaker_count_stores_none_to_inherit(self):
        self.pod.whisper_num_speakers = 4
        self.pod.save()
        self._call({'whisper_num_speakers': ''})
        self.assertIsNone(self.pod.whisper_num_speakers)
