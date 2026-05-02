"""
Security-focused test pass for the helpers and view changes added in the
recent hardening pass: signed OAuth state, SSRF guard, rate limiting, OTP
attempt accounting, and HTML sanitization. Plus a couple of view-level
integration tests for the Recurly login rate limits.

Run with: python manage.py test pod_manager
"""
import time
from datetime import timedelta
from unittest import mock

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import TestCase, RequestFactory, override_settings
from django.urls import reverse
from django.utils import timezone

from pod_manager import views
from pod_manager.models import (
    Network, PatronProfile, Podcast, Episode, NetworkMembership, NetworkMix
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
        req.session['pending_otp_email'] = 'victim@example.com'
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
        with mock.patch('pod_manager.views.recurly.Client') as mock_client_cls:
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


@override_settings(CACHES=TEST_CACHES)
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
        import fakeredis
        self.user = User.objects.create_user(username='listener', email='listener@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.membership = NetworkMembership.objects.create(user=self.user, network=self.network)
        self.fake = fakeredis.FakeRedis()
        # Force the sweep onto a non-locmem cache so the early return doesn't
        # fire, AND make redis.from_url return our fakeredis instance.
        self._patches = [
            mock.patch.object(views, 'cache', cache),  # not strictly needed
            mock.patch('pod_manager.tasks.redis.from_url', return_value=self.fake),
            override_settings(CACHES={
                'default': {
                    'BACKEND': 'django.core.cache.backends.redis.RedisCache',
                    'LOCATION': 'redis://fake:6379/0',
                }
            }),
        ]
        for p in self._patches:
            p.enable() if hasattr(p, 'enable') else p.__enter__()

    def tearDown(self):
        for p in reversed(self._patches):
            p.disable() if hasattr(p, 'disable') else p.__exit__(None, None, None)

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
