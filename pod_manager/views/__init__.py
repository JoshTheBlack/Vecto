"""
Re-exports the public symbols of the pod_manager view layer so that
`from pod_manager import views` and `views.<name>` keep working after the
package split. Also re-exports module names (recurly) and private helpers
(_link_creator_campaign, _fetch_patreon_identity, _exchange_patreon_token)
that tests target with mock.patch('pod_manager.views.<name>').
"""

from .auth import (
    patreon_login,
    patreon_callback,
    logout_view,
    recurly_login,
    RecurlyLoginView,
    LoginState,
    start_impersonation,
    stop_impersonation,
    generate_qr_code,
    verify_authenticator,
    remove_authenticator,
    _exchange_patreon_token,
    _link_creator_campaign,
    _fetch_patreon_identity,
    _secure_login,
    recurly,
)

from .feeds import (
    RSSFeedBuilder,
    parse_duration,
    get_or_build_feed_shell,
    get_or_build_episode_fragment,
    generate_custom_feed,
    generate_public_feed,
    generate_mix_feed,
    generate_network_mix_feed,
    play_episode,
    episode_chapters,
)

from .creator import (
    creator_settings,
    submit_episode_edit,
    publish_episode,
    manage_episode,
    _handle_approve_edit,
    _handle_reject_edit,
    _handle_rollback_single_edit,
    _handle_bulk_rollback,
)

from .listener import (
    home,
    user_feeds,
    episode_detail,
    user_profile,
)

from .api import (
    traefik_config_api,
    check_audio_status,
    update_avatar_preference,
    upload_custom_avatar,
    stream_feed_import,
    process_mix_image_url,
    toggle_totp_mode,
)

# patreon_webhook lives in services because it's a pure data-receiver — but
# config/urls.py wires it through `views.patreon_webhook`, so re-export it
# here to avoid disturbing url config.
from ..services.patreon import patreon_webhook

# Tests target these via mock.patch('pod_manager.views.X') and the old views.py
# imported them at the module level, so preserve that surface.
from ..security import (
    _sign_oauth_state,
    _unsign_oauth_state,
    _is_rate_limited,
    _client_ip,
    _record_otp_failure,
    _clear_otp_state,
    MAX_OTP_ATTEMPTS,
)
from ..utils import validate_public_url, sanitize_user_html

from .staff import log_viewer, log_stream, log_level_toggle, log_resources
