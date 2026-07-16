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
    generate_calendar_feed,
    play_episode,
    episode_chapters,
)

from .creator import (
    creator_settings,
    merge_desk_partial,
    creator_show_form,
    creator_mix_form,
    creator_audit_edit,
    creator_tab_partial,
    submit_episode_edit,
    submit_speaker_labels,
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
    import_feed_start,
    import_feed_poll,
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

from .staff import log_viewer, log_stream, log_poll, log_level_toggle, log_resources, superuser_required

from .admin_console import (
    console as admin_console,
    command_detail as admin_console_command_detail,
    build as admin_console_build,
    run as admin_console_run,
    run_poll as admin_console_run_poll,
    run_cancel as admin_console_run_cancel,
    run_detail as admin_console_run_detail,
    history as admin_console_history,
    episode_search as admin_console_episode_search,
)

from .creator.gdrive_recovery import (
    gdrive_recovery_files,
    gdrive_recovery_run,
    gdrive_recovery_poll,
    gdrive_recovery_rewind,
)

from .calendar import (
    calendar_page,
    calendar_events,
    calendar_manage,
)

from .transcripts import serve_transcript
from .api import backfill_transcripts_api, retranscribe_episode_api
