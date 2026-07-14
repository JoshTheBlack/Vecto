"""
Manually re-render live /schedule Discord posts — the same work the
`task_refresh_live_schedules` Celery task does on episode publish, runnable
from the CLI for testing (e.g. after linking an already-published episode to a
calendar entry via /admin, which doesn't fire the publish hooks).

    ./.venv/Scripts/python.exe manage.py refresh_live_schedules              # all networks
    ./.venv/Scripts/python.exe manage.py refresh_live_schedules --network baldmove
    ./.venv/Scripts/python.exe manage.py refresh_live_schedules --list      # show, don't edit
"""
from django.core.management.base import BaseCommand, CommandError

from pod_manager.models import LiveSchedulePost, Network
from pod_manager.services.discord_schedule import refresh_live_posts


class Command(BaseCommand):
    help = "Re-render + PATCH live /schedule Discord embeds (test hook for the publish-time refresh)."

    def add_arguments(self, parser):
        parser.add_argument(
            '--network',
            help="Limit to one network (slug or numeric id). Default: all networks with live posts.",
        )
        parser.add_argument(
            '--list', action='store_true',
            help="List active live posts and exit without editing anything.",
        )

    def _resolve_network(self, ref):
        qs = Network.objects.all()
        network = (qs.filter(pk=ref).first() if ref.isdigit() else None) or qs.filter(slug=ref).first()
        if not network:
            raise CommandError(f"No network matching '{ref}' (tried id and slug).")
        return network

    def handle(self, *args, **options):
        network_id = None
        if options['network']:
            network_id = self._resolve_network(options['network']).id

        posts = LiveSchedulePost.objects.select_related('network').order_by('id')
        if network_id is not None:
            posts = posts.filter(network_id=network_id)

        if not posts.exists():
            self.stdout.write("No live schedule posts found for that scope.")
            return

        if options['list']:
            self.stdout.write(f"{posts.count()} live schedule post(s):")
            for p in posts:
                self.stdout.write(
                    f"  #{p.id}  {p.network.slug}  msg={p.message_id}  "
                    f"ch={p.channel_id}  until {p.window_end:%Y-%m-%d %H:%M}Z  · {p.subtitle}"
                )
            return

        results = refresh_live_posts(network_id)
        style = {
            'updated': self.style.SUCCESS,
            'expired': self.style.WARNING,
            'dropped': self.style.WARNING,
            'failed':  self.style.ERROR,
        }
        for post, action in results:
            painter = style.get(action, str)
            self.stdout.write(
                f"  #{post.id}  {post.network.slug}  msg={post.message_id}  ->  {painter(action)}"
            )
        counts = {}
        for _, action in results:
            counts[action] = counts.get(action, 0) + 1
        summary = ", ".join(f"{n} {a}" for a, n in sorted(counts.items())) or "nothing"
        self.stdout.write(self.style.SUCCESS(f"Done: {summary}."))
