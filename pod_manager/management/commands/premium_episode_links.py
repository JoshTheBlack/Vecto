"""List a podcast's premium episodes as Discord-ready audio links.

Each premium episode becomes one Markdown link, oldest first:

    - [S01E03 full - Episode Title](https://audio.example/ep3.mp3)

The season/episode tag and the episode type are left out when the episode has
none. Every candidate URL is checked with a HEAD request before it is used, in
priority order R2 > private (subscriber) > public, and the first one that answers
with real audio wins:

  - R2 link        plain link
  - private/public ⚠️ prefix and a "(private)" / "(public)" note, so it is
                   obvious the R2 mirror is missing or broken
  - nothing works  ❌ and the label struck through, with no link

Output is split into blocks of at most 2000 characters (Discord's message limit),
each ready to paste as one message; lines are never split across blocks.
Read-only: nothing is written to the database.

    python manage.py premium_episode_links --podcast=watchmen
    python manage.py premium_episode_links --podcast=watchmen --include-unpublished
    python manage.py premium_episode_links --podcast=watchmen --timeout=20
"""

from collections import Counter

from django.core.management.base import BaseCommand, CommandError

from pod_manager.admin_console.summary import emit_summary
from pod_manager.models import Episode, Podcast
from pod_manager.services.url_probe import probe_url

DISCORD_MESSAGE_LIMIT = 2000

# (label, Episode field) in the order a working URL is preferred.
AUDIO_SOURCES = (
    ('r2', 'r2_url'),
    ('private', 'audio_url_subscriber'),
    ('public', 'audio_url_public'),
)
PREFERRED_SOURCE = AUDIO_SOURCES[0][0]
DEAD = 'dead'


def episode_label(episode):
    """'S01E03 full - Title', dropping the tag and type when they're unset."""
    sxe = ''
    if episode.season_number is not None and episode.episode_number is not None:
        sxe = f'S{episode.season_number:02d}E{episode.episode_number:02d}'
    prefix = ' '.join(part for part in (sxe, episode.episode_type) if part)
    return f'{prefix} - {episode.title}' if prefix else episode.title


def _link_text(text):
    """Escape the characters that would end a Markdown link's text early."""
    return text.replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]')


def _link_target(url):
    """Percent-encode the characters that would end a Markdown link's URL early."""
    return url.replace(' ', '%20').replace('(', '%28').replace(')', '%29')


def resolve_audio(episode, timeout):
    """Return (source, url) for the first source that serves audio, or None."""
    for source, field in AUDIO_SOURCES:
        url = getattr(episode, field)
        if url and probe_url(url, timeout=timeout).serves_audio:
            return source, url
    return None


def render_line(label, resolved):
    """One Discord list item for an episode."""
    if resolved is None:
        return f'- ❌ ~~{label}~~'
    source, url = resolved
    link = f'[{_link_text(label)}]({_link_target(url)})'
    if source == PREFERRED_SOURCE:
        return f'- {link}'
    return f'- ⚠️ {link} ({source})'


def chunk_lines(lines, limit=DISCORD_MESSAGE_LIMIT):
    """Pack whole lines into newline-joined blocks no longer than ``limit``."""
    chunks, current = [], []
    size = 0
    for line in lines:
        added = len(line) + (1 if current else 0)
        if current and size + added > limit:
            chunks.append('\n'.join(current))
            current, size = [], 0
            added = len(line)
        current.append(line)
        size += added
    if current:
        chunks.append('\n'.join(current))
    return chunks


class Command(BaseCommand):
    help = ("Print a podcast's premium episodes as Discord-formatted audio links, "
            "checking each URL and falling back R2 > private > public.")

    def add_arguments(self, parser):
        parser.add_argument('--podcast', required=True,
                            help='Slug of the podcast whose premium episodes to list.')
        parser.add_argument('--include-unpublished', action='store_true',
                            help='Also list unpublished (draft and scheduled) episodes.')
        parser.add_argument('--timeout', type=float, default=10,
                            help='Seconds to wait for each URL check (default 10).')

    def handle(self, *args, **options):
        try:
            podcast = Podcast.objects.get(slug=options['podcast'])
        except Podcast.DoesNotExist:
            raise CommandError(f"No podcast with slug '{options['podcast']}'.")

        episodes = self._select(podcast, options['include_unpublished'])
        self.stdout.write(f"Checking {len(episodes)} premium episode(s) of '{podcast.title}'...")

        tally = Counter()
        lines = [f'**{podcast.title} — Premium Episodes**']
        for episode in episodes:
            resolved = resolve_audio(episode, options['timeout'])
            tally[resolved[0] if resolved else DEAD] += 1
            lines.append(render_line(episode_label(episode), resolved))

        messages = chunk_lines(lines)
        for number, message in enumerate(messages, start=1):
            self.stdout.write(f'\n--- Discord message {number}/{len(messages)} ---')
            self.stdout.write(message)

        fallbacks = len(episodes) - tally[PREFERRED_SOURCE] - tally[DEAD]
        self.stdout.write(self.style.SUCCESS(
            f"\n{len(episodes)} episode(s): {tally[PREFERRED_SOURCE]} on R2, "
            f"{fallbacks} on a fallback URL, {tally[DEAD]} with no working audio."))
        emit_summary(self.stdout, {
            'podcast': podcast.slug, 'episodes': len(episodes),
            **{source: tally[source] for source, _ in AUDIO_SOURCES},
            DEAD: tally[DEAD], 'messages': len(messages),
        })

    @staticmethod
    def _select(podcast, include_unpublished):
        qs = (Episode.objects
              .filter(podcast=podcast, audio_url_subscriber__isnull=False)
              .exclude(audio_url_subscriber='')
              .order_by('pub_date', 'id'))
        if not include_unpublished:
            qs = qs.filter(is_published=True)
        return [episode for episode in qs if episode.is_premium]
