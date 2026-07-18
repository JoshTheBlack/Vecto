"""One-time scan for pre-fix duplicate-GUID episode rows, seeded as reviewable
EpisodeMatchSuggestion pairs.

Before the ingest fix in commit_episode's guids_diverge branch (see
planned_migration_match_suggestions.txt §1b), a diverging auto-migrate could
mint two saved Episode rows sharing the same guid_private (or guid_public) in
one network — corruption that live detection can no longer see once it has
happened, because live detection only asks "does the incoming public GUID
match a row?" / "...private GUID?" (two .first() lookups) and a row holding
BOTH duplicated GUIDs can answer both questions, hiding the tangle entirely.

This command finds those historical duplicates the only way possible: a
column scan, per network, for every GUID value shared by more than one
Episode row in guid_public or guid_private (excluding null/blank). Each
collision is seeded as an EpisodeMatchSuggestion
(detected_reason="backfill_duplicate_guid") through the same service used by
live detection (record_match_suggestion) — same partial-unique-constraint
insert, same GUID-triple sticky-dismiss check, same PENDING dedup. A cluster
of more than two rows sharing one GUID value seeds one card per collision,
each extra row paired against the cluster's best candidate row, so the owner
resolves the cluster via successive merges (N rows -> N-1 merges).

Preview is the default; pass --apply to persist. Neither role assignment nor
the "best candidate" cluster anchor matters functionally for same-podcast
duplicates (§4b: "roles are cosmetic anyway") — they only decide which FK
slot (public_episode / private_episode) each row lands in on the suggestion
row, and which side of a cross-parent card shows first.

    python manage.py backfill_match_suggestions --network=baldmove          # preview
    python manage.py backfill_match_suggestions --network=baldmove --apply
    python manage.py backfill_match_suggestions --all --apply
"""
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count

from pod_manager.admin_console.summary import emit_summary
from pod_manager.models import Episode, EpisodeMatchSuggestion, Network
from pod_manager.services.match_editor import _has_transcript
from pod_manager.services.match_suggestions import record_match_suggestion

GUID_FIELDS = ('guid_private', 'guid_public')


def _cluster_anchor(cluster):
    """The cluster's 'best candidate' row for pairing extra rows against
    (§4b): transcript owner, else subscriber-audio owner, else the oldest row
    (lowest id — Episode has no created_at; insertion order is what "older"
    means for a same-podcast ingest-race duplicate)."""
    def score(ep):
        return (0 if _has_transcript(ep) else 1, 0 if ep.audio_url_subscriber else 1, ep.id)
    return min(cluster, key=score)


def _assign_roles(row_a, row_b):
    """Which of a colliding pair becomes public_episode vs private_episode,
    per §4b's tiebreaker order:
      1. subscriber-audio / transcript ownership suggests the PRIVATE role
         (mirrors default_survivor's reading of those signals) — but the
         2026-07-17 prod scan found both rows of every real pair carrying
         both, so this tier rarely discriminates.
      2. guid_public presence suggests the PUBLIC role.
      3. older-row-wins-public (lower id) as the final fallback.
    Returns (public_row, private_row)."""
    def is_private_like(ep):
        return _has_transcript(ep) or bool(ep.audio_url_subscriber)

    a_priv, b_priv = is_private_like(row_a), is_private_like(row_b)
    if a_priv != b_priv:
        return (row_b, row_a) if a_priv else (row_a, row_b)

    a_pub, b_pub = bool(row_a.guid_public), bool(row_b.guid_public)
    if a_pub != b_pub:
        return (row_a, row_b) if a_pub else (row_b, row_a)

    return (row_a, row_b) if row_a.id < row_b.id else (row_b, row_a)


class Command(BaseCommand):
    help = ("Scan each network for episodes sharing the same guid_public/guid_private "
            "(pre-fix ingest corruption) and seed EpisodeMatchSuggestion rows. Preview "
            "by default; pass --apply to save.")

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Scan every network.")
        parser.add_argument("--network", help="Restrict the scan to a network slug.")
        parser.add_argument("--apply", action="store_true",
                             help="Persist changes (default is a preview that lists them only).")

    def handle(self, *args, **options):
        if options["network"]:
            networks = Network.objects.filter(slug=options["network"])
            if not networks.exists():
                raise CommandError(f"No network with slug '{options['network']}'")
        elif options["all"]:
            networks = Network.objects.all()
        else:
            raise CommandError("Specify a scope: --all / --network=<slug>.")

        apply_ = options["apply"]
        preview = not apply_

        self.stdout.write(
            f"{networks.count()} network(s) selected (mode={'preview' if preview else 'apply'})."
        )

        seeded = refreshed = dismissed_skips = already_processed = 0

        for network in networks:
            seen_pairs = set()  # frozenset({episode_id, episode_id}) already handled this network

            for guid_field in GUID_FIELDS:
                collisions = (
                    Episode.objects.filter(podcast__network=network)
                    .exclude(**{f"{guid_field}__isnull": True})
                    .exclude(**{guid_field: ""})
                    .values(guid_field)
                    .annotate(cnt=Count("id"))
                    .filter(cnt__gt=1)
                )
                for row in collisions:
                    guid_value = row[guid_field]
                    cluster = list(
                        Episode.objects.filter(podcast__network=network, **{guid_field: guid_value})
                        .select_related('podcast')
                        .order_by('id')
                    )
                    anchor = _cluster_anchor(cluster)
                    for other in cluster:
                        if other.pk == anchor.pk:
                            continue
                        pair_key = frozenset((anchor.pk, other.pk))
                        if pair_key in seen_pairs:
                            already_processed += 1
                            continue
                        seen_pairs.add(pair_key)

                        public_ep, private_ep = _assign_roles(anchor, other)
                        pub_guid = public_ep.guid_public or ''
                        priv_guid = private_ep.guid_private or ''

                        if preview:
                            if EpisodeMatchSuggestion.objects.filter(
                                network=network, pub_guid=pub_guid, priv_guid=priv_guid,
                                status=EpisodeMatchSuggestion.Status.DISMISSED,
                            ).exists():
                                dismissed_skips += 1
                                self.stdout.write(
                                    f"  [{network.slug}] {guid_field}={guid_value!r}: "
                                    f"ep {public_ep.id}/{private_ep.id} — sticky-dismissed, would skip"
                                )
                                continue
                            if EpisodeMatchSuggestion.objects.filter(
                                public_episode=public_ep, private_episode=private_ep,
                                status=EpisodeMatchSuggestion.Status.PENDING,
                            ).exists():
                                refreshed += 1
                                self.stdout.write(
                                    f"  [{network.slug}] {guid_field}={guid_value!r}: "
                                    f"ep {public_ep.id}/{private_ep.id} — already PENDING, would bump last_seen_at"
                                )
                                continue
                            seeded += 1
                            self.stdout.write(
                                f"  [{network.slug}] {guid_field}={guid_value!r}: "
                                f"would seed ep {public_ep.id} (public) / {private_ep.id} (private)"
                            )
                            continue

                        before = EpisodeMatchSuggestion.objects.filter(
                            public_episode=public_ep, private_episode=private_ep,
                            status=EpisodeMatchSuggestion.Status.PENDING,
                        ).exists()
                        suggestion = record_match_suggestion(
                            public_ep, private_ep,
                            source=private_ep.podcast, target=public_ep.podcast,
                            reason="backfill_duplicate_guid",
                        )
                        if suggestion is None:
                            dismissed_skips += 1
                            self.stdout.write(
                                f"  [{network.slug}] {guid_field}={guid_value!r}: "
                                f"ep {public_ep.id}/{private_ep.id} — sticky-dismissed, skipped"
                            )
                        elif before:
                            refreshed += 1
                            self.stdout.write(
                                f"  [{network.slug}] {guid_field}={guid_value!r}: "
                                f"ep {public_ep.id}/{private_ep.id} — already PENDING, bumped last_seen_at"
                            )
                        else:
                            seeded += 1
                            self.stdout.write(
                                f"  [{network.slug}] {guid_field}={guid_value!r}: "
                                f"seeded suggestion #{suggestion.pk} "
                                f"(public=ep {public_ep.id}, private=ep {private_ep.id})"
                            )

        self.stdout.write(self.style.SUCCESS(
            f"\n{'Would seed' if preview else 'Seeded'} {seeded} suggestion(s). "
            f"{'Would refresh' if preview else 'Refreshed'} {refreshed} already-PENDING pair(s). "
            f"Skipped {dismissed_skips} sticky-dismissed pair(s); "
            f"{already_processed} pair(s) already handled by the other GUID column."
        ))
        if preview:
            self.stdout.write("Re-run with --apply to perform the changes.")
        emit_summary(self.stdout, {
            "mode": "preview" if preview else "apply",
            "seeded": seeded, "refreshed": refreshed,
            "dismissed_skips": dismissed_skips, "already_processed": already_processed,
        })
