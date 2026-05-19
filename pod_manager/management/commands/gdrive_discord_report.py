import csv
import os
from django.core.management.base import BaseCommand

CONFIDENCE_ORDER = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2}


class Command(BaseCommand):
    help = (
        'Generates a Discord-formatted links report from an existing recovery CSV. '
        'Use this when recover_gdrive_audio was run without the Discord report code deployed.'
    )

    def add_arguments(self, parser):
        parser.add_argument('csv_path', type=str, help='Path to the recovery report CSV')
        parser.add_argument(
            '--output', type=str, default=None,
            help='Output path for the Discord report. Defaults to discord_report.txt alongside the CSV.',
        )

    def handle(self, *args, **options):
        csv_path = options['csv_path']
        output_path = options['output']

        if not os.path.exists(csv_path):
            self.stdout.write(self.style.ERROR(f"CSV not found: {csv_path}"))
            return

        by_podcast = {}
        try:
            with open(csv_path, newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    by_podcast.setdefault(row['Podcast'], []).append(row)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to read CSV: {e}"))
            return

        if not by_podcast:
            self.stdout.write(self.style.WARNING("CSV is empty — nothing to report."))
            return

        discord_lines = []
        for pod_name in sorted(by_podcast):
            entries = sorted(
                by_podcast[pod_name],
                key=lambda r: (CONFIDENCE_ORDER.get(r.get('Confidence', ''), 0), r.get('Title', '')),
            )
            count = len(entries)
            discord_lines.append(f"**{pod_name} ({count} episode{'s' if count != 1 else ''})**")
            for entry in entries:
                discord_lines.append(
                    f"- {entry.get('Confidence', '?')} | "
                    f"[{entry.get('Title', '?')}]({entry.get('Vecto Link', '')}) | "
                    f"[Drive Audio]({entry.get('Verification Link', '')})"
                )
            discord_lines.append("")
            discord_lines.append("----------------------------------------")
            discord_lines.append("")

        if not output_path:
            base = os.path.splitext(csv_path)[0]
            output_path = f"{base}_discord.txt"

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(discord_lines))

        total = sum(len(v) for v in by_podcast.values())
        self.stdout.write(self.style.SUCCESS(
            f"Discord report: {os.path.abspath(output_path)} ({total} episodes, {len(by_podcast)} podcast(s))"
        ))
