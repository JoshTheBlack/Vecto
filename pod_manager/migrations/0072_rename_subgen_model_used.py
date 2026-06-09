from django.db import migrations


def rename_if_needed(apps, schema_editor):
    """Rename subgen_model_used → whisper_model_used only if the old column
    still exists. On a fresh DB, migration 0071 already creates the column
    as whisper_model_used, so there is nothing to do."""
    db = schema_editor.connection
    if db.vendor == 'sqlite':
        with db.cursor() as cursor:
            cursor.execute("PRAGMA table_info(pod_manager_transcript)")
            columns = [row[1] for row in cursor.fetchall()]
        if 'subgen_model_used' in columns:
            schema_editor.execute(
                'ALTER TABLE pod_manager_transcript '
                'RENAME COLUMN subgen_model_used TO whisper_model_used'
            )
    else:
        # PostgreSQL: safe no-op when column doesn't exist.
        schema_editor.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'pod_manager_transcript'
                      AND column_name = 'subgen_model_used'
                ) THEN
                    ALTER TABLE pod_manager_transcript
                    RENAME COLUMN subgen_model_used TO whisper_model_used;
                END IF;
            END $$;
            """
        )


def reverse_rename_if_needed(apps, schema_editor):
    db = schema_editor.connection
    if db.vendor == 'sqlite':
        with db.cursor() as cursor:
            cursor.execute("PRAGMA table_info(pod_manager_transcript)")
            columns = [row[1] for row in cursor.fetchall()]
        if 'whisper_model_used' in columns:
            schema_editor.execute(
                'ALTER TABLE pod_manager_transcript '
                'RENAME COLUMN whisper_model_used TO subgen_model_used'
            )
    else:
        schema_editor.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'pod_manager_transcript'
                      AND column_name = 'whisper_model_used'
                ) THEN
                    ALTER TABLE pod_manager_transcript
                    RENAME COLUMN whisper_model_used TO subgen_model_used;
                END IF;
            END $$;
            """
        )


class Migration(migrations.Migration):
    """
    0071_transcript was applied on the live dev DB when the field was still
    named subgen_model_used. The migration file was later corrected to
    whisper_model_used. This migration renames the actual column on DBs where
    the old name is still present. On a fresh DB the column is already correct,
    so this is a safe no-op.
    """

    dependencies = [
        ('pod_manager', '0071_transcript'),
    ]

    operations = [
        migrations.RunPython(rename_if_needed, reverse_rename_if_needed),
    ]
