import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

from paldo_os_outbound import Database
from notifier import (
    CONSOLE_BACKEND,
    JSON_FILE_BACKEND,
    NOTIFICATION_ALLOWED_ACTIONS_VALUE,
    NOTIFICATION_CHANNEL_ID,
    NOTIFICATION_CHANNELS,
    NOTIFICATION_JSONL_DEFAULT_PATH,
    NOTIFICATION_OPERATOR_ID,
    ConsoleBackend,
    JsonFileBackend,
    Notifier,
    NotifierBlockedError,
    NotifierExternalStateUnknown,
    NotifierValidationError,
    inspect_notifier_readiness,
    migrate_notifier,
    notification_jsonl_path,
    queue_notification,
    reconcile_notification_outcome,
    resolve_backend,
    summarize_notifier_provenance,
    validate_notification_registry,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


class NotifierTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db = Database(Path(self.temp_dir.name) / "notifier.sqlite3")
        self.addCleanup(self.db.close)
        migrate_notifier(self.db)

    def enable_delivery(self, cap: int = 5) -> None:
        self.db.set_config("notifications_enabled", 1)
        self.db.set_config("notification_daily_cap", cap)

    def test_registry_and_safe_defaults_are_stored(self):
        registry = validate_notification_registry(self.db)
        self.assertTrue(registry["valid"])
        self.assertEqual(registry["channel_id"], NOTIFICATION_CHANNEL_ID)
        self.assertEqual(registry["operator_id"], NOTIFICATION_OPERATOR_ID)
        self.assertEqual(registry["topics"], dict(NOTIFICATION_CHANNELS))
        config = self.db.read_config()
        self.assertEqual(config["notification_backend"], CONSOLE_BACKEND)
        self.assertEqual(config["notification_allowed_actions"], NOTIFICATION_ALLOWED_ACTIONS_VALUE)
        self.assertEqual(config["notifications_enabled"], 0)
        self.assertEqual(config["notification_daily_cap"], 0)
        self.assertEqual(config["notification_jsonl_path"], NOTIFICATION_JSONL_DEFAULT_PATH)

    def test_configured_route_values_are_stored_verbatim(self):
        row = self.db.connection.execute("SELECT * FROM notification_registry WHERE id=1").fetchone()
        self.assertEqual(row["channel_id"], NOTIFICATION_CHANNEL_ID)
        self.assertEqual(row["operator_id"], NOTIFICATION_OPERATOR_ID)
        self.assertEqual(row["control_topic_id"], NOTIFICATION_CHANNELS["control"])
        self.assertEqual(row["outbound_queue_topic_id"], NOTIFICATION_CHANNELS["outbound_queue"])
        self.assertEqual(row["alerts_topic_id"], NOTIFICATION_CHANNELS["alerts"])
        self.assertEqual(row["policy_version"], "PALDO_NOTIFY_V1")

    def test_readiness_is_safe_and_canonical_is_blocked(self):
        self.enable_delivery()
        ready = inspect_notifier_readiness(self.db, fixture_override=True, now=NOW)
        self.assertTrue(ready["ready"], ready)
        self.assertEqual(ready["backend"], CONSOLE_BACKEND)
        self.assertFalse(ready["external_operations"])
        canonical = Database(Path(self.temp_dir.name) / "canonical-guard.sqlite3")
        self.addCleanup(canonical.close)
        canonical.path = Path(__file__).resolve().parents[1] / "data" / "paldo_os_outbound.sqlite3"
        migrate_notifier(canonical)
        blocked = inspect_notifier_readiness(canonical, fixture_override=False, now=NOW)
        self.assertFalse(blocked["ready"])
        self.assertIn("CANONICAL_NOTIFICATION_OPERATION_BLOCKED", blocked["blocking_reasons"])

    def test_notifier_exposes_only_the_delivery_methods(self):
        notifier = Notifier()
        self.assertTrue(callable(notifier.send_message))
        self.assertTrue(callable(notifier.edit_message))
        for name in (
            "delete_message", "create_topic", "get_updates", "poll", "configure_webhook",
            "get_members", "send_email", "answer_callback_query",
        ):
            self.assertFalse(hasattr(notifier, name), name)

    def test_console_backend_prints_a_readable_summary(self):
        stream = StringIO()
        notifier = Notifier(ConsoleBackend(stream=stream))
        result = notifier.send_message({
            "channel_id": NOTIFICATION_CHANNEL_ID,
            "thread_id": NOTIFICATION_CHANNELS["alerts"],
            "text": "Fictional alert card.\nSecond line.",
            "message_fingerprint": "console-fp",
        })
        printed = stream.getvalue()
        self.assertEqual(result["status"], "SENT")
        self.assertIn("SENT", printed)
        self.assertIn(str(NOTIFICATION_CHANNELS["alerts"]), printed)
        self.assertIn("Fictional alert card.", printed)
        self.assertIn("Second line.", printed)

    def test_json_file_backend_appends_one_line_per_notification_from_config(self):
        target = Path(self.temp_dir.name) / "out" / "notifications.jsonl"
        self.db.set_config("notification_jsonl_path", str(target))
        self.assertEqual(notification_jsonl_path(self.db), str(target))
        notifier = Notifier(resolve_backend(JSON_FILE_BACKEND, self.db))
        for text in ("First notification.", "Second notification."):
            notifier.send_message({
                "channel_id": NOTIFICATION_CHANNEL_ID,
                "thread_id": NOTIFICATION_CHANNELS["outbound_queue"],
                "text": text,
                "message_fingerprint": f"fp-{text[:5]}",
            })
        lines = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(lines), 2)
        self.assertEqual([line["text"] for line in lines], ["First notification.", "Second notification."])
        self.assertEqual(lines[0]["action"], "SEND_MESSAGE")
        self.assertEqual(lines[0]["channel_id"], NOTIFICATION_CHANNEL_ID)
        self.assertTrue(all(line["message_id"] for line in lines))

    def test_backend_resolution_rejects_unknown_backends(self):
        with self.assertRaises(NotifierValidationError):
            resolve_backend("FIXTURE_ONLY")
        with self.assertRaises(NotifierValidationError):
            JsonFileBackend("")

    def test_outbox_persists_before_delivery_and_reuses_success(self):
        self.enable_delivery()
        notifier = Notifier()
        first = queue_notification(self.db, notifier=notifier, topic_id=NOTIFICATION_CHANNELS["outbound_queue"], text="Fictional review card.", entity_type="draft", entity_id="7", entity_version="1", content_fingerprint="fp-1", fixture_override=True, now=NOW)
        second = queue_notification(self.db, notifier=notifier, topic_id=NOTIFICATION_CHANNELS["outbound_queue"], text="Fictional review card.", entity_type="draft", entity_id="7", entity_version="1", content_fingerprint="fp-1", fixture_override=True, now=NOW)
        self.assertTrue(first["ok"], first)
        self.assertTrue(second["reused"])
        self.assertEqual(notifier.send_calls, 1)
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM notification_outbox").fetchone()[0], 1)
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM notification_attempts").fetchone()[0], 1)

    def test_queue_requires_an_allowlisted_route_and_override(self):
        self.enable_delivery()
        notifier = Notifier()
        with self.assertRaises(NotifierValidationError):
            queue_notification(self.db, notifier=notifier, topic_id=999, text="Fictional.", entity_type="draft", entity_id="1", entity_version="1", content_fingerprint="fp", fixture_override=True, now=NOW)
        with self.assertRaises(NotifierValidationError):
            queue_notification(self.db, notifier=notifier, topic_id=NOTIFICATION_CHANNELS["outbound_queue"], text="Fictional.", entity_type="draft", entity_id="1", entity_version="1", content_fingerprint="fp", channel_id=999, fixture_override=True, now=NOW)
        with self.assertRaises(NotifierBlockedError):
            queue_notification(self.db, notifier=notifier, topic_id=NOTIFICATION_CHANNELS["outbound_queue"], text="Fictional.", entity_type="draft", entity_id="1", entity_version="1", content_fingerprint="fp", fixture_override=False, now=NOW)

    def test_message_text_guards_and_zero_cap_block_fail_closed(self):
        self.enable_delivery(cap=1)
        notifier = Notifier()
        base = dict(topic_id=NOTIFICATION_CHANNELS["outbound_queue"], entity_type="draft", entity_id="1", entity_version="1", content_fingerprint="fp", fixture_override=True, now=NOW)
        with self.assertRaises(NotifierValidationError):
            queue_notification(self.db, notifier=notifier, text="bot123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZ", **base)
        with self.assertRaises(NotifierValidationError):
            queue_notification(self.db, notifier=notifier, text="x" * 4097, **base)
        self.db.set_config("notification_daily_cap", 0)
        blocked = queue_notification(self.db, notifier=notifier, text="Fictional card.", **base)
        self.assertFalse(blocked["ok"])
        self.assertEqual(blocked["result_category"], "OUTBOX_BLOCKED")
        self.assertIn("NOTIFICATION_DELIVERY_CAP_ZERO", blocked["blocking_reasons"])
        self.assertEqual(notifier.send_calls, 0)

    def test_unknown_outbox_outcome_does_not_duplicate_and_can_reconcile(self):
        self.enable_delivery()
        notifier = Notifier(unknown_on_send=True)
        first = queue_notification(self.db, notifier=notifier, topic_id=NOTIFICATION_CHANNELS["outbound_queue"], text="Unknown fixture outcome.", entity_type="draft", entity_id="8", entity_version="1", content_fingerprint="fp-8", fixture_override=True, now=NOW)
        self.assertFalse(first["ok"])
        again = queue_notification(self.db, notifier=notifier, topic_id=NOTIFICATION_CHANNELS["outbound_queue"], text="Unknown fixture outcome.", entity_type="draft", entity_id="8", entity_version="1", content_fingerprint="fp-8", fixture_override=True, now=NOW)
        self.assertTrue(again["reused"])
        self.assertEqual(notifier.send_calls, 1)
        reconciled = reconcile_notification_outcome(
            self.db,
            outbox_id=first["outbox_id"],
            external_result={"message_id": "notification-reconciled", "channel_id": NOTIFICATION_CHANNEL_ID, "thread_id": NOTIFICATION_CHANNELS["outbound_queue"], "text": "Unknown fixture outcome.", "status": "SENT"},
            fixture_override=True,
            now=NOW + timedelta(minutes=1),
        )
        self.assertTrue(reconciled["ok"], reconciled)
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM notification_message_mappings").fetchone()[0], 1)

    def test_unknown_outcome_is_raised_by_the_injected_backend_signal(self):
        notifier = Notifier(unknown_on_edit=True)
        with self.assertRaises(NotifierExternalStateUnknown) as raised:
            notifier.edit_message({"channel_id": NOTIFICATION_CHANNEL_ID, "thread_id": NOTIFICATION_CHANNELS["outbound_queue"], "text": "Fictional edit.", "message_fingerprint": "fp-edit"})
        self.assertTrue(raised.exception.external_result.get("message_id"))

    def test_migration_is_additive_idempotent_and_operational_tables_start_empty(self):
        self.assertEqual(migrate_notifier(self.db), 15)
        self.assertEqual(migrate_notifier(self.db), 15)
        names = {row[0] for row in self.db.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        expected = {"notification_registry", "notification_outbox", "notification_attempts", "notification_message_mappings", "notification_reconciliation_events"}
        self.assertTrue(expected <= names)
        for name in expected - {"notification_registry"}:
            self.assertEqual(self.db.connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0], 0)

    def test_provenance_is_safe_and_excludes_tokens_payloads_and_internal_details(self):
        provenance = summarize_notifier_provenance(self.db)
        encoded = str(provenance)
        self.assertIn(CONSOLE_BACKEND, encoded)
        self.assertIn("notification_registry", encoded)
        self.assertNotIn("token", encoded.lower())
        self.assertNotIn("stack", encoded.lower())
        self.assertFalse(provenance["external_operations"])


if __name__ == "__main__":
    unittest.main()
