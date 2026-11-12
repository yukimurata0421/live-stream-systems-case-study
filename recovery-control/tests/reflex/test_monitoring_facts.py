import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("facts", Path(__file__).resolve().parents[2] / "tools/monitoring_facts.py")
facts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(facts)


class FactsTests(unittest.TestCase):
    def test_payload_is_allowlisted_and_free_text_or_addresses_are_removed(self):
        x = facts.sanitized(
            {
                "reason": "TARGET_UNAVAILABLE",
                "mode": "enforce",
                "policy_version": 3,
                "endpoint": "private",
                "token": "secret",
                "response": {"key": "secret"},
            }
        )
        self.assertEqual(x, {"reason": "TARGET_UNAVAILABLE", "mode": "enforce", "policy_version": 3})
        self.assertEqual(facts.sanitized({"reason": "192.0.2.3", "state": "private credential text"}), {})

    def test_database_is_not_created_and_write_is_refused(self):
        with tempfile.TemporaryDirectory() as root:
            p = Path(root) / "facts.sqlite"
            with self.assertRaises(ValueError):
                facts.connect(p)
            self.assertFalse(p.exists())
            with sqlite3.connect(p) as c:
                c.execute("CREATE TABLE preserved(value TEXT)")
                c.execute("INSERT INTO preserved VALUES('original')")
            c = facts.connect(p)
            self.assertEqual(c.execute("SELECT value FROM preserved").fetchone()[0], "original")
            with self.assertRaises(sqlite3.OperationalError):
                c.execute("DELETE FROM preserved")
            c.close()
