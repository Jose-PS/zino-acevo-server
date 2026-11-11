import io
import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

from dashboard import app, event_store


class EventStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        patcher = patch.object(event_store, "DB_PATH", self.tmp / "data" / "events.sqlite")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(event_store.flush)

    def test_empty_store_has_null_ids(self):
        self.assertEqual(event_store.fetch(), {"events": [], "first_id": None, "last_id": None})

    def test_record_strips_newline_and_assigns_ascending_ids(self):
        self.assertTrue(event_store.record(b"[a] one\r\n"))
        self.assertTrue(event_store.record("[b] two\n"))
        event_store.flush()
        result = event_store.fetch()
        self.assertEqual([event["line"] for event in result["events"]], ["[a] one", "[b] two"])
        self.assertEqual(result["first_id"], result["events"][0]["id"])
        self.assertEqual(result["last_id"], result["events"][1]["id"])
        self.assertLess(result["first_id"], result["last_id"])

    def test_duplicate_lines_are_ignored(self):
        self.assertTrue(event_store.record("[a] same"))
        self.assertTrue(event_store.record("[a] same"))
        event_store.flush()
        self.assertEqual(len(event_store.fetch()["events"]), 1)

    def test_ping_and_blank_lines_are_filtered(self):
        self.assertFalse(event_store.record("[2026] [net] Received Ping from 1"))
        self.assertFalse(event_store.record("\n"))
        self.assertEqual(event_store.fetch()["events"], [])

    def test_cursor_pagination(self):
        for index in range(5):
            event_store.record(f"line {index}")
        event_store.flush()
        first = event_store.fetch(after=0, limit=2)
        self.assertEqual([event["line"] for event in first["events"]], ["line 0", "line 1"])
        second = event_store.fetch(after=first["last_id"], limit=2)
        self.assertEqual([event["line"] for event in second["events"]], ["line 2", "line 3"])
        rest = event_store.fetch(after=second["last_id"], limit=10)
        self.assertEqual([event["line"] for event in rest["events"]], ["line 4"])
        end = event_store.fetch(after=rest["last_id"])
        self.assertEqual(end, {"events": [], "first_id": None, "last_id": None})

    def test_limit_is_clamped(self):
        event_store.record("line")
        event_store.flush()
        self.assertEqual(len(event_store.fetch(limit=0)["events"]), 1)
        self.assertEqual(len(event_store.fetch(limit=10**9)["events"]), 1)

    def test_import_file_dedupes_and_filters(self):
        log = self.tmp / "server.log"
        log.write_bytes(b"[a] one\n[b] Received Ping\n[c] two\n[a] one\n")
        self.assertEqual(event_store.import_file(log), 2)
        self.assertEqual(event_store.import_file(log), 0)
        self.assertEqual([event["line"] for event in event_store.fetch()["events"]], ["[a] one", "[c] two"])

    def test_import_missing_file_is_noop(self):
        self.assertEqual(event_store.import_file(self.tmp / "missing.log"), 0)

    def test_import_flushes_pending_queue_first_to_keep_order(self):
        event_store.record("[a] queued")
        event_store.import_lines(["[b] imported"])
        self.assertEqual([event["line"] for event in event_store.fetch()["events"]], ["[a] queued", "[b] imported"])

    def test_record_never_blocks_on_write_failure_and_writer_survives(self):
        with (
            patch.object(event_store, "_insert_batch", side_effect=RuntimeError("db down")),
            patch.object(event_store.sys, "stderr", io.StringIO()) as stderr,
        ):
            self.assertTrue(event_store.record("[a] lost"))
            self.assertTrue(event_store.flush())
        self.assertIn("event store write failed", stderr.getvalue())
        self.assertTrue(event_store.record("[b] kept"))
        self.assertTrue(event_store.flush())
        self.assertEqual([event["line"] for event in event_store.fetch()["events"]], ["[b] kept"])

    def test_large_burst_is_batched_and_ordered(self):
        for index in range(event_store.BATCH_SIZE * 2 + 7):
            event_store.record(f"line {index}")
        self.assertTrue(event_store.flush())
        events = event_store.fetch(limit=event_store.MAX_LIMIT)["events"]
        self.assertEqual(len(events), event_store.BATCH_SIZE * 2 + 7)
        self.assertEqual(events[0]["line"], "line 0")
        self.assertEqual(events[-1]["line"], f"line {event_store.BATCH_SIZE * 2 + 6}")

    def test_concurrent_records_from_threads(self):
        def worker(offset):
            for index in range(20):
                event_store.record(f"thread {offset} line {index}")

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertTrue(event_store.flush())
        self.assertEqual(len(event_store.fetch(limit=5000)["events"]), 80)


class EventsEndpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        patcher = patch.object(event_store, "DB_PATH", self.tmp / "events.sqlite")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _serve(self, password=""):
        config = app.DashboardConfig(config_path=self.tmp / "server_launcher.json", user="admin", password=password)
        server = app.make_server(config, "127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}"

    def _get(self, base, path, headers=None):
        request = urllib.request.Request(base + path, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            self.addCleanup(exc.close)
            return exc.code, json.loads(exc.read())

    def test_returns_events_and_cursor_ids(self):
        event_store.record("[a] one")
        event_store.record("[b] two")
        event_store.flush()
        base = self._serve()
        status, body = self._get(base, "/api/server/events")
        self.assertEqual(status, 200)
        self.assertEqual([event["line"] for event in body["events"]], ["[a] one", "[b] two"])
        self.assertEqual(body["first_id"], body["events"][0]["id"])
        self.assertEqual(body["last_id"], body["events"][1]["id"])
        status, body = self._get(base, f"/api/server/events?after={body['first_id']}&limit=5")
        self.assertEqual([event["line"] for event in body["events"]], ["[b] two"])

    def test_empty_store(self):
        status, body = self._get(self._serve(), "/api/server/events")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"events": [], "first_id": None, "last_id": None})

    def test_limit_boundaries(self):
        base = self._serve()
        self.assertEqual(self._get(base, f"/api/server/events?limit={event_store.MAX_LIMIT}")[0], 200)
        self.assertEqual(self._get(base, f"/api/server/events?limit={event_store.MAX_LIMIT + 1}")[0], 400)
        self.assertEqual(self._get(base, "/api/server/events?limit=0")[0], 400)

    def test_invalid_parameters_return_400(self):
        base = self._serve()
        for query in ("after=abc", "limit=xyz", "after=-1", "limit=1.5"):
            status, body = self._get(base, f"/api/server/events?{query}")
            self.assertEqual(status, 400, query)
            self.assertIn("error", body)

    def test_requires_authentication(self):
        base = self._serve(password="s3cret")
        status, _body = self._get(base, "/api/server/events")
        self.assertEqual(status, 401)
