"""HTTP tests for restoring one cell from an uploaded backup SQLite database."""

import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="battery-restore-test-")
_DB = os.path.join(_TMP, "test.db")
os.environ.setdefault("BATTERY_DB_PATH", _DB)

from fastapi.testclient import TestClient  # noqa: E402

from app.algorithm import detect_cycles  # noqa: E402
from app.db import Database  # noqa: E402
from app.main import app, db  # noqa: E402

LIVE_DB = db.db_path


def csv_text(rows, header="timestamp,current\n"):
    return header + "".join(f"{ts},{cur}\n" for ts, cur in rows)


def upload(client, cell_id, rows):
    return client.post(
        f"/cells/{cell_id}/samples",
        files={"file": ("samples.csv", csv_text(rows), "text/csv")},
    )


TWO_CYCLES = [
    (0, 0.0),
    (10, 2.0),
    (20, 0.0),
    (30, -3.0),
    (40, 0.0),
    (50, 1.0),
    (60, 0.0),
    (70, -1.5),
    (80, 0.0),
]


def make_backup(path, cell_id, batches, extra_cell=None):
    """Build a realistic backup with the production Database code, checkpointed.

    Returns the path to a single self-contained .db file (no -wal sidecar).
    """
    bdb = Database(path)
    for rows in batches:
        bdb.import_batch(cell_id, rows)
    if extra_cell is not None:
        other, other_rows = extra_cell
        bdb.import_batch(other, other_rows)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    return path


def scrub_sidecars(path):
    for suffix in ("-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


class RestoreTestBase(unittest.TestCase):
    def setUp(self):
        self.c = TestClient(app)
        self.tmp = tempfile.mkdtemp(prefix="restore-case-", dir=_TMP)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def new_backup(self, cell_id, batches=(TWO_CYCLES, TWO_CYCLES), extra_cell=None):
        path = os.path.join(self.tmp, f"backup-{cell_id}.db")
        make_backup(path, cell_id, list(batches), extra_cell=extra_cell)
        scrub_sidecars(path)
        return path

    def post_backup(self, cell_id, path, filename="backup.db", client=None):
        with open(path, "rb") as fh:
            content = fh.read()
        return (client or self.c).post(
            f"/cells/{cell_id}/restore",
            files={"file": (filename, content, "application/octet-stream")},
        )

    def q(self, sql, params=()):
        conn = sqlite3.connect(LIVE_DB)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()


class SuccessfulRestoreTests(RestoreTestBase):
    def test_restore_preserves_samples_report_and_version(self):
        cell = "RESTORE-OK"
        path = self.new_backup(cell, batches=[TWO_CYCLES, TWO_CYCLES])
        backup_rep = sqlite3.connect(path)
        backup_version = backup_rep.execute(
            "SELECT MAX(version) FROM report_versions WHERE cell_id=?", (cell,)
        ).fetchone()[0]
        backup_samples = backup_rep.execute(
            "SELECT ts, current_ma FROM samples WHERE cell_id=? ORDER BY ts", (cell,)
        ).fetchall()
        backup_rep.close()
        self.assertEqual(backup_version, 2)

        r = self.post_backup(cell, path)
        self.assertEqual(r.status_code, 201, r.text)
        body = r.json()
        self.assertEqual(body["cell_id"], cell)
        self.assertEqual(body["version"], 2)
        self.assertEqual(body["inserted"], 9)
        self.assertEqual(body["total_cycles"], 2)
        self.assertEqual(body["cycles_restored"], 2)

        rep = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(rep["version"], 2)
        self.assertEqual(rep["total_cycles"], 2)
        expected = detect_cycles([(ts, ma / 1e6) for ts, ma in backup_samples])
        self.assertEqual(len(rep["cycles"]), len(expected))
        for got, want in zip(rep["cycles"], expected):
            self.assertEqual(got["cycle_no"], want.cycle_no)
            self.assertEqual(got["started_at"], want.started_at)
            self.assertEqual(got["ended_at"], want.ended_at)
            self.assertAlmostEqual(got["charge_ah"], want.charge_ah, places=9)
            self.assertAlmostEqual(got["discharge_ah"], want.discharge_ah, places=9)

        restored_samples = self.q(
            "SELECT ts, current_ma FROM samples WHERE cell_id=? ORDER BY ts", (cell,)
        )
        self.assertEqual(restored_samples, backup_samples)

        # the cell appears in the list and other cells remain queryable
        listed = {x["cell_id"]: x for x in self.c.get("/cells").json()}
        self.assertIn(cell, listed)
        self.assertEqual(listed[cell]["version"], 2)

    def test_subsequent_upload_continues_from_original_version(self):
        cell = "RESTORE-CONTINUE"
        path = self.new_backup(cell, batches=[TWO_CYCLES, TWO_CYCLES])
        self.assertEqual(self.post_backup(cell, path).status_code, 201)

        # duplicate upload: idempotent, but a fresh version on top of version 2
        r = upload(self.c, cell, TWO_CYCLES)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["version"], 3)
        self.assertEqual(r.json()["inserted"], 0)
        self.assertEqual(r.json()["duplicate"], 9)

        # late new points are merged with restored samples and recomputed
        r = upload(
            self.c,
            cell,
            [(90, 1.0), (100, 0.0), (110, -1.0), (120, 0.0)],
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["version"], 4)
        self.assertEqual(r.json()["total_cycles"], 3)
        rep = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(rep["version"], 4)
        self.assertEqual([x["cycle_no"] for x in rep["cycles"]], [1, 2, 3])

    def test_restore_does_not_disturb_other_cells(self):
        other = "RESTORE-OTHER"
        self.assertEqual(
            upload(self.c, other, [(0, 0.0), (10, 1.0), (20, 0.0), (30, -1.0), (40, 0.0)]).status_code,
            200,
        )
        before = self.c.get(f"/cells/{other}/report").json()

        cell = "RESTORE-NEW"
        path = self.new_backup(cell, extra_cell=(other, [(500, 0.0)]))
        self.assertEqual(self.post_backup(cell, path).status_code, 201)

        after = self.c.get(f"/cells/{other}/report").json()
        self.assertEqual(after, before)
        self.assertEqual(self.c.get(f"/cells/{cell}/report").status_code, 200)

    def test_backup_without_cycles_restores_empty_report(self):
        cell = "RESTORE-NOCYCLES"
        # samples that do not form a cycle; the backup's report says 0 cycles
        path = self.new_backup(cell, batches=[[(0, 0.0), (10, 1.0), (20, 0.0)]])
        r = self.post_backup(cell, path)
        self.assertEqual(r.status_code, 201, r.text)
        rep = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(rep["version"], 1)
        self.assertEqual(rep["total_cycles"], 0)
        self.assertEqual(rep["cycles"], [])
        # late point can still complete a cycle on top of restored samples
        r = upload(self.c, cell, [(30, -1.0), (40, 0.0)])
        self.assertEqual(r.json()["version"], 2)
        self.assertEqual(r.json()["total_cycles"], 1)


class RejectInvalidBackupTests(RestoreTestBase):
    def assert_rejected(self, cell, path, status=422):
        r = self.post_backup(cell, path)
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(
            self.q("SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,))[0][0], 0
        )
        self.assertEqual(self.c.get(f"/cells/{cell}/report").status_code, 404)

    def test_random_bytes(self):
        path = os.path.join(self.tmp, "junk.db")
        with open(path, "wb") as fh:
            fh.write(b"this is definitely not sqlite" * 100)
        self.assert_rejected("RESTORE-JUNK", path)

    def test_empty_file(self):
        path = os.path.join(self.tmp, "empty.db")
        open(path, "wb").close()
        r = self.post_backup("RESTORE-EMPTY", path)
        self.assertEqual(r.status_code, 400, r.text)

    def test_oversized_file_rejected_without_writing(self):
        import app.main as main_module

        cell = "RESTORE-BIG"
        path = self.new_backup(cell)
        old_limit = main_module.MAX_RESTORE_BYTES
        main_module.MAX_RESTORE_BYTES = 100
        try:
            r = self.post_backup(cell, path)
        finally:
            main_module.MAX_RESTORE_BYTES = old_limit
        self.assertEqual(r.status_code, 413, r.text)
        self.assertEqual(self.c.get(f"/cells/{cell}/report").status_code, 404)

    def test_corrupt_pages_fail_integrity_check(self):
        cell = "RESTORE-CORRUPT"
        path = self.new_backup(cell)
        with open(path, "r+b") as fh:
            fh.seek(4096)  # beyond the header, inside page 2
            blob = fh.read(512)
            fh.seek(4096)
            fh.write(bytes(b ^ 0xFF for b in blob))
        self.assert_rejected(cell, path)

    def test_truncated_database(self):
        cell = "RESTORE-TRUNC"
        path = self.new_backup(cell)
        size = os.path.getsize(path)
        truncated = os.path.join(self.tmp, "truncated.db")
        with open(path, "rb") as src, open(truncated, "wb") as dst:
            dst.write(src.read(max(2048, size // 3)))
        r = self.post_backup(cell, truncated)
        self.assertEqual(r.status_code, 422, r.text)

    def test_missing_cell(self):
        path = self.new_backup("SOMEONE-ELSE")
        self.assert_rejected("RESTORE-ABSENT", path)

    def test_missing_samples_for_cell(self):
        cell = "RESTORE-NOSAMPLES"
        path = self.new_backup(cell)
        conn = sqlite3.connect(path)
        conn.execute("DELETE FROM samples WHERE cell_id=?", (cell,))
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)

    def test_missing_cycle_detail(self):
        cell = "RESTORE-NOCYCLE"
        path = self.new_backup(cell)
        conn = sqlite3.connect(path)
        conn.execute(
            "DELETE FROM cycles WHERE cell_id=? AND version=2 AND cycle_no=2", (cell,)
        )
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)

    def test_missing_report(self):
        cell = "RESTORE-NOREPORT"
        path = self.new_backup(cell)
        conn = sqlite3.connect(path)
        conn.execute("DELETE FROM report_versions WHERE cell_id=?", (cell,))
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)

    def test_wrong_total_cycles(self):
        cell = "RESTORE-BADTOTAL"
        path = self.new_backup(cell)
        conn = sqlite3.connect(path)
        conn.execute(
            "UPDATE report_versions SET total_cycles=3 WHERE cell_id=? AND version=2",
            (cell,),
        )
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)

    def test_tampered_capacity(self):
        cell = "RESTORE-BADAH"
        path = self.new_backup(cell)
        conn = sqlite3.connect(path)
        conn.execute(
            "UPDATE cycles SET charge_ah=charge_ah+0.5 WHERE cell_id=?"
            " AND version=2 AND cycle_no=1",
            (cell,),
        )
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)

    def test_tampered_boundaries(self):
        cell = "RESTORE-BADTS"
        path = self.new_backup(cell)
        conn = sqlite3.connect(path)
        conn.execute(
            "UPDATE cycles SET ended_at=ended_at+1 WHERE cell_id=?"
            " AND version=2 AND cycle_no=1",
            (cell,),
        )
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)

    def test_samples_rewritten_but_report_untouched(self):
        # report still says 2 cycles, but raw samples no longer form them
        cell = "RESTORE-SAMPLEEDIT"
        path = self.new_backup(cell)
        conn = sqlite3.connect(path)
        conn.execute(
            "UPDATE samples SET current_ma=0 WHERE cell_id=? AND ts IN (30,70)",
            (cell,),
        )
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)

    def test_missing_schema(self):
        cell = "RESTORE-SCHEMA"
        path = os.path.join(self.tmp, "schema.db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE unrelated(x INTEGER)")
        conn.execute("INSERT INTO unrelated VALUES(1)")
        conn.commit()
        conn.close()
        self.assert_rejected(cell, path)


class ConflictTests(RestoreTestBase):
    def test_existing_cell_restore_rejected(self):
        cell = "RESTORE-EXISTING"
        upload(self.c, cell, [(0, 0.0), (10, 1.0)])
        before = self.c.get(f"/cells/{cell}/report").json()
        path = self.new_backup(cell)
        r = self.post_backup(cell, path)
        self.assertEqual(r.status_code, 409, r.text)
        after = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(after, before)
        self.assertEqual(
            self.q("SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,))[0][0], 2
        )

    def test_second_restore_of_same_cell_rejected(self):
        cell = "RESTORE-TWICE"
        path = self.new_backup(cell)
        self.assertEqual(self.post_backup(cell, path).status_code, 201)
        first = self.c.get(f"/cells/{cell}/report").json()
        r = self.post_backup(cell, path)
        self.assertEqual(r.status_code, 409, r.text)
        self.assertEqual(self.c.get(f"/cells/{cell}/report").json(), first)

    def test_different_backup_same_cell_rejected_without_merge(self):
        cell = "RESTORE-DIFF"
        path1 = self.new_backup(cell)
        self.assertEqual(self.post_backup(cell, path1).status_code, 201)
        v1 = self.c.get(f"/cells/{cell}/report").json()

        other_batch = [(1000, 0.0), (1010, 1.0), (1020, 0.0), (1030, -1.0), (1040, 0.0)]
        path2 = self.new_backup(cell, batches=[other_batch])
        r = self.post_backup(cell, path2)
        self.assertEqual(r.status_code, 409, r.text)
        self.assertEqual(self.c.get(f"/cells/{cell}/report").json(), v1)
        self.assertEqual(
            self.q("SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,))[0][0], 9
        )


class AtomicityTests(RestoreTestBase):
    def test_failure_mid_insert_leaves_no_partial_data(self):
        cell = "RESTORE-ATOMIC"
        path = self.new_backup(cell)
        conn = sqlite3.connect(LIVE_DB)
        conn.execute(
            "CREATE TRIGGER fail_restore BEFORE INSERT ON cycles"
            " WHEN NEW.cell_id = 'RESTORE-ATOMIC'"
            " BEGIN SELECT RAISE(ABORT, 'simulated disk failure'); END"
        )
        conn.commit()
        conn.close()
        try:
            r = self.post_backup(
                cell,
                path,
                client=TestClient(app, raise_server_exceptions=False),
            )
            self.assertEqual(r.status_code, 500, r.text)
            self.assertEqual(
                self.q("SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,))[0][0],
                0,
            )
            self.assertEqual(
                self.q("SELECT COUNT(*) FROM cells WHERE cell_id=?", (cell,))[0][0], 0
            )
            self.assertEqual(
                self.q(
                    "SELECT COUNT(*) FROM report_versions WHERE cell_id=?", (cell,)
                )[0][0],
                0,
            )
            self.assertEqual(self.c.get(f"/cells/{cell}/report").status_code, 404)
        finally:
            conn = sqlite3.connect(LIVE_DB)
            conn.execute("DROP TRIGGER IF EXISTS fail_restore")
            conn.commit()
            conn.close()

        # retry succeeds cleanly once the fault is gone
        r = self.post_backup(cell, path)
        self.assertEqual(r.status_code, 201, r.text)
        self.assertEqual(self.c.get(f"/cells/{cell}/report").json()["total_cycles"], 2)


class InterleavingTests(RestoreTestBase):
    def test_restore_racing_first_upload_exactly_one_wins(self):
        cell = "RESTORE-RACE-UPLOAD"
        path = self.new_backup(cell)
        outcomes = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def do_restore():
            barrier.wait()
            r = self.post_backup(cell, path)
            with lock:
                outcomes.append(("restore", r.status_code, r.json()))

        def do_upload():
            barrier.wait()
            r = upload(self.c, cell, [(900, 0.0), (910, 1.0), (920, 0.0), (930, -1.0), (940, 0.0)])
            with lock:
                outcomes.append(("upload", r.status_code, r.json()))

        threads = [
            threading.Thread(target=do_restore),
            threading.Thread(target=do_upload),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        statuses = sorted(x[1] for x in outcomes)
        # Restoration validates the backup before taking the write lock, so
        # either side may reach the database first; exactly one must win.
        self.assertIn(statuses, ([200, 409], [201, 200]), outcomes)
        rep = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(rep["total_cycles"], len(rep["cycles"]))
        if any(x[0] == "restore" and x[1] == 201 for x in outcomes):
            # restore won: upload appended exactly one committed version
            self.assertEqual(rep["version"], 3)
            self.assertEqual(rep["total_cycles"], 3)
            sample_count = self.q(
                "SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,)
            )[0][0]
            self.assertEqual(sample_count, 14)
        else:
            # upload won: restore refused and upload's version is 1
            self.assertEqual(rep["version"], 1)
            self.assertEqual(rep["total_cycles"], 1)
            self.assertEqual(
                self.q("SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,))[0][0],
                5,
            )
            self.assertEqual(
                self.q(
                    "SELECT COUNT(*) FROM report_versions WHERE cell_id=?", (cell,)
                )[0][0],
                1,
            )

    def test_concurrent_restores_and_other_cell_traffic(self):
        errors = []
        barrier = threading.Barrier(4)
        cell = "RESTORE-CONC-SAME"
        # build both upload payloads serially so the race is only on restore
        path_a = os.path.join(self.tmp, "backup-conc-a.db")
        path_b = os.path.join(self.tmp, "backup-conc-b.db")
        make_backup(path_a, cell, [TWO_CYCLES, TWO_CYCLES])
        make_backup(path_b, cell, [TWO_CYCLES, TWO_CYCLES])
        for p in (path_a, path_b):
            scrub_sidecars(p)

        def restore_same(path):
            try:
                barrier.wait()
                r = self.post_backup(cell, path)
                if r.status_code not in (201, 409):
                    errors.append(f"restore: {r.status_code} {r.text}")
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        def traffic(n):
            try:
                barrier.wait()
                other = f"RESTORE-CONC-OTHER-{n}"
                r = upload(
                    self.c,
                    other,
                    [(0, 0.0), (10, 1.0), (20, 0.0), (30, -1.0), (40, 0.0)],
                )
                if r.status_code != 200:
                    errors.append(f"traffic {n}: {r.status_code} {r.text}")
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        threads = [
            threading.Thread(target=restore_same, args=(path_a,)),
            threading.Thread(target=restore_same, args=(path_b,)),
            threading.Thread(target=traffic, args=(0,)),
            threading.Thread(target=traffic, args=(1,)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        rep = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(rep["version"], 2)
        self.assertEqual(rep["total_cycles"], 2)
        self.assertEqual(
            self.q("SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,))[0][0], 9
        )
        self.assertEqual(
            self.q(
                "SELECT COUNT(*) FROM report_versions WHERE cell_id=?", (cell,)
            )[0][0],
            1,
        )
        for n in range(2):
            other = self.c.get(f"/cells/RESTORE-CONC-OTHER-{n}/report").json()
            self.assertEqual(other["total_cycles"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
