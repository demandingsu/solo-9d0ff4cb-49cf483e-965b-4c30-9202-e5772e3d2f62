"""Live-server smoke test.

Runs against an already-started uvicorn (default http://127.0.0.1:8000).
Covers: late-point rewrite/renumbering, gap truncation, idempotency and
conflict rejection, concurrent uploads/queries with snapshot consistency,
and single-cell restore from an uploaded backup database.
"""

import concurrent.futures
import os
import sqlite3
import sys
import tempfile
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"

BACKUP_SCHEMA = """
CREATE TABLE cells (
    cell_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT '2026-01-01T00:00:00.000Z',
    updated_at TEXT NOT NULL DEFAULT '2026-01-01T00:00:00.000Z'
);
CREATE TABLE samples (
    cell_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    current_ma INTEGER NOT NULL,
    PRIMARY KEY (cell_id, ts)
);
CREATE TABLE report_versions (
    cell_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    total_cycles INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT '2026-01-01T00:00:00.000Z',
    PRIMARY KEY (cell_id, version)
);
CREATE TABLE cycles (
    cell_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    cycle_no INTEGER NOT NULL,
    charge_ah REAL NOT NULL,
    discharge_ah REAL NOT NULL,
    started_at INTEGER NOT NULL,
    ended_at INTEGER NOT NULL,
    PRIMARY KEY (cell_id, version, cycle_no)
);
CREATE TABLE upload_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cell_id TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT '2026-01-01T00:00:00.000Z'
);
"""


def build_backup(cell, samples, total_cycles, cycles):
    """Write a self-contained backup file: samples + one latest report."""
    fd, path = tempfile.mkstemp(suffix=".db", prefix="battery-backup-")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(BACKUP_SCHEMA)
    conn.execute("INSERT INTO cells(cell_id) VALUES(?)", (cell,))
    conn.executemany(
        "INSERT INTO samples(cell_id, ts, current_ma) VALUES(?,?,?)",
        [(cell, ts, round(cur * 1_000_000)) for ts, cur in samples],
    )
    conn.execute(
        "INSERT INTO report_versions(cell_id, version, total_cycles) VALUES(?,?,?)",
        (cell, 4, total_cycles),
    )
    conn.executemany(
        "INSERT INTO cycles(cell_id, version, cycle_no, charge_ah, discharge_ah,"
        " started_at, ended_at) VALUES(?,?,?,?,?,?,?)",
        [
            (cell, 4, no, ch, dis, start, end)
            for no, ch, dis, start, end in cycles
        ],
    )
    conn.commit()
    conn.close()
    return path


def upload(client, cell, rows, header="timestamp,current"):
    text = header + "\n" + "".join(f"{ts},{cur}\n" for ts, cur in rows)
    return client.post(
        f"{BASE}/cells/{cell}/samples",
        files={"file": ("batch.csv", text, "text/csv")},
    )


def restore(client, cell, path):
    with open(path, "rb") as fh:
        return client.post(
            f"{BASE}/cells/{cell}/restore",
            files={"file": ("backup.db", fh.read(), "application/octet-stream")},
        )


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print(f"  PASS: {msg}")


def main():
    with httpx.Client(timeout=30) as c:
        r = c.get(f"{BASE}/health")
        check(r.status_code == 200, "health endpoint")

        # --- 1. late point rewrites/renumbers cycles --------------------
        cell = "LIVE-LATE"
        r = upload(c, cell, [(0, 0), (10, 1), (20, 0), (30, -1), (40, 0), (50, 1)])
        check(r.status_code == 200, "first batch accepted")
        check(r.json()["total_cycles"] == 1, "one closed cycle, tail ignored")
        v1 = r.json()["version"]

        r = upload(c, cell, [(60, 0), (70, -1), (80, 0)])
        check(r.status_code == 200, "late batch accepted")
        check(r.json()["version"] == v1 + 1, "report version bumped")
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 2, "late point closed second cycle")
        check([x["cycle_no"] for x in rep["cycles"]] == [1, 2], "cycles renumbered 1..N")
        check(
            abs(rep["cycles"][0]["charge_ah"] - 1 * 10 / 3600) < 1e-12,
            "charge Ah from left-value integral",
        )

        # --- 2. gap > 60 s truncates ------------------------------------
        cell = "LIVE-GAP"
        r = upload(c, cell, [(0, 0), (10, 2), (71, 0), (80, -3), (90, 0)])
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 0, "61 s gap truncates the sequence")

        r = upload(c, cell, [(0, 0), (60, 2), (120, 0), (130, -1), (140, 0)])
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 1, "exactly-60 s intervals are kept")

        # --- 3. idempotency + conflict whole-batch rejection ------------
        cell = "LIVE-IDEM"
        rows = [(0, 0), (10, 1.5), (20, 0), (30, -2.5), (40, 0)]
        upload(c, cell, rows)
        r = upload(c, cell, rows)
        check(r.json()["inserted"] == 0 and r.json()["duplicate"] == 5,
              "identical re-upload is idempotent")
        before = c.get(f"{BASE}/cells/{cell}/report").json()
        r = upload(c, cell, [(10, 9.9), (99, 1)])
        check(r.status_code == 409, "different value at same ts -> 409")
        after = c.get(f"{BASE}/cells/{cell}/report").json()
        check(after["version"] == before["version"], "rejected batch keeps old version")
        check(after["total_cycles"] == before["total_cycles"], "old report unchanged")

        # --- 4. concurrency: 8 writers, parallel readers ----------------
        cell = "LIVE-CONC"

        def writer(n):
            base = n * 1000
            with httpx.Client(timeout=30) as wc:
                return upload(
                    wc, cell,
                    [(base, 0), (base + 10, 1), (base + 20, 0),
                     (base + 30, -1), (base + 40, 0)],
                ).status_code

        def reader(_):
            bad = []
            with httpx.Client(timeout=30) as rc:
                for _ in range(40):
                    rr = rc.get(f"{BASE}/cells/{cell}/report")
                    if rr.status_code == 404:
                        continue
                    rep = rr.json()
                    nos = [x["cycle_no"] for x in rep["cycles"]]
                    if rep["total_cycles"] != len(rep["cycles"]) or nos != list(
                        range(1, rep["total_cycles"] + 1)
                    ):
                        bad.append(rep)
            return bad

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            f_w = [pool.submit(writer, n) for n in range(8)]
            f_r = [pool.submit(reader, n) for n in range(8)]
            codes = [f.result() for f in f_w]
            inconsistencies = [x for f in f_r for x in f.result()]
        check(codes == [200] * 8, f"all 8 concurrent writers committed (got {codes})")
        check(not inconsistencies, "every query saw one internally consistent state")
        final = c.get(f"{BASE}/cells/{cell}/report").json()
        check(final["total_cycles"] == 8, "final report contains all 8 cycles")

        cells = c.get(f"{BASE}/cells").json()
        check(any(x["cell_id"] == cell for x in cells), "cell list endpoint")

        # --- 5. restore one cell from a backup database ------------------
        cell = "LIVE-RESTORE"
        samples = [(0, 0), (10, 2), (20, 0), (30, -3), (40, 0),
                   (50, 1), (60, 0), (70, -1.5), (80, 0)]
        cycles = [
            (1, 2 * 10 / 3600, 3 * 10 / 3600, 0, 40),
            (2, 1 * 10 / 3600, 1.5 * 10 / 3600, 40, 80),
        ]
        backup = build_backup(cell, samples, 2, cycles)
        try:
            r = restore(c, cell, backup)
            check(r.status_code == 201, f"backup restored (got {r.status_code})")
            check(r.json()["version"] == 4, "original report version preserved")
            rep = c.get(f"{BASE}/cells/{cell}/report").json()
            check(rep["version"] == 4 and rep["total_cycles"] == 2,
                  "restored report is queryable with the backup's version")
            check([x["cycle_no"] for x in rep["cycles"]] == [1, 2],
                  "restored cycle details complete and ordered")
            check(abs(rep["cycles"][1]["discharge_ah"] - 1.5 * 10 / 3600) < 1e-12,
                  "restored capacity equals recomputed capacity")

            r = upload(c, cell, [(90, 1), (100, 0), (110, -1), (120, 0)])
            check(r.status_code == 200 and r.json()["version"] == 5,
                  "later upload continues from the restored version")
            rep = c.get(f"{BASE}/cells/{cell}/report").json()
            check(rep["total_cycles"] == 3, "continued upload recomputes on top")

            r = restore(c, cell, backup)
            check(r.status_code == 409, "second restore of the same cell -> 409")
            check(c.get(f"{BASE}/cells/{cell}/report").json()["version"] == 5,
                  "rejected restore leaves cell unchanged")

            bad = os.path.join(tempfile.gettempdir(), "not-a-backup.db")
            with open(bad, "wb") as fh:
                fh.write(b"definitely not sqlite" * 40)
            try:
                r = restore(c, "LIVE-RESTORE-CORRUPT", bad)
                check(r.status_code == 422, "corrupt backup -> 422")
                check(c.get(f"{BASE}/cells/LIVE-RESTORE-CORRUPT/report").status_code == 404,
                      "corrupt backup leaves no cell behind")
            finally:
                os.remove(bad)
        finally:
            os.remove(backup)

    print("\nALL LIVE CHECKS PASSED")


if __name__ == "__main__":
    for attempt in range(20):
        try:
            httpx.get(f"{BASE}/health", timeout=2)
            break
        except httpx.HTTPError:
            time.sleep(0.5)
    main()
