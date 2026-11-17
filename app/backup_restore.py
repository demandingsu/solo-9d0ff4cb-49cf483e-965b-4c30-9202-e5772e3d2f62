"""Validate an untrusted SQLite backup and extract one cell's committed state.

The uploaded backup is copied to an isolated temporary directory by the HTTP
layer and never executed.  This module opens it strictly read-only/immutable,
runs SQLite's own integrity check, verifies the expected schema, then replays
the raw samples through the current cycle detector.  The replay must agree with
the backup's own latest committed report (count, cycle boundaries and Ah
capacity) before a snapshot is handed to :meth:`Database.restore_cell`.

A backup is rejected as a whole when it is not a database, is structurally
corrupt, lacks the requested cell/samples/report rows, or its latest report
does not match a fresh recomputation from the samples.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from .algorithm import detect_cycles

SQLITE_HEADER = b"SQLite format 3\x00"

# Integer report fields (counts, boundaries, version) must match exactly.
# Capacities are floats written by the reporting service; allow only a tiny
# round-off margin (1e-6 Ah == 1 µAh) so genuine backups pass while tampered
# capacity values are rejected.
ABS_TOL_AH = 1e-6
REL_TOL_AH = 1e-9

REQUIRED_COLUMNS: Dict[str, Set[str]] = {
    "cells": {"cell_id"},
    "samples": {"cell_id", "ts", "current_ma"},
    "report_versions": {"cell_id", "version", "total_cycles"},
    "cycles": {
        "cell_id",
        "version",
        "cycle_no",
        "charge_ah",
        "discharge_ah",
        "started_at",
        "ended_at",
    },
}


class BackupInvalidError(ValueError):
    """Uploaded file is not a trustworthy, self-consistent backup."""


@dataclass(frozen=True)
class BackupCycle:
    cycle_no: int
    charge_ah: float
    discharge_ah: float
    started_at: int
    ended_at: int


@dataclass(frozen=True)
class BackupCell:
    """Validated, still-untouched snapshot of one cell from a backup file."""

    cell_id: str
    samples: Tuple[Tuple[int, int], ...]  # (ts, microamps) exactly as stored
    version: int
    total_cycles: int
    cycles: Tuple[BackupCycle, ...]
    report_created_at: Optional[str]
    older_versions_skipped: int


def _fail(message: str) -> None:
    raise BackupInvalidError(message)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _check_sqlite_header(path: str) -> None:
    try:
        with open(path, "rb") as fh:
            header = fh.read(len(SQLITE_HEADER))
    except OSError as exc:
        raise BackupInvalidError(f"cannot read backup file: {exc}") from exc
    if header != SQLITE_HEADER:
        _fail("not a SQLite database file (bad header)")


def _open_read_only(path: str) -> sqlite3.Connection:
    uri = Path(path).resolve().as_uri() + "?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=10.0)
    conn.row_factory = sqlite3.Row
    # immutable=1 plus query_only means SQLite never writes the uploaded file
    # and never creates -wal/-shm side files next to it. trusted_schema=OFF
    # stops any embedded schema from invoking application-defined functions.
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA trusted_schema = OFF")
    return conn


def _check_integrity(conn: sqlite3.Connection) -> None:
    try:
        rows = conn.execute("PRAGMA integrity_check").fetchall()
    except sqlite3.Error as exc:
        raise BackupInvalidError(f"SQLite integrity check failed: {exc}") from exc
    messages = [str(row[0]) for row in rows]
    if messages != ["ok"]:
        preview = "; ".join(messages[:5]) or "no result"
        _fail(f"backup failed SQLite integrity check: {preview}")


def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    # `table` always comes from REQUIRED_COLUMNS, never from user input.
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = ?", (table,)
    ).fetchone()
    if row is None or row["type"] != "table":
        _fail(f"backup is missing table {table!r}")
    names = {
        r["name"] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    }
    missing = REQUIRED_COLUMNS[table] - names
    if missing:
        _fail(
            f"backup table {table!r} is missing column(s): "
            f"{', '.join(sorted(missing))}"
        )
    return names


def _read_samples(conn: sqlite3.Connection, cell_id: str) -> Tuple[Tuple[int, int], ...]:
    rows = conn.execute(
        "SELECT ts, current_ma FROM samples WHERE cell_id = ? ORDER BY ts ASC",
        (cell_id,),
    ).fetchall()
    if not rows:
        _fail(f"backup contains no samples for cell {cell_id!r}")

    samples: List[Tuple[int, int]] = []
    previous_ts: Optional[int] = None
    for row in rows:
        ts = row["ts"]
        current_ma = row["current_ma"]
        if not _is_int(ts) or ts < 0:
            _fail(f"backup contains an invalid sample timestamp {ts!r}")
        if not _is_int(current_ma):
            _fail(f"backup contains a non-integer current at ts={ts}")
        if previous_ts is not None and ts <= previous_ts:
            _fail(f"backup contains a duplicate or unsorted timestamp ts={ts}")
        samples.append((ts, current_ma))
        previous_ts = ts
    return tuple(samples)


def _read_latest_report(
    conn: sqlite3.Connection, cell_id: str, columns: Set[str]
) -> Tuple[int, int, Optional[str], int]:
    created_expr = "created_at" if "created_at" in columns else "NULL AS created_at"
    rows = conn.execute(
        "SELECT version, total_cycles, " + created_expr + " FROM report_versions"
        " WHERE cell_id = ? ORDER BY version DESC",
        (cell_id,),
    ).fetchall()
    if not rows:
        _fail(f"backup contains no report for cell {cell_id!r}")

    latest = rows[0]
    version = latest["version"]
    total_cycles = latest["total_cycles"]
    if not _is_int(version) or version < 1:
        _fail(f"backup has an invalid report version {version!r}")
    if not _is_int(total_cycles) or total_cycles < 0:
        _fail(f"backup has an invalid total_cycles {total_cycles!r}")

    created_at = latest["created_at"]
    if not isinstance(created_at, str):
        created_at = None
    return version, total_cycles, created_at, len(rows) - 1


def _read_cycles(
    conn: sqlite3.Connection, cell_id: str, version: int, total_cycles: int
) -> Tuple[BackupCycle, ...]:
    rows = conn.execute(
        "SELECT cycle_no, charge_ah, discharge_ah, started_at, ended_at"
        " FROM cycles WHERE cell_id = ? AND version = ? ORDER BY cycle_no ASC",
        (cell_id, version),
    ).fetchall()
    if len(rows) != total_cycles:
        _fail(
            f"latest report version {version} declares {total_cycles} cycle(s) "
            f"but backup has {len(rows)} detail row(s)"
        )

    cycles: List[BackupCycle] = []
    for row in rows:
        cycle_no = row["cycle_no"]
        charge_ah = row["charge_ah"]
        discharge_ah = row["discharge_ah"]
        started_at = row["started_at"]
        ended_at = row["ended_at"]
        if not _is_int(cycle_no) or cycle_no < 1:
            _fail(f"backup has an invalid cycle number {cycle_no!r}")
        if not _is_int(started_at) or not _is_int(ended_at):
            _fail(f"cycle {cycle_no} has non-integer boundaries")
        if not _is_number(charge_ah) or not _is_number(discharge_ah):
            _fail(f"cycle {cycle_no} has a non-numeric capacity")
        cycles.append(
            BackupCycle(
                cycle_no=cycle_no,
                charge_ah=float(charge_ah),
                discharge_ah=float(discharge_ah),
                started_at=started_at,
                ended_at=ended_at,
            )
        )

    numbers = [cycle.cycle_no for cycle in cycles]
    if numbers != list(range(1, total_cycles + 1)):
        _fail(
            f"latest report version {version} has non-contiguous cycle numbers"
        )
    return tuple(cycles)


def _verify_against_recomputation(
    samples: Sequence[Tuple[int, int]],
    cycles: Sequence[BackupCycle],
    total_cycles: int,
) -> None:
    # The service stores amperes as integer microamps (see db._to_storage).
    replayed = detect_cycles([(ts, current_ma / 1_000_000.0) for ts, current_ma in samples])
    if len(replayed) != total_cycles:
        _fail(
            f"recomputed {len(replayed)} cycle(s) from backup samples but the "
            f"latest report declares {total_cycles}"
        )

    for stored, computed in zip(cycles, replayed):
        if (
            stored.cycle_no != computed.cycle_no
            or stored.started_at != computed.started_at
            or stored.ended_at != computed.ended_at
        ):
            _fail(
                f"cycle {stored.cycle_no} boundaries differ: backup "
                f"{stored.started_at}..{stored.ended_at}, recomputed "
                f"{computed.started_at}..{computed.ended_at}"
            )
        for label, stored_ah, computed_ah in (
            ("charge", stored.charge_ah, computed.charge_ah),
            ("discharge", stored.discharge_ah, computed.discharge_ah),
        ):
            if not math.isclose(
                stored_ah, computed_ah, rel_tol=REL_TOL_AH, abs_tol=ABS_TOL_AH
            ):
                _fail(
                    f"cycle {stored.cycle_no} {label} capacity differs: backup "
                    f"{stored_ah} Ah, recomputed {computed_ah} Ah"
                )


def read_backup_cell(path: str, cell_id: str) -> BackupCell:
    """Validate *path* and return the committed state of *cell_id*.

    Raises :class:`BackupInvalidError` for any non-SQLite, corrupt, incomplete
    or self-inconsistent backup.  The file is only ever read.
    """
    _check_sqlite_header(path)
    try:
        conn = _open_read_only(path)
    except sqlite3.Error as exc:
        raise BackupInvalidError(f"cannot open backup database: {exc}") from exc

    try:
        _check_integrity(conn)
        columns = {table: _table_columns(conn, table) for table in REQUIRED_COLUMNS}

        if (
            conn.execute(
                "SELECT 1 FROM cells WHERE cell_id = ?", (cell_id,)
            ).fetchone()
            is None
        ):
            _fail(f"backup does not contain cell {cell_id!r}")

        samples = _read_samples(conn, cell_id)
        version, total_cycles, created_at, skipped = _read_latest_report(
            conn, cell_id, columns["report_versions"]
        )
        cycles = _read_cycles(conn, cell_id, version, total_cycles)
        _verify_against_recomputation(samples, cycles, total_cycles)

        return BackupCell(
            cell_id=cell_id,
            samples=samples,
            version=version,
            total_cycles=total_cycles,
            cycles=cycles,
            report_created_at=created_at,
            older_versions_skipped=skipped,
        )
    except sqlite3.Error as exc:
        # Malformed pages, missing tables, truncated files, etc.
        raise BackupInvalidError(f"cannot read backup database: {exc}") from exc
    finally:
        conn.close()
