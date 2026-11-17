"""FastAPI application: battery cell sample upload and cycle reports."""

from __future__ import annotations

import os
import shutil
import tempfile

from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from .backup_restore import BackupInvalidError, read_backup_cell
from .csv_import import CSVParseError, parse_samples_csv
from .db import (
    ConflictError,
    Database,
    ImportError_,
    RestoreConflictError,
)

DB_PATH = os.environ.get("BATTERY_DB_PATH", "/data/battery.db")
MAX_RESTORE_BYTES = int(
    os.environ.get("BATTERY_MAX_RESTORE_BYTES", str(512 * 1024 * 1024))
)
RESTORE_CHUNK_BYTES = 1024 * 1024

app = FastAPI(
    title="Battery Cell Cycle Capacity API",
    version="1.0.0",
    description=(
        "Upload UTC-second current samples per cell. Zero current marks "
        "segment boundaries; only zero→positive→zero→negative→zero sequences "
        "count as cycles. Gaps over 60 s truncate a sequence. Ah is the "
        "left-value current integral over each interval."
    ),
)
db = Database(DB_PATH)


class CycleOut(BaseModel):
    cycle_no: int
    charge_ah: float
    discharge_ah: float
    started_at: int
    ended_at: int


class ReportOut(BaseModel):
    cell_id: str
    version: int
    total_cycles: int
    created_at: str | None
    cycles: list[CycleOut]


class UploadOut(BaseModel):
    cell_id: str
    inserted: int
    duplicate: int
    version: int
    total_cycles: int


class RestoreOut(BaseModel):
    cell_id: str
    inserted: int
    version: int
    total_cycles: int
    cycles_restored: int
    skipped_older_versions: int


async def _spool_upload(file: UploadFile) -> str:
    """Copy an uploaded backup into a private temp dir, enforcing a size cap."""
    tmp_dir = tempfile.mkdtemp(prefix="battery-restore-")
    path = os.path.join(tmp_dir, "backup.db")
    total = 0
    try:
        with open(path, "wb") as out:
            while True:
                chunk = await file.read(RESTORE_CHUNK_BYTES)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_RESTORE_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=(
                            "backup file too large: limit is "
                            f"{MAX_RESTORE_BYTES} bytes"
                        ),
                    )
                out.write(chunk)
        if total == 0:
            raise HTTPException(status_code=400, detail="empty backup file")
        return path
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


@app.post(
    "/cells/{cell_id}/samples",
    response_model=UploadOut,
    tags=["upload"],
    summary="Upload a CSV batch of (UTC seconds, current A) samples",
)
async def upload_samples(cell_id: str, file: UploadFile = File(...)):
    content = await file.read()
    try:
        rows = parse_samples_csv(content)
        result = db.import_batch(cell_id, rows)
    except CSVParseError as exc:
        raise HTTPException(status_code=400, detail=f"CSV error: {exc}") from exc
    except ImportError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                f"batch rejected, no data changed: same timestamp {exc.ts} "
                f"already stored with {exc.existing} A, incoming {exc.incoming} A"
            ),
        ) from exc
    return result


@app.post(
    "/cells/{cell_id}/restore",
    response_model=RestoreOut,
    status_code=201,
    tags=["restore"],
    summary="Restore one cell from an uploaded backup SQLite database",
    description=(
        "Validates the uploaded backup (SQLite integrity, schema, presence of "
        "the cell) and replays its raw samples with the current cycle "
        "detector. The recomputed cycle count, boundaries and capacities must "
        "match the latest report stored in the backup. On success, samples, "
        "the latest report and its original version number are inserted "
        "atomically. The restore is rejected if the cell already exists. Any "
        "rejection leaves the live database unchanged."
    ),
)
async def restore_cell(cell_id: str, file: UploadFile = File(...)):
    path = await _spool_upload(file)
    try:
        # Integrity checking and sample replay are CPU/disk bound; keep them
        # off the event loop so concurrent uploads and queries stay responsive.
        backup = await run_in_threadpool(read_backup_cell, path, cell_id)
        result = await run_in_threadpool(db.restore_cell, backup)
    except BackupInvalidError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"backup rejected, no data changed: {exc}",
        ) from exc
    except RestoreConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"restore rejected, no data changed: {exc}",
        ) from exc
    except ImportError_ as exc:
        raise HTTPException(
            status_code=400,
            detail=f"restore rejected, no data changed: {exc}",
        ) from exc
    finally:
        shutil.rmtree(os.path.dirname(path), ignore_errors=True)
    return result


@app.get("/cells", tags=["query"], summary="List all cells with latest report info")
def list_cells():
    return db.list_cells()


@app.get(
    "/cells/{cell_id}/report",
    response_model=ReportOut,
    tags=["query"],
    summary="Latest committed cycle report for one cell",
)
def get_report(cell_id: str):
    report = db.get_report(cell_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"unknown cell {cell_id}")
    return report


@app.get("/health", tags=["ops"], summary="Liveness probe")
def health():
    return {"status": "ok"}
