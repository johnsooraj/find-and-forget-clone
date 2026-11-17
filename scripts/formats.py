"""Formats: the ONLY code that knows the input and report JSON layouts.

To change a format (see docs/GUIDE.md, "Changing the input/output format"):
  1. edit the matching file in examples/
  2. edit the function below
  3. run pytest
"""
import json
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from common import now

SCHEMA_VERSION = "3"
SUPPORTED_SCHEMA_VERSIONS = {"3"}
MATCH_TYPES = ("EMAIL", "PHONE", "NAME", "ADDRESS")
MATCH, NO_MATCH, ERROR = "MATCH", "NO_MATCH", "ERROR"
IN_PROGRESS, COMPLETE, INCOMPLETE = "IN_PROGRESS", "COMPLETE", "INCOMPLETE"
OK, FAILED = "OK", "FAILED"
HASH = re.compile(r"[0-9A-Fa-f]{64}")
RUN_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
ETAG = re.compile(r'"[A-Fa-f0-9]+(?:-[0-9]+)?"')
MAX_REQUESTS = 100_000


# ----------------------------------------------------------------------------- internal structures
@dataclass
class Request:
    """One input item. `error` is set when the item itself is invalid."""

    index: int
    run_id: object  # echoed back exactly as given
    hash_value: object  # echoed back exactly as given
    match_type: object  # echoed back exactly as given (uppercased when valid)
    hash_key: Optional[str] = None  # lowercase hex, set when valid
    error: Optional[str] = None


@dataclass
class FileHits:
    """Everything to delete from one part file (one line of files.jsonl)."""

    uri: str
    etag: str
    version_id: Optional[str]
    tran_ids: Set[str] = field(default_factory=set)
    run_ids: Set[str] = field(default_factory=set)


# ----------------------------------------------------------------------------- input
def parse_input(document) -> List[Request]:
    """Input: [{"run_id", "hash_value", "match_type"}, ...].

    A wrong overall shape fails the run. A bad item becomes an ERROR result
    and the other items are still processed.
    """
    if not isinstance(document, list):
        raise ValueError("Input must be a JSON array of {run_id, hash_value, match_type}")
    if not document:
        raise ValueError("Input array is empty")
    if len(document) > MAX_REQUESTS:
        raise ValueError(f"At most {MAX_REQUESTS} items per batch")
    requests = []
    for index, item in enumerate(document):
        if not isinstance(item, dict):
            requests.append(Request(index, None, None, None, error="Item must be a JSON object"))
            continue
        run_id, hash_value, match_type = item.get("run_id"), item.get("hash_value"), item.get("match_type")
        request = Request(index, run_id, hash_value, match_type)
        if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
            request.error = "run_id must be a string such as a UUID"
        elif not isinstance(hash_value, str) or not HASH.fullmatch(hash_value.strip()):
            request.error = "hash_value must be a 64-character hex SHA-256"
        elif not isinstance(match_type, str) or match_type.strip().upper() not in MATCH_TYPES:
            request.error = "match_type must be EMAIL, PHONE, NAME or ADDRESS"
        else:
            request.hash_key = hash_value.strip().lower()
            request.match_type = match_type.strip().upper()
        requests.append(request)
    return requests


# ----------------------------------------------------------------------------- batch.json (find, first run)
def build_batch(batch_id, input_uri, input_etag, archive_uri, month_from, month_to, hash_mode,
                files_total, skipped_folders) -> dict:
    """Fixed settings of a batch. Later runs must use the same values."""
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "created_at": now(),
        "input_uri": input_uri,
        "input_etag": input_etag,
        "archive_uri": archive_uri,
        "month_from": month_from,
        "month_to": month_to,
        "hash_mode": hash_mode,
        "files_total": files_total,
        "skipped_folders": skipped_folders,
    }


def parse_batch(document, batch_id) -> dict:
    if not isinstance(document, dict):
        raise ValueError("batch.json must be a JSON object")
    _check_schema(document, "batch.json")
    if document.get("batch_id") != batch_id:
        raise ValueError("batch.json batch_id does not match")
    for key in ("input_uri", "input_etag", "archive_uri", "hash_mode", "files_total"):
        if key not in document:
            raise ValueError(f"batch.json is missing {key}")
    return document


# ----------------------------------------------------------------------------- selected_files.jsonl
def selected_line(uri) -> dict:
    return {"uri": uri}


def parse_selected_line(line) -> str:
    entry = json.loads(line)
    if not isinstance(entry, dict) or not isinstance(entry.get("uri"), str):
        raise ValueError("Invalid selected_files.jsonl line")
    return entry["uri"]


# ----------------------------------------------------------------------------- waves/wave-NNNNN.jsonl
def wave_line(outcome) -> dict:
    """One scanned file. outcome comes from tasks.scan_task."""
    return {
        "schema_version": SCHEMA_VERSION,
        "uri": outcome["uri"],
        "status": outcome["status"],
        "etag": outcome.get("etag"),
        "version_id": outcome.get("version_id"),
        "bytes": outcome.get("bytes"),
        "rows": outcome.get("rows"),
        "seconds": outcome.get("seconds"),
        "error": outcome.get("error"),
        "matches": {str(index): sorted(ids) for index, ids in sorted(outcome.get("matches", {}).items())},
    }


def parse_wave_line(line, where) -> dict:
    """Return the outcome with matches keyed by input index (int)."""
    try:
        entry = json.loads(line)
    except json.JSONDecodeError:
        raise ValueError(f"{where}: invalid JSON") from None
    if not isinstance(entry, dict) or not isinstance(entry.get("uri"), str) or entry.get("status") not in (OK, FAILED):
        raise ValueError(f"{where}: invalid wave line")
    _check_schema(entry, where)
    matches = entry.get("matches") or {}
    entry["matches"] = {int(index): list(ids) for index, ids in matches.items()}
    return entry


# ----------------------------------------------------------------------------- results.json
def result_entry(request: Request, match_result: str, data_identifier: Dict[str, List[str]], error=None) -> dict:
    """One element of results.json. data_identifier: {s3 uri: [TRAN_ID, ...]}."""
    entry = {
        "run_id": request.run_id,
        "hash_value": request.hash_value,
        "match_type": request.match_type,
        "match_result": match_result,
        "match_count": sum(len(ids) for ids in data_identifier.values()),
        "data_identifier": {uri: sorted(ids) for uri, ids in sorted(data_identifier.items())},
    }
    if error:
        entry["error"] = error
    return entry


# ----------------------------------------------------------------------------- files.jsonl
def file_line(hits: FileHits) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "uri": hits.uri,
        "etag": hits.etag,
        "version_id": hits.version_id,
        "tran_ids": sorted(hits.tran_ids),
        "run_ids": sorted(hits.run_ids),
    }


def parse_file_line(line: str, number: int) -> FileHits:
    where = f"files.jsonl line {number}"
    try:
        entry = json.loads(line)
    except json.JSONDecodeError:
        raise ValueError(f"{where}: invalid JSON") from None
    if not isinstance(entry, dict):
        raise ValueError(f"{where}: expected an object")
    _check_schema(entry, where)
    uri, etag, version_id = entry.get("uri"), entry.get("etag"), entry.get("version_id")
    tran_ids, run_ids = entry.get("tran_ids"), entry.get("run_ids")
    if not isinstance(uri, str):
        raise ValueError(f"{where}: uri must be a string")
    if not isinstance(etag, str) or not ETAG.fullmatch(etag):
        raise ValueError(f"{where}: invalid etag")
    if version_id is not None and not isinstance(version_id, str):
        raise ValueError(f"{where}: invalid version_id")
    if not isinstance(tran_ids, list) or not tran_ids or any(not isinstance(t, str) or not t.strip() for t in tran_ids):
        raise ValueError(f"{where}: invalid tran_ids")
    if not isinstance(run_ids, list) or any(not isinstance(r, str) for r in run_ids):
        raise ValueError(f"{where}: invalid run_ids")
    return FileHits(uri, etag, version_id, set(tran_ids), set(run_ids))


# ----------------------------------------------------------------------------- summary.json (find)
def build_summary(batch_id, batch, status, files_done, failed_files, file_hits, results) -> dict:
    """Progress and counts shown by the Jenkins status pipeline. No customer data.

    While IN_PROGRESS, results is None and the request counts are null.
    """
    complete = results is not None
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "status": status,
        "generated_at": now(),
        "archive_uri": batch["archive_uri"],
        "month_from": batch.get("month_from"),
        "month_to": batch.get("month_to"),
        "hash_mode": batch["hash_mode"],
        "files_total": batch["files_total"],
        "files_done": files_done,
        "files_failed": len(failed_files),
        "failed_files": failed_files,
        "files_to_rewrite": len(file_hits),
        "rows_to_delete": sum(len(hits.tran_ids) for hits in file_hits),
        "requests_total": len(results) if complete else None,
        "requests_match": sum(r["match_result"] == MATCH for r in results) if complete else None,
        "requests_no_match": sum(r["match_result"] == NO_MATCH for r in results) if complete else None,
        "requests_error": sum(r["match_result"] == ERROR for r in results) if complete else None,
        "skipped_folders": batch.get("skipped_folders", []),
    }


def parse_summary(document, batch_id) -> dict:
    if not isinstance(document, dict):
        raise ValueError("summary.json must be a JSON object")
    _check_schema(document, "summary.json")
    if document.get("batch_id") != batch_id:
        raise ValueError("summary.json batch_id does not match")
    return document


# ----------------------------------------------------------------------------- remove_summary.json (forget)
def build_remove_summary(batch_id, purge, outcomes) -> dict:
    """outcomes: one per file in files.jsonl, status DONE / SKIPPED / FAILED / PENDING."""
    count = {s: sum(o["status"] == s for o in outcomes) for s in ("DONE", "SKIPPED", "FAILED", "PENDING")}
    if count["FAILED"]:
        status = INCOMPLETE
    elif count["PENDING"]:
        status = IN_PROGRESS
    else:
        status = COMPLETE
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "status": status,
        "generated_at": now(),
        "purge_old_versions": purge,
        "files_total": len(outcomes),
        "files_done": count["DONE"],
        "files_skipped": count["SKIPPED"],
        "files_failed": count["FAILED"],
        "files_pending": count["PENDING"],
        "files": outcomes,
    }


def _check_schema(document, what):
    version = document.get("schema_version", SCHEMA_VERSION)
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(f"Unsupported {what} schema_version {version!r}")
