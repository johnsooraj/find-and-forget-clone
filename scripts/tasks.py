"""Work done for ONE part file. Runs inside Spark tasks (or locally in tests).

  scan_task    (layer 2, find)   read one part once, ask the find strategy which rows match
  forget_task  (layer 3, forget) rewrite one part without the listed rows, same name,
                                 then run the forget strategy's post step

Both return a plain dict instead of raising, so one bad file never stops a
wave and Spark never retries a file that failed for a data reason. Both get
the strategy by NAME from `shared` (see find_strategies / forget_strategies).

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
import csv
import io
import tarfile
import tempfile
import time
from contextlib import closing
from typing import Dict, Set

import common
import find_strategies
import forget_strategies
import formats

MAX_PUT = 5_000_000_000
META_BATCH = "drop-glue-batch"
META_SOURCE_ETAG = "drop-glue-source-etag"

_clients = {}


def s3_for(shared):
    """The S3 client for this task: injected in tests, else one per executor process."""
    if shared.get("s3") is not None:
        return shared["s3"]
    region = shared["region"]
    if region not in _clients:
        _clients[region] = common.s3_client(region)
    return _clients[region]


# =============================================================================
# FIND
# =============================================================================
def scan_archive(s3, uri, match_row, match_shared):
    """Read one part once. Return (GET response, {match key: TRAN_IDs}, rows read)."""
    loc = common.location(uri)
    obj = s3.get_object(**loc)
    found: Dict[str, Set[str]] = {}
    rows = 0
    with closing(obj["Body"]) as body, common.archive_rows(body, loc["Key"]) as (_, reader):
        for row in common.records(reader):
            rows += 1
            for key in match_row(match_shared, row):
                found.setdefault(key, set()).add(row["TRAN_ID"])
    return obj, found, rows


def scan_task(shared, item):
    """Spark task for find. shared: find_strategy, match (strategy state), region. item: {"uri"}."""
    common.raise_csv_limit()
    started = time.time()
    outcome = {"uri": item["uri"]}
    try:
        strategy = find_strategies.FIND_STRATEGIES[shared["find_strategy"]]
        obj, found, rows = scan_archive(s3_for(shared), item["uri"], strategy.match_row, shared["match"])
        outcome.update(status=formats.OK, etag=obj["ETag"], version_id=obj.get("VersionId"),
                       bytes=obj.get("ContentLength"), rows=rows, matches=found)
    except Exception as exc:
        outcome.update(status=formats.FAILED, error=str(exc) or type(exc).__name__)
    outcome["seconds"] = round(time.time() - started, 2)
    return outcome


# =============================================================================
# FORGET
# =============================================================================
def keep_row(row, tran_ids):
    """The row filter. Swap this for another rule if needed."""
    return row["TRAN_ID"] not in tran_ids


def rewrite_archive(s3, hits, batch_id):
    """Replace one part without the listed rows: one GET, one PUT.

    Returns (rows removed, PUT response). Keeps the S3 key, the CSV name and
    tar metadata, the header and column order, the content type and user
    metadata, and tags the object with this batch so an interrupted run can
    recognise its own work. A part whose rows are all removed is kept with just
    the header row.
    """
    source = common.location(hits.uri)
    obj = s3.get_object(**source, IfMatch=hits.etag)
    found, removed = set(), 0
    with tempfile.TemporaryFile() as csv_file, tempfile.TemporaryFile() as archive:
        with closing(obj["Body"]) as body, common.archive_rows(body, source["Key"]) as (member, reader):
            stream = io.TextIOWrapper(csv_file, encoding="utf-8", newline="")
            try:
                writer = csv.DictWriter(stream, fieldnames=reader.fieldnames)
                writer.writeheader()
                for row in common.records(reader):
                    if keep_row(row, hits.tran_ids):
                        writer.writerow(row)
                    else:
                        found.add(row["TRAN_ID"])
                        removed += 1
                stream.flush()
            finally:
                stream.detach()
        if found != hits.tran_ids:
            raise ValueError(f"{len(hits.tran_ids - found)} listed transaction IDs are missing from the archive")
        replacement = tarfile.TarInfo(member.name)
        for attribute in ("mode", "mtime", "uid", "gid", "uname", "gname"):
            setattr(replacement, attribute, getattr(member, attribute))
        replacement.size = csv_file.tell()
        csv_file.seek(0)
        with tarfile.open(fileobj=archive, mode="w:gz") as bundle:
            bundle.addfile(replacement, csv_file)
        size = archive.tell()
        if size > MAX_PUT:
            raise ValueError("Replacement archive exceeds the single PUT limit")
        archive.seek(0)
        metadata = dict(obj.get("Metadata") or {})
        metadata[META_BATCH] = batch_id
        metadata[META_SOURCE_ETAG] = hits.etag.strip('"')
        extra = {"StorageClass": obj["StorageClass"]} if obj.get("StorageClass") else {}
        response = s3.put_object(
            **source,
            Body=archive,
            ContentLength=size,
            ContentType=obj.get("ContentType") or "application/gzip",
            Metadata=metadata,
            IfMatch=hits.etag,
            **extra,
        )
    return removed, response


def replaced_by_this_batch(s3, hits, batch_id):
    """After an ETag mismatch: was the part already replaced by this batch (a run
    stopped after the PUT but before the marker)? Return its HEAD response or None."""
    head = s3.head_object(**common.location(hits.uri))
    metadata = head.get("Metadata") or {}
    if metadata.get(META_BATCH) == batch_id and metadata.get(META_SOURCE_ETAG) == hits.etag.strip('"'):
        return head
    return None


def write_marker(s3, marker_uri, marker, create_only):
    common.put_json(s3, marker_uri, marker, create_only)


def post_step_done(marker, strategy):
    """Has the marker recorded this strategy's post step? (schema 3 markers: versions_purged)."""
    if not strategy.has_post_step:
        return True
    done = marker.get("post_replace_done") or []
    legacy = marker.get("versions_purged") and strategy.name == forget_strategies.PurgeOldVersions.name
    return strategy.name in done or bool(legacy)


def process_file(s3, hits, marker_uri, already_done, strategy, batch_id, outcome):
    """Rewrite one part (or finish an earlier attempt), run the strategy's post
    step and record a completion marker.

    The marker is written as soon as the object is replaced, so a later failure
    (e.g. while purging old versions) can be resumed without an ETag mismatch.
    """
    bucket, key = common.split_uri(hits.uri)
    if already_done:
        marker = common.get_json(s3, marker_uri)
        outcome["new_etag"], outcome["new_version_id"] = marker.get("new_etag"), marker.get("new_version_id")
        if post_step_done(marker, strategy):
            outcome["status"] = "SKIPPED"
            outcome["reason"] = "Already completed by an earlier run"
            return
        outcome["rows_removed"] = marker.get("rows_removed")
    else:
        try:
            removed, response = rewrite_archive(s3, hits, batch_id)
        except Exception as exc:
            if common.error_code(exc) not in ("PreconditionFailed", "412"):
                raise
            head = replaced_by_this_batch(s3, hits, batch_id)
            if head is None:
                raise
            removed, response = None, head
            outcome["recovered"] = True
        outcome["rows_removed"] = removed
        marker = {
            "uri": hits.uri,
            "rows_removed": removed,
            "source_version_id": hits.version_id,
            "new_etag": response.get("ETag"),
            "new_version_id": response.get("VersionId"),
            "post_replace_done": [],
            "completed_at": common.now(),
        }
        outcome["new_etag"], outcome["new_version_id"] = marker["new_etag"], marker["new_version_id"]
        write_marker(s3, marker_uri, marker, create_only=True)
    if strategy.has_post_step:
        outcome.update(strategy.after_replace(s3, bucket, key, marker.get("source_version_id", hits.version_id),
                                              marker.get("new_version_id")))
        marker["post_replace_done"] = sorted(set(marker.get("post_replace_done") or []) | {strategy.name})
        write_marker(s3, marker_uri, marker, create_only=False)
    outcome["status"] = "DONE"


def forget_task(shared, item):
    """Spark task for forget. shared: batch_id, forget_strategy, region.
    item: {"uri", "etag", "version_id", "tran_ids", "marker_uri", "already_done"}."""
    common.raise_csv_limit()
    started = time.time()
    hits = formats.FileHits(item["uri"], item["etag"], item["version_id"], set(item["tran_ids"]))
    outcome = {"uri": hits.uri, "rows_expected": len(hits.tran_ids)}
    try:
        strategy = forget_strategies.get(shared["forget_strategy"])
        process_file(s3_for(shared), hits, item["marker_uri"], item["already_done"], strategy,
                     shared["batch_id"], outcome)
    except Exception as exc:
        outcome["status"] = "FAILED"
        if common.error_code(exc) in ("PreconditionFailed", "412"):
            outcome["error"] = "The part changed since Find ran (ETag mismatch); run Find again with a new BATCH_ID"
        else:
            outcome["error"] = str(exc) or type(exc).__name__
    outcome["seconds"] = round(time.time() - started, 2)
    return outcome
