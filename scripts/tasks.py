"""Work done for ONE part file. Runs inside Spark tasks (or locally in tests).

  scan_task    (find)   read one part, hash its XML values, return the matches
  forget_task  (forget) rewrite one part without the listed rows, same name

Both return a plain dict instead of raising, so one bad file never stops a
wave and Spark never retries a file that failed for a data reason.

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
import csv
import hashlib
import io
import re
import tarfile
import tempfile
import time
import xml.etree.ElementTree as ET
from contextlib import closing
from typing import Dict, List, Set

import common
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
DEFAULT_TAGS = {
    "PHONE": "phone,phoneNumber",
    "EMAIL": "email,emailAddress",
    "NAME": "name,fullName",
    "ADDRESS": "address,fullAddress",
}


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_lookup(requests) -> Dict[str, Dict[str, List[int]]]:
    """match_type -> hash -> indexes of the valid input items with that hash."""
    lookup: Dict[str, Dict[str, List[int]]] = {}
    for request in requests:
        if not request.error:
            lookup.setdefault(request.match_type, {}).setdefault(request.hash_key, []).append(request.index)
    return lookup


def build_tag_map(tag_settings: Dict[str, str], match_types) -> Dict[str, Set[str]]:
    """XML local tag name (lowercase) -> match types it can hold."""
    tag_map: Dict[str, Set[str]] = {}
    for match_type in match_types:
        tags = {t.strip().lower() for t in tag_settings[match_type].split(",") if t.strip()}
        if not tags:
            raise ValueError(f"Configure at least one XML tag for {match_type}")
        for tag in tags:
            tag_map.setdefault(tag, set()).add(match_type)
    return tag_map


def find_hits(xml, tag_map, lookup, hash_mode) -> Set[int]:
    """Return the indexes of input items whose hash matches a value in this XML."""
    if not xml.strip():
        return set()
    if re.search(r"<!DOCTYPE", xml, re.IGNORECASE):
        raise ValueError("XML document types are unsupported")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        raise ValueError("Invalid XML in RAW_RESPONSE") from None
    hits: Set[int] = set()
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        match_types = tag_map.get(element.tag.rsplit("}", 1)[-1].lower())
        if not match_types:
            continue
        text = "".join(element.itertext())
        for match_type in match_types:
            hashes = lookup.get(match_type)
            value = common.normalize(text, match_type, hash_mode) if hashes else ""
            if value:
                hits.update(hashes.get(sha256_hex(value), ()))
    return hits


def scan_archive(s3, uri, tag_map, lookup, hash_mode):
    """Read one part once. Return (GET response, {item index: TRAN_IDs}, rows read)."""
    loc = common.location(uri)
    obj = s3.get_object(**loc)
    found: Dict[int, Set[str]] = {}
    rows = 0
    with closing(obj["Body"]) as body, common.archive_rows(body, loc["Key"]) as (_, reader):
        for row in common.records(reader):
            rows += 1
            for index in find_hits(row["RAW_RESPONSE"], tag_map, lookup, hash_mode):
                found.setdefault(index, set()).add(row["TRAN_ID"])
    return obj, found, rows


def scan_task(shared, item):
    """Spark task for find. shared: lookup, tag_map, hash_mode, region. item: {"uri"}."""
    common.raise_csv_limit()
    started = time.time()
    outcome = {"uri": item["uri"]}
    try:
        obj, found, rows = scan_archive(s3_for(shared), item["uri"], shared["tag_map"], shared["lookup"],
                                        shared["hash_mode"])
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


def purge_old_versions(s3, bucket, key, keep_version_id):
    """Delete every noncurrent version of key. Return the number deleted."""
    if not keep_version_id:
        return 0  # bucket is not versioned: nothing older is kept
    versions, key_marker, version_marker = [], None, None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": key}
        if key_marker:
            kwargs.update(KeyMarker=key_marker, VersionIdMarker=version_marker)
        page = s3.list_object_versions(**kwargs)
        versions += [v for v in page.get("Versions", []) if v["Key"] == key]
        if not page.get("IsTruncated"):
            break
        key_marker, version_marker = page["NextKeyMarker"], page["NextVersionIdMarker"]
    latest = [v for v in versions if v["IsLatest"]]
    if len(latest) != 1 or latest[0]["VersionId"] != keep_version_id:
        raise ValueError("Object changed after replacement; old versions were not purged")
    deleted = 0
    for version in versions:
        if not version["IsLatest"]:
            s3.delete_object(Bucket=bucket, Key=key, VersionId=version["VersionId"])
            deleted += 1
    return deleted


def write_marker(s3, marker_uri, marker, create_only):
    common.put_json(s3, marker_uri, marker, create_only)


def process_file(s3, hits, marker_uri, already_done, purge, batch_id, outcome):
    """Rewrite one part (or finish an earlier attempt) and record a completion marker.

    The marker is written as soon as the object is replaced, so a later failure
    (e.g. while purging old versions) can be resumed without an ETag mismatch.
    """
    bucket, key = common.split_uri(hits.uri)
    if already_done:
        marker = common.get_json(s3, marker_uri)
        if not purge or marker.get("versions_purged"):
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
            "new_etag": response.get("ETag"),
            "new_version_id": response.get("VersionId"),
            "versions_purged": False,
            "completed_at": common.now(),
        }
        write_marker(s3, marker_uri, marker, create_only=True)
    if purge:
        outcome["versions_deleted"] = purge_old_versions(s3, bucket, key, marker.get("new_version_id"))
        marker["versions_purged"] = True
        write_marker(s3, marker_uri, marker, create_only=False)
    outcome["status"] = "DONE"


def forget_task(shared, item):
    """Spark task for forget. shared: batch_id, purge, region.
    item: {"uri", "etag", "version_id", "tran_ids", "marker_uri", "already_done"}."""
    common.raise_csv_limit()
    started = time.time()
    hits = formats.FileHits(item["uri"], item["etag"], item["version_id"], set(item["tran_ids"]))
    outcome = {"uri": hits.uri, "rows_expected": len(hits.tran_ids)}
    try:
        process_file(s3_for(shared), hits, item["marker_uri"], item["already_done"], shared["purge"],
                     shared["batch_id"], outcome)
    except Exception as exc:
        outcome["status"] = "FAILED"
        if common.error_code(exc) in ("PreconditionFailed", "412"):
            outcome["error"] = "The part changed since Find ran (ETag mismatch); run Find again with a new BATCH_ID"
        else:
            outcome["error"] = str(exc) or type(exc).__name__
    outcome["seconds"] = round(time.time() - started, 2)
    return outcome
