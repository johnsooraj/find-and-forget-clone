"""Processed-file tracker: avoid scanning the same part file again for nothing.

A record is kept per (part file, scope), where scope = "<find strategy>#<fingerprint>".
The fingerprint covers the strategy settings and the input file's ETag, so
"same strategy + same input" has the same scope (see FindStrategy.fingerprint).

Records are written by the DRIVERS only:
  SCANNED_NO_MATCH  Find scanned the file and found nothing (ETag at scan time)
  CLEANED           Forget rewrote the file (the NEW ETag)

Find checks the tracker once, when a batch is created: a selected file is
skipped when its scope has a record AND its current ETag (from the listing)
equals the recorded ETag. So:
  - a file with matches that was not forgotten yet is never recorded, so it is
    always scanned again (the results stay correct);
  - a file changed by anything else has a new ETag, so it is scanned again;
  - a new input file has a new fingerprint, so every file is scanned again.

Backends: DYNAMODB (default in Terraform), S3_TAG (a tag on the part file
itself), NONE. Writing a record never fails the job (worst case: a file is
scanned again); a failed lookup at the start does fail it.

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
import time
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Optional, Tuple

import common

DYNAMODB, S3_TAG, NONE = "DYNAMODB", "S3_TAG", "NONE"
TRACKERS = (DYNAMODB, S3_TAG, NONE)
SCANNED_NO_MATCH, CLEANED = "SCANNED_NO_MATCH", "CLEANED"
TAG_PREFIX = "drop-glue-"


class ProcessedTracker(ABC):
    @abstractmethod
    def lookup(self, scope: str, uris: List[str]) -> Dict[str, dict]:
        """{uri: {"etag": str or None, "stage": str}} for the files that have a record in scope."""

    @abstractmethod
    def record(self, scope: str, entries: List[dict], batch_id: str) -> None:
        """entries: [{"uri", "etag", "version_id", "stage"}]. Raise on failure; callers use safe_record."""


def skippable(tracker, scope, files: Iterable[Tuple[str, Optional[str]]]) -> set:
    """URIs whose current ETag equals the tracked ETag. files: [(uri, current etag)].

    A record without an ETag (S3_TAG: the tag lives on the object version, so
    the version is the check) counts as a match.
    """
    files = [(uri, etag) for uri, etag in files if etag]
    if not files or tracker is None:
        return set()
    records = tracker.lookup(scope, [uri for uri, _ in files])
    return {uri for uri, etag in files
            if uri in records and records[uri].get("etag") in (None, etag)}


def safe_record(tracker, scope, entries, batch_id):
    """Write records; print a warning instead of failing the job."""
    if tracker is None or not scope or not entries:
        return
    try:
        tracker.record(scope, entries, batch_id)
    except Exception as exc:  # noqa: BLE001 - tracking is an optimization
        print(f"WARNING: could not record {len(entries)} file(s) in the tracker: {exc}")


def make_tracker(config, s3=None, dynamodb=None) -> Optional[ProcessedTracker]:
    if config.tracker == DYNAMODB:
        if dynamodb is None:
            import boto3
            from botocore.config import Config

            dynamodb = boto3.client("dynamodb", region_name=config.region,
                                    config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))
        return DynamoDbTracker(dynamodb, config.tracker_table)
    if config.tracker == S3_TAG:
        return S3TagTracker(s3)
    return None


# ----------------------------------------------------------------------------- DynamoDB
class DynamoDbTracker(ProcessedTracker):
    """Table: partition key file_uri (S), sort key scope (S). See main.tf."""

    GET_BATCH, PUT_BATCH, ATTEMPTS = 100, 25, 8

    def __init__(self, client, table):
        self.client = client
        self.table = table

    def lookup(self, scope, uris):
        found = {}
        for start in range(0, len(uris), self.GET_BATCH):
            keys = [{"file_uri": {"S": uri}, "scope": {"S": scope}} for uri in uris[start:start + self.GET_BATCH]]
            request = {self.table: {"Keys": keys, "ProjectionExpression": "#u, #e, #s",  # names may be reserved
                                    "ExpressionAttributeNames": {"#u": "file_uri", "#e": "etag", "#s": "stage"}}}
            for attempt in range(self.ATTEMPTS):
                response = self.client.batch_get_item(RequestItems=request)
                for item in response.get("Responses", {}).get(self.table, []):
                    found[item["file_uri"]["S"]] = {"etag": item.get("etag", {}).get("S"),
                                                     "stage": item.get("stage", {}).get("S")}
                request = response.get("UnprocessedKeys") or {}
                if not request:
                    break
                time.sleep(min(2 ** attempt * 0.1, 5))
            else:
                raise RuntimeError("DynamoDB kept throttling the tracker lookup")
        return found

    def record(self, scope, entries, batch_id):
        stamp = common.now()
        for start in range(0, len(entries), self.PUT_BATCH):
            writes = [{"PutRequest": {"Item": {
                "file_uri": {"S": e["uri"]},
                "scope": {"S": scope},
                "etag": {"S": e["etag"]},
                "stage": {"S": e["stage"]},
                "batch_id": {"S": batch_id},
                "updated_at": {"S": stamp},
            }}} for e in entries[start:start + self.PUT_BATCH] if e.get("etag")]
            request = {self.table: writes} if writes else {}
            for attempt in range(self.ATTEMPTS):
                if not request:
                    break
                response = self.client.batch_write_item(RequestItems=request)
                request = response.get("UnprocessedItems") or {}
                if request:
                    time.sleep(min(2 ** attempt * 0.1, 5))
            if request:
                raise RuntimeError("DynamoDB kept throttling the tracker writes")


# ----------------------------------------------------------------------------- S3 object tag
def tag_key(scope):
    """One tag per find strategy: drop-glue-hash_match / drop-glue-tran_id_suffix."""
    return TAG_PREFIX + scope.split("#", 1)[0].lower()


def tag_value(scope, stage):
    return f"{scope.split('#', 1)[1][:64]}:{stage}"


class S3TagTracker(ProcessedTracker):
    """Marker tag on the part file itself.

    Limits: S3 allows 10 tags per object and tags belong to one object
    version, so a rewrite by anything else drops the marker (that is what we
    want: the file is scanned again). One GetObjectTagging call per selected
    file at batch start, so DYNAMODB is cheaper for large archives.
    """

    def __init__(self, s3):
        self.s3 = s3

    def lookup(self, scope, uris):
        key, found = tag_key(scope), {}
        fingerprint = scope.split("#", 1)[1][:64]
        for uri in uris:
            bucket, object_key = common.split_uri(uri)
            value = common.get_tags(self.s3, bucket, object_key).get(key, "")
            recorded, _, stage = value.partition(":")
            if recorded == fingerprint:
                found[uri] = {"etag": None, "stage": stage}
        return found

    def record(self, scope, entries, batch_id):
        failures = []
        for e in entries:
            bucket, object_key = common.split_uri(e["uri"])
            try:
                common.put_merged_tags(self.s3, bucket, object_key, {tag_key(scope): tag_value(scope, e["stage"])},
                                       version_id=e.get("version_id"))
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{e['uri']}: {exc}")
        if failures:
            raise RuntimeError("; ".join(failures[:5]))
