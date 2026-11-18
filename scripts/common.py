"""Shared code for every layer: S3 helpers, archive reading, value
normalization, month selection, the stop switch and the Spark/local runners.

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
import csv
import datetime
import gzip
import hashlib
import io
import json
import re
import sys
import tarfile
import unicodedata
from contextlib import closing, contextmanager
from pathlib import PurePosixPath

FIELDS = {"TRAN_ID", "SEQ_NB", "PROFILE_ID", "PROD_OPT", "TP_DB_TS", "RAW_RESPONSE"}
BATCH_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")
MONTH = re.compile(r"\d{4}-\d{2}")
DATE_FOLDER = re.compile(r"\d{4}-\d{2}-\d{2}")
MAX_JSON = 64 * 1024 * 1024
HASH_MODES = ("normalized", "raw")
MAX_FILES_PER_WAVE = 5000


# ----------------------------------------------------------------------------- misc
def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def check_batch_id(value):
    if not value or not BATCH_ID.fullmatch(value):
        raise ValueError("Provide --batch_id: 1-128 letters, digits, _ or -")
    return value


def parse_bool(value, name):
    text = str(value or "false").strip().lower()
    if text in ("true", "1", "yes"):
        return True
    if text in ("false", "0", "no"):
        return False
    raise ValueError(f"{name} must be true or false")


def parse_int(value, name, minimum, maximum):
    try:
        number = int(str(value).strip())
    except ValueError:
        raise ValueError(f"{name} must be a whole number") from None
    if not minimum <= number <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return number


def raise_csv_limit():
    csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


# ----------------------------------------------------------------------------- S3
def s3_client(region):
    """S3 client with adaptive retries, so 100+ parallel tasks back off instead of failing."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        region_name=region,
        config=Config(retries={"max_attempts": 10, "mode": "adaptive"}, max_pool_connections=16),
    )


def split_uri(uri):
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise ValueError("Expected an s3:// URI")
    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket:
        raise ValueError("Expected an s3:// URI with a bucket")
    return bucket, key


def location(uri):
    bucket, key = split_uri(uri)
    if not key:
        raise ValueError("Expected an S3 object URI")
    return {"Bucket": bucket, "Key": key}


def error_code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


def get_json_with_etag(s3, uri, limit=MAX_JSON):
    obj = s3.get_object(**location(uri))
    with closing(obj["Body"]) as body:
        raw = body.read(limit + 1)
    if len(raw) > limit:
        raise ValueError(f"{uri} exceeds the size limit")
    return json.loads(raw), obj["ETag"]


def get_json(s3, uri, limit=MAX_JSON):
    return get_json_with_etag(s3, uri, limit)[0]


def get_lines(s3, uri, limit=512 * 1024 * 1024):
    """Return the non-empty lines of a JSON Lines object."""
    with closing(s3.get_object(**location(uri))["Body"]) as body:
        raw = body.read(limit + 1)
    if len(raw) > limit:
        raise ValueError(f"{uri} exceeds the size limit")
    return [line for line in raw.decode("utf-8").splitlines() if line.strip()]


def put_json(s3, uri, document, create_only):
    payload = json.dumps(document, indent=2, sort_keys=True).encode("utf-8")
    extra = {"IfNoneMatch": "*"} if create_only else {}
    s3.put_object(**location(uri), Body=payload, ContentType="application/json", **extra)


def put_lines(s3, uri, documents, create_only):
    payload = b"".join(json.dumps(d, sort_keys=True).encode("utf-8") + b"\n" for d in documents)
    extra = {"IfNoneMatch": "*"} if create_only else {}
    s3.put_object(**location(uri), Body=payload, ContentType="application/x-ndjson", **extra)


def list_objects(s3, bucket, prefix, delimiter=None):
    """Return (folder prefixes, {object key: ETag}) under prefix, following pagination."""
    folders, objects, token = [], {}, None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if delimiter:
            kwargs["Delimiter"] = delimiter
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        folders += [item["Prefix"] for item in page.get("CommonPrefixes", [])]
        objects.update({item["Key"]: item.get("ETag") for item in page.get("Contents", [])})
        if not page.get("IsTruncated"):
            return folders, objects
        token = page["NextContinuationToken"]


def list_keys(s3, bucket, prefix, delimiter=None):
    """Return (folder prefixes, object keys) under prefix, following pagination."""
    folders, objects = list_objects(s3, bucket, prefix, delimiter)
    return folders, list(objects)


MAX_TAGS = 10  # S3 limit per object (version)


def get_tags(s3, bucket, key, version_id=None):
    """{tag key: value} of one object version (the current one when version_id is None)."""
    extra = {"VersionId": version_id} if version_id else {}
    response = s3.get_object_tagging(Bucket=bucket, Key=key, **extra)
    return {t["Key"]: t["Value"] for t in response.get("TagSet", [])}


def put_merged_tags(s3, bucket, key, updates, version_id=None):
    """Add/replace `updates` on an object version, keeping its other tags.

    PutObjectTagging replaces the whole tag set, so read it first. Does not
    change the object's ETag or create a new version.
    """
    tags = get_tags(s3, bucket, key, version_id)
    tags.update(updates)
    if len(tags) > MAX_TAGS:
        raise ValueError(f"s3://{bucket}/{key} would have more than {MAX_TAGS} tags")
    extra = {"VersionId": version_id} if version_id else {}
    s3.put_object_tagging(Bucket=bucket, Key=key, Tagging={"TagSet": [{"Key": k, "Value": v}
                                                                      for k, v in sorted(tags.items())]}, **extra)


def object_exists(s3, uri):
    bucket, key = split_uri(uri)
    _, keys = list_keys(s3, bucket, key)
    return key in keys


# ----------------------------------------------------------------------------- stop switch
def stop_requested(s3, stop_uri):
    """True while the emergency stop switch (an S3 object) exists. See Jenkinsfile.stop."""
    return object_exists(s3, stop_uri)


# ----------------------------------------------------------------------------- runners
class LocalRunner:
    """Runs a wave in this process, one file after another. Used by the tests."""

    def __init__(self, shared):
        self.shared = shared

    def map(self, func, items):
        return [func(self.shared, item) for item in items]


class SparkRunner:
    """Runs a wave on Spark: one task per file, all at once (up to the executor cores).

    `shared` (lookup tables, settings) is broadcast once per run, not once per task.
    """

    def __init__(self, spark_context, shared):
        self.sc = spark_context
        self.shared = spark_context.broadcast(shared)

    def map(self, func, items):
        shared = self.shared
        return self.sc.parallelize(items, len(items)).map(lambda item: func(shared.value, item)).collect()


# ----------------------------------------------------------------------------- values
def sha256_hex(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize(value, match_type, mode="normalized"):
    """Turn an XML field value into the text that is hashed.

    MUST match how the upstream system builds hash_value (see docs/GUIDE.md,
    "How hashing works").
    """
    if mode == "raw":
        return value.strip()
    value = unicodedata.normalize("NFKC", value).strip()
    if match_type == "PHONE":
        return "".join(str(unicodedata.decimal(char)) for char in value if char.isdecimal())
    if match_type in ("NAME", "ADDRESS"):
        return " ".join(value.split()).lower()
    return value.lower()  # EMAIL


# ----------------------------------------------------------------------------- archives
class StreamingTarFile(tarfile.TarFile):
    class fileobject(tarfile.ExFileObject):
        def seekable(self):
            return False


@contextmanager
def archive_rows(body, key):
    """Yield (tar member, csv.DictReader) for a part file.

    AB_CDEFG_DATA_01_part_0001.tar.gz must hold exactly one CSV named
    AB_CDEFG_DATA_01_part_0001.csv, starting with the header row.
    """
    expected_name = PurePosixPath(key).name.removesuffix(".tar.gz") + ".csv"
    with gzip.GzipFile(fileobj=body) as compressed:
        with StreamingTarFile.open(fileobj=compressed, mode="r|") as bundle:
            member = bundle.next()
            if member is None or not member.isfile() or member.name != expected_name:
                raise ValueError(f"Expected a single CSV named {expected_name} at the root of the archive")
            with io.TextIOWrapper(bundle.extractfile(member), encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream, strict=True)
                if not reader.fieldnames or set(reader.fieldnames) != FIELDS or len(reader.fieldnames) != len(FIELDS):
                    raise ValueError("Unexpected CSV header")
                yield member, reader
            if bundle.next() is not None:
                raise ValueError("Archive contains more than one member")
        while compressed.read(1024 * 1024):
            pass


def records(reader):
    for row in reader:
        if None in row or None in row.values() or not row["TRAN_ID"].strip():
            raise ValueError(f"Invalid CSV row at line {reader.line_num}")
        yield row


# ----------------------------------------------------------------------------- month selection
def parse_month(value, name):
    """'2026-01' -> '2026-01'; empty -> None."""
    if not value:
        return None
    if not MONTH.fullmatch(value) or not 1 <= int(value[5:]) <= 12:
        raise ValueError(f"{name} must be YYYY-MM")
    return value


def folder_month(name):
    """Month of an archive folder: '2026-01-01' -> '2026-01'. None if not a date folder."""
    if not DATE_FOLDER.fullmatch(name):
        return None
    try:
        datetime.date.fromisoformat(name)
    except ValueError:
        return None
    return name[:7]


def under_prefix(uri, source_prefix):
    return uri.startswith(source_prefix) and uri.endswith(".tar.gz")


def resolve_archive_uri(uri, source_prefix):
    """Accept a folder (trailing slash added) or one .tar.gz, under the allowed prefix. Empty -> None."""
    if not uri:
        return None
    if not uri.endswith(".tar.gz") and not uri.endswith("/"):
        uri += "/"
    if not uri.startswith(source_prefix):
        raise ValueError(f"--archive_uri must be under {source_prefix}")
    return uri


def select_archives(s3, archive_uri, month_from, month_to):
    """Return (sorted part file URIs, skipped non-date folder names, {uri: ETag}).

    archive_uri is one .tar.gz, or a folder whose children are YYYY-MM-DD
    folders (the monthly run date, e.g. 2026-01-01). A folder is selected when
    its month is within month_from..month_to; empty months mean every folder.
    Every .tar.gz part in a selected folder is included. Files directly in
    archive_uri are included only when no month range is given. The ETags come
    from the listing (no extra calls) and are used by the processed-file tracker.
    """
    bucket, prefix = split_uri(archive_uri)
    if prefix.endswith(".tar.gz"):
        if month_from or month_to:
            raise ValueError("--month_from/--month_to apply only when --archive_uri is a folder")
        _, objects = list_objects(s3, bucket, prefix)
        return [archive_uri], [], {archive_uri: objects.get(prefix)}
    folders, direct = list_objects(s3, bucket, prefix, "/")
    selected, skipped = {}, []
    for folder in sorted(folders):
        name = folder[len(prefix):].rstrip("/")
        month = folder_month(name)
        if month is None:
            skipped.append(name)
            continue
        if (month_from and month < month_from) or (month_to and month > month_to):
            continue
        _, objects = list_objects(s3, bucket, folder, "/")
        selected.update({key: etag for key, etag in objects.items() if key.endswith(".tar.gz")})
    if not month_from and not month_to:
        selected.update({key: etag for key, etag in direct.items() if key.endswith(".tar.gz")})
    etags = {f"s3://{bucket}/{key}": selected[key] for key in sorted(selected)}
    return list(etags), skipped, etags
