"""Shared test helpers: an in-memory fake S3 and DynamoDB, archive builders and runners.
No AWS and no Spark needed: waves run through common.LocalRunner."""
import csv
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import common  # noqa: E402
import find  # noqa: E402
import find_strategies  # noqa: E402
import forget  # noqa: E402

EXAMPLES = ROOT / "examples"
SOURCE = "s3://lake/DBD001/archive/"
RESULTS = "s3://jobs/drop-glue/results/"
STOP = "s3://jobs/drop-glue/control/STOP"
INPUT = "s3://jobs/drop-glue/requests/in.json"
COLUMNS = ["TRAN_ID", "SEQ_NB", "PROFILE_ID", "PROD_OPT", "TP_DB_TS", "RAW_RESPONSE"]

common.raise_csv_limit()


# ----------------------------------------------------------------------------- fake S3
class S3Error(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """Just enough of the S3 client: versioning, conditional writes, pagination, metadata."""

    def __init__(self, versioned=True, page_size=2):
        self.versioned = versioned
        self.page_size = page_size
        self.store = {}  # (bucket, key) -> [version, ...] oldest first
        self.counter = 0
        self.put_log = []  # every successful put as "s3://bucket/key"
        self.tag_log = []  # every put_object_tagging as ("s3://bucket/key", version id)

    def _latest(self, bucket, key):
        versions = self.store.get((bucket, key))
        if not versions:
            raise S3Error("NoSuchKey")
        return versions[-1]

    def put_object(self, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None, ContentType=None, Metadata=None, **_):
        data = Body if isinstance(Body, bytes) else Body.read()
        existing = self.store.get((Bucket, Key))
        if IfNoneMatch == "*" and existing:
            raise S3Error("PreconditionFailed")
        if IfMatch is not None and (not existing or existing[-1]["etag"] != IfMatch):
            raise S3Error("PreconditionFailed")
        self.counter += 1
        version = {
            "body": data,
            "etag": '"%s"' % hashlib.md5(data).hexdigest(),
            "version_id": f"v{self.counter}" if self.versioned else None,
            "content_type": ContentType,
            "metadata": dict(Metadata or {}),
            "tags": {},
        }
        if self.versioned:
            self.store.setdefault((Bucket, Key), []).append(version)
        else:
            self.store[(Bucket, Key)] = [version]
        self.put_log.append(f"s3://{Bucket}/{Key}")
        response = {"ETag": version["etag"]}
        if self.versioned:
            response["VersionId"] = version["version_id"]
        return response

    def _describe(self, version):
        response = {"ETag": version["etag"], "Metadata": dict(version["metadata"]),
                    "ContentLength": len(version["body"])}
        if version["content_type"]:
            response["ContentType"] = version["content_type"]
        if version["version_id"]:
            response["VersionId"] = version["version_id"]
        return response

    def get_object(self, Bucket, Key, IfMatch=None):
        version = self._latest(Bucket, Key)
        if IfMatch is not None and version["etag"] != IfMatch:
            raise S3Error("PreconditionFailed")
        return dict(self._describe(version), Body=io.BytesIO(version["body"]))

    def head_object(self, Bucket, Key):
        return self._describe(self._latest(Bucket, Key))

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        entries, seen = [], set()
        for key in sorted(k for (b, k), v in self.store.items() if b == Bucket and k.startswith(Prefix) and v):
            rest = key[len(Prefix):]
            if Delimiter and Delimiter in rest:
                folder = Prefix + rest.split(Delimiter, 1)[0] + Delimiter
                if folder not in seen:
                    seen.add(folder)
                    entries.append(("prefix", folder))
            else:
                entries.append(("key", key))
        start = int(ContinuationToken or 0)
        page = entries[start:start + self.page_size]
        response = {
            "Contents": [{"Key": v, "ETag": self._latest(Bucket, v)["etag"]} for t, v in page if t == "key"],
            "CommonPrefixes": [{"Prefix": v} for t, v in page if t == "prefix"],
            "IsTruncated": start + self.page_size < len(entries),
        }
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(start + self.page_size)
        return response

    def list_object_versions(self, Bucket, Prefix, **_):
        versions = []
        for (bucket, key), items in self.store.items():
            if bucket == Bucket and key.startswith(Prefix):
                for index, item in enumerate(items):
                    versions.append({"Key": key, "VersionId": item["version_id"], "IsLatest": index == len(items) - 1})
        return {"Versions": versions, "IsTruncated": False}

    def delete_object(self, Bucket, Key, VersionId=None):
        if VersionId is None:
            del self.store[(Bucket, Key)]
        else:
            self.store[(Bucket, Key)] = [v for v in self.store[(Bucket, Key)] if v["version_id"] != VersionId]

    def _version(self, Bucket, Key, VersionId):
        if VersionId is None:
            return self._latest(Bucket, Key)
        for version in self.store.get((Bucket, Key), []):
            if version["version_id"] == VersionId:
                return version
        raise S3Error("NoSuchVersion")

    def get_object_tagging(self, Bucket, Key, VersionId=None):
        tags = self._version(Bucket, Key, VersionId)["tags"]
        return {"TagSet": [{"Key": k, "Value": v} for k, v in sorted(tags.items())]}

    def put_object_tagging(self, Bucket, Key, Tagging, VersionId=None):
        self.tag_log.append((f"s3://{Bucket}/{Key}", VersionId))
        self._version(Bucket, Key, VersionId)["tags"] = {t["Key"]: t["Value"] for t in Tagging["TagSet"]}
        return {}

    # helpers for tests
    def put(self, uri, data, **kwargs):
        loc = common.location(uri)
        return self.put_object(Bucket=loc["Bucket"], Key=loc["Key"], Body=data, **kwargs)

    def remove(self, uri):
        loc = common.location(uri)
        self.delete_object(Bucket=loc["Bucket"], Key=loc["Key"])

    def exists(self, uri):
        loc = common.location(uri)
        return bool(self.store.get((loc["Bucket"], loc["Key"])))

    def read(self, uri):
        return self.latest(uri)["body"]

    def latest(self, uri):
        loc = common.location(uri)
        return self._latest(loc["Bucket"], loc["Key"])

    def versions(self, uri):
        loc = common.location(uri)
        return self.store[(loc["Bucket"], loc["Key"])]

    def read_json(self, uri):
        return json.loads(self.read(uri))

    def tags(self, uri, version_id=None):
        loc = common.location(uri)
        return dict(self._version(loc["Bucket"], loc["Key"], version_id)["tags"])


# ----------------------------------------------------------------------------- fake DynamoDB
class FakeDynamo:
    """batch_get_item / batch_write_item of the low-level client. unprocessed_once
    returns everything as unprocessed on the first call, to exercise the retry loop."""

    def __init__(self, unprocessed_once=False):
        self.items = {}  # (table, file_uri, scope) -> item
        self.unprocessed_once = unprocessed_once
        self.calls = 0

    def _bounce(self):
        self.calls += 1
        return self.unprocessed_once and self.calls == 1

    def batch_get_item(self, RequestItems):
        if self._bounce():
            return {"Responses": {}, "UnprocessedKeys": RequestItems}
        responses = {}
        for table, request in RequestItems.items():
            assert len(request["Keys"]) <= 100
            for key in request["Keys"]:
                found = self.items.get((table, key["file_uri"]["S"], key["scope"]["S"]))
                if found:
                    responses.setdefault(table, []).append(found)
        return {"Responses": responses, "UnprocessedKeys": {}}

    def batch_write_item(self, RequestItems):
        if self._bounce():
            return {"UnprocessedItems": RequestItems}
        for table, writes in RequestItems.items():
            assert len(writes) <= 25
            for write in writes:
                item = write["PutRequest"]["Item"]
                self.items[(table, item["file_uri"]["S"], item["scope"]["S"])] = item
        return {"UnprocessedItems": {}}

    def records(self):
        return {(k[1], k[2].split("#")[0]): v["stage"]["S"] for k, v in self.items.items()}


# ----------------------------------------------------------------------------- data builders
PHONE = "+1 (202) 555-0134"
EMAIL = "Jane.Doe@Example.com"
NAME = "Jane  Doe"
ADDRESS = "1 Main St,  Springfield"


def xml(phone="", email="", name="", address=""):
    return (
        f"<resp xmlns='urn:x'><customer><phoneNumber>{phone}</phoneNumber><email>{email}</email>"
        f"<fullName>{name}</fullName><fullAddress>{address}</fullAddress></customer></resp>"
    )


def row(tran_id, raw):
    return {"TRAN_ID": tran_id, "SEQ_NB": "1", "PROFILE_ID": "P1", "PROD_OPT": "X",
            "TP_DB_TS": "2026-01-31 10:00:00", "RAW_RESPONSE": raw}


def make_archive(name, rows):
    """A part file: <name>.tar.gz holding <name>.csv with a header row."""
    text = io.StringIO()
    writer = csv.DictWriter(text, fieldnames=COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    data = text.getvalue().encode("utf-8")
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as bundle:
        member = tarfile.TarInfo(name + ".csv")
        member.size = len(data)
        member.mtime = 1_700_000_000
        bundle.addfile(member, io.BytesIO(data))
    return out.getvalue()


def read_archive(data):
    """Return (inner file name, header, rows)."""
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as bundle:
        member = bundle.next()
        text = bundle.extractfile(member).read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    return member.name, reader.fieldnames, rows


def hashed(value, match_type, mode="normalized"):
    return common.sha256_hex(common.normalize(value, match_type, mode))


def item(run_id, value, match_type, mode="normalized"):
    return {"run_id": run_id, "hash_value": hashed(value, match_type, mode), "match_type": match_type}


def uri_of(path):
    return f"{SOURCE}{path}.tar.gz"


# Monthly folders named after the run date (day 1). January has two parts.
# The phone appears in 3 rows over 2 files; row 102 holds both the phone and
# the email; the name and address appear in March.
J1, J2 = "2026-01-01/D_01_part_0001", "2026-01-01/D_01_part_0002"
F1, M1 = "2026-02-01/D_02_part_0001", "2026-03-01/D_03_part_0001"
ARCHIVES = {
    J1: [row("101", xml(phone="1-202-555-0134")),
         row("102", xml(phone="(1) 202 555 0134", email="jane.doe@example.com")),
         row("103", xml(phone="999", email="x@y.z"))],
    J2: [row("140", xml(email="JANE.DOE@example.com")), row("141", xml(email="a@b.c"))],
    F1: [row("877", xml(phone="12025550134")), row("878", xml(name="John Roe"))],
    M1: [row("901", xml(name="jane doe")), row("902", xml(name="John Roe")),
         row("903", xml(address="1 MAIN ST, Springfield"))],
}

ITEMS = [
    item("run-phone", PHONE, "PHONE"),
    item("run-email", EMAIL, "EMAIL"),
    item("run-name", NAME, "NAME"),
    item("run-address", ADDRESS, "ADDRESS"),
    item("run-nobody", "Nobody Here", "NAME"),
]


def add_archives(client, archives):
    for path, rows in archives.items():
        client.put(uri_of(path), make_archive(path.split("/")[-1], rows), ContentType="application/x-gzip",
                   Metadata={"owner": "lake"})


@pytest.fixture
def s3():
    client = FakeS3()
    add_archives(client, ARCHIVES)
    client.put(SOURCE + "misc/readme.txt", b"not an archive")
    client.put(INPUT, json.dumps(ITEMS).encode())
    client.put_log.clear()
    return client


# ----------------------------------------------------------------------------- running the jobs
def local_runner(s3, after_wave=None):
    """Runner factory for the tests. after_wave(number) runs after each wave."""

    class Runner(common.LocalRunner):
        waves = 0

        def map(self, func, items):
            results = super().map(func, items)
            Runner.waves += 1
            if after_wave:
                after_wave(Runner.waves)
            return results

    return lambda shared: Runner(dict(shared, s3=s3))


COMMON_ARGS = dict(region="us-east-1", source_prefix=SOURCE, result_prefix=RESULTS, stop_uri=STOP,
                   batch_id="B1", files_per_wave="100", max_waves="0", tracker="NONE", tracker_table=None,
                   notifier="LOG", notify_to="")


def find_args(**overrides):
    values = dict(COMMON_ARGS, find_strategy=None, input_uri=INPUT, archive_uri=SOURCE, month_from=None,
                  month_to=None, hash_mode=None, tran_id_suffix=None, retry_failed="false")
    values.update({f"{t.lower()}_tags": v for t, v in find_strategies.DEFAULT_TAGS.items()})
    values.update(overrides)
    return SimpleNamespace(**values)


def forget_args(**overrides):
    values = dict(COMMON_ARGS, forget_strategy="REPLACE_KEEP_VERSIONS")
    values.update(overrides)
    return SimpleNamespace(**values)


def base(batch_id="B1"):
    return f"{RESULTS}{batch_id}/"


def run_find(s3, after_wave=None, tracker=None, **overrides):
    """Run find once; return its summary."""
    return find.run(s3, find_args(**overrides), local_runner(s3, after_wave), tracker)


def find_outputs(s3, batch_id="B1"):
    """(summary, results, files.jsonl lines) of a COMPLETE batch."""
    lines = [json.loads(line) for line in s3.read(base(batch_id) + "files.jsonl").decode().splitlines() if line]
    return s3.read_json(base(batch_id) + "summary.json"), s3.read_json(base(batch_id) + "results.json"), lines


def run_forget(s3, after_wave=None, tracker=None, **overrides):
    """Run forget once; return (remove_summary, RuntimeError or None)."""
    args = forget_args(**overrides)
    try:
        forget.run(s3, args, local_runner(s3, after_wave), tracker)
        error = None
    except RuntimeError as exc:
        error = exc
    return s3.read_json(base(args.batch_id) + "remove_summary.json"), error
