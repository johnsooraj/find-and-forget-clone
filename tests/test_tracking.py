"""Processed-file tracker: skip files already processed for the same strategy + input."""
import json

import pytest

import tracking
from conftest import (
    INPUT, ITEMS, FakeDynamo, add_archives, base, find_outputs, item, make_archive, row, run_find, run_forget, uri_of,
    xml, PHONE,
)

QUIET = "2026-04-01/D_04_part_0001"  # no customer of ITEMS in it


@pytest.fixture
def lake(s3):
    add_archives(s3, {QUIET: [row("400", xml(phone="1")), row("401", xml())]})
    return s3


@pytest.fixture(params=["dynamodb", "s3_tag"])
def tracker(request, lake):
    if request.param == "dynamodb":
        return tracking.DynamoDbTracker(FakeDynamo(), "drop-glue-tracker")
    return tracking.S3TagTracker(lake)


def scanned(s3, batch_id):
    """URIs scanned by the batch's waves (not skipped by the tracker)."""
    prefix = base(batch_id).replace("s3://jobs/", "") + "waves/"
    uris = []
    for (bucket, key), versions in s3.store.items():
        if bucket == "jobs" and key.startswith(prefix):
            uris += [json.loads(line)["uri"] for line in versions[-1]["body"].decode().splitlines() if line]
    return sorted(uris)


ALL = sorted(uri_of(p) for p in ["2026-01-01/D_01_part_0001", "2026-01-01/D_01_part_0002",
                                 "2026-02-01/D_02_part_0001", "2026-03-01/D_03_part_0001", QUIET])


def test_no_match_file_is_skipped_on_a_rerun_with_the_same_input(lake, tracker):
    first = run_find(lake, tracker=tracker, batch_id="B1")
    assert first["files_skipped_tracked"] == 0 and scanned(lake, "B1") == ALL
    second = run_find(lake, tracker=tracker, batch_id="B2")
    assert second["files_skipped_tracked"] == 1 and second["files_done"] == 5
    assert uri_of(QUIET) not in scanned(lake, "B2")
    # Files with matches that were not forgotten yet are always scanned again.
    assert len(scanned(lake, "B2")) == 4
    assert find_outputs(lake, "B1")[1:] == find_outputs(lake, "B2")[1:]


def test_new_input_scans_everything_again(lake, tracker):
    run_find(lake, tracker=tracker, batch_id="B1")
    lake.put(INPUT, json.dumps(ITEMS + [item("run-extra", PHONE, "PHONE")]).encode())
    summary = run_find(lake, tracker=tracker, batch_id="B2")
    assert summary["files_skipped_tracked"] == 0 and scanned(lake, "B2") == ALL


def test_files_cleaned_by_forget_are_skipped(lake, tracker):
    run_find(lake, tracker=tracker, batch_id="B1")
    report, error = run_forget(lake, tracker=tracker, batch_id="B1")
    assert error is None and report["files_done"] == 4
    summary = run_find(lake, tracker=tracker, batch_id="B2")
    assert summary["files_skipped_tracked"] == 5 and scanned(lake, "B2") == []
    _, results, lines = find_outputs(lake, "B2")
    assert lines == []
    assert {r["match_result"] for r in results} == {"NO_MATCH"}


def test_file_changed_outside_is_scanned_again(lake, tracker):
    run_find(lake, tracker=tracker, batch_id="B1")
    lake.put(uri_of(QUIET), make_archive("D_04_part_0001", [row("400", xml(phone="12025550134"))]))
    summary = run_find(lake, tracker=tracker, batch_id="B2")
    assert summary["files_skipped_tracked"] == 0
    _, results, _ = find_outputs(lake, "B2")
    assert uri_of(QUIET) in {r["run_id"]: r for r in results}["run-phone"]["data_identifier"]


def test_other_strategy_does_not_share_records(lake, tracker):
    run_find(lake, tracker=tracker, batch_id="B1")
    summary = run_find(lake, tracker=tracker, batch_id="B2", find_strategy="TRAN_ID_SUFFIX", input_uri=None)
    assert summary["files_skipped_tracked"] == 0


def test_dynamodb_table_holds_one_record_per_file_and_scope(lake):
    dynamo = FakeDynamo()
    tracker = tracking.DynamoDbTracker(dynamo, "t")
    run_find(lake, tracker=tracker)
    assert dynamo.records() == {(uri_of(QUIET), "HASH_MATCH"): tracking.SCANNED_NO_MATCH}
    run_forget(lake, tracker=tracker)
    stages = dynamo.records()
    assert len(stages) == 5
    assert {s for (uri, _), s in stages.items() if uri != uri_of(QUIET)} == {tracking.CLEANED}
    new_etag = lake.latest(uri_of("2026-01-01/D_01_part_0001"))["etag"]
    cleaned = [v for k, v in dynamo.items.items() if k[1] == uri_of("2026-01-01/D_01_part_0001")]
    assert cleaned[0]["etag"]["S"] == new_etag and cleaned[0]["batch_id"]["S"] == "B1"


def test_dynamodb_retries_unprocessed_items(lake):
    dynamo = FakeDynamo(unprocessed_once=True)
    run_find(lake, tracker=tracking.DynamoDbTracker(dynamo, "t"))
    assert dynamo.records() == {(uri_of(QUIET), "HASH_MATCH"): tracking.SCANNED_NO_MATCH}


def test_s3_tag_marker_is_on_the_part_file(lake):
    run_find(lake, tracker=tracking.S3TagTracker(lake))
    tags = lake.tags(uri_of(QUIET))
    assert list(tags) == ["drop-glue-hash_match"]
    assert tags["drop-glue-hash_match"].endswith(":" + tracking.SCANNED_NO_MATCH)


def test_a_failing_tracker_write_never_fails_the_job(lake):
    class Broken(tracking.ProcessedTracker):
        def lookup(self, scope, uris):
            return {}

        def record(self, scope, entries, batch_id):
            raise RuntimeError("table is gone")

    assert run_find(lake, tracker=Broken())["status"] == "COMPLETE"
    report, error = run_forget(lake, tracker=Broken())
    assert error is None and report["status"] == "COMPLETE"


def test_a_failing_tracker_lookup_fails_the_job_before_scanning(lake):
    class Down(tracking.ProcessedTracker):
        def lookup(self, scope, uris):
            raise RuntimeError("no access")

        def record(self, scope, entries, batch_id):
            pass

    with pytest.raises(RuntimeError, match="no access"):
        run_find(lake, tracker=Down())
    assert scanned(lake, "B1") == []


def test_no_tracker_scans_everything(lake):
    run_find(lake, batch_id="B1")
    assert run_find(lake, batch_id="B2")["files_skipped_tracked"] == 0
