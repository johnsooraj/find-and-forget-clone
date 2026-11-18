"""Forget: one rewrite per part file, same name, safe reruns, recovery, version purge."""
import json

import pytest

import forget
import forget_strategies
import tasks
from conftest import (
    ARCHIVES, EMAIL, F1, INPUT, J1, J2, M1, NAME, PHONE, RESULTS, FakeS3, add_archives, base, forget_args, item,
    local_runner, read_archive, row, run_find, run_forget, uri_of, xml, COLUMNS, make_archive,
)

EXPECTED_LEFT = {J1: ["103"], J2: ["141"], F1: ["878"], M1: ["902"]}


def source_puts(s3):
    return [uri for uri in s3.put_log if uri.startswith("s3://lake/")]


def found_and_cleared(s3):
    run_find(s3)
    s3.put_log.clear()


def test_round_trip_removes_rows_and_keeps_names(s3):
    found_and_cleared(s3)
    report, error = run_forget(s3)
    assert error is None and report["status"] == "COMPLETE" and report["files_done"] == 4
    for path, left in EXPECTED_LEFT.items():
        name, header, rows = read_archive(s3.read(uri_of(path)))
        assert name == path.split("/")[-1] + ".csv"  # D_01_part_0001.csv inside D_01_part_0001.tar.gz
        assert header == COLUMNS
        assert [r["TRAN_ID"] for r in rows] == left
        original = {r["TRAN_ID"]: r for r in ARCHIVES[path]}
        assert all(r == original[r["TRAN_ID"]] for r in rows)  # kept rows unchanged


def test_exactly_one_put_per_part(s3):
    found_and_cleared(s3)
    run_forget(s3)
    assert sorted(source_puts(s3)) == sorted(uri_of(p) for p in EXPECTED_LEFT)


def test_twelve_parts_mean_twelve_rewrites():
    s3 = FakeS3()
    parts = {}
    for n in range(1, 13):
        path = f"2026-01-01/M_part_{n:04d}"
        parts[path] = [row(f"{n}-{k}", xml(phone="12025550134", email="jane.doe@example.com", name="jane doe"))
                       for k in range(5)] + [row(f"{n}-keep", xml(phone="1"))]
    add_archives(s3, parts)
    s3.put(INPUT, json.dumps([item("p", PHONE, "PHONE"), item("e", EMAIL, "EMAIL"), item("n", NAME, "NAME")]).encode())
    summary = run_find(s3)
    assert summary["files_to_rewrite"] == 12 and summary["rows_to_delete"] == 60
    s3.put_log.clear()
    report, error = run_forget(s3, files_per_wave="5")
    assert error is None and report["files_done"] == 12
    assert len(source_puts(s3)) == 12


def test_part_with_every_row_removed_keeps_its_header():
    s3 = FakeS3()
    add_archives(s3, {"2026-01-01/X_part_0001": [row("1", xml(phone="12025550134"))]})
    s3.put(INPUT, json.dumps([item("p", PHONE, "PHONE")]).encode())
    run_find(s3)
    run_forget(s3)
    name, header, rows = read_archive(s3.read(uri_of("2026-01-01/X_part_0001")))
    assert name == "X_part_0001.csv" and header == COLUMNS and rows == []


def test_content_type_and_metadata_kept_and_batch_tagged(s3):
    found_and_cleared(s3)
    run_forget(s3)
    latest = s3.latest(uri_of(J1))
    assert latest["content_type"] == "application/x-gzip"
    assert latest["metadata"]["owner"] == "lake"
    assert latest["metadata"][tasks.META_BATCH] == "B1"


def test_purge_deletes_old_versions(s3):
    found_and_cleared(s3)
    report, error = run_forget(s3, forget_strategy="REPLACE_PURGE_OLD_VERSIONS")
    assert error is None
    for path in EXPECTED_LEFT:
        assert len(s3.versions(uri_of(path))) == 1
    assert all(f["versions_deleted"] == 1 for f in report["files"])


def test_without_purge_old_versions_remain(s3):
    found_and_cleared(s3)
    run_forget(s3)
    assert len(s3.versions(uri_of(J1))) == 2


def test_rerun_skips_finished_parts(s3):
    found_and_cleared(s3)
    run_forget(s3)
    s3.put_log.clear()
    report, error = run_forget(s3)
    assert error is None and report["files_skipped"] == 4
    assert source_puts(s3) == []


def test_changed_part_fails_alone_and_is_untouched(s3):
    found_and_cleared(s3)
    changed = make_archive("D_02_part_0001", ARCHIVES[F1] + [row("999", xml())])
    s3.put(uri_of(F1), changed)
    report, error = run_forget(s3)
    assert error is not None and report["status"] == "INCOMPLETE"
    by_uri = {f["uri"]: f for f in report["files"]}
    assert "ETag" in by_uri[uri_of(F1)]["error"]
    assert s3.read(uri_of(F1)) == changed
    assert report["files_done"] == 3


def test_killed_after_put_is_recovered_without_rewriting(s3, monkeypatch):
    """A KILL between the PUT and the completion marker must not show up as FAILED later."""
    found_and_cleared(s3)
    original = tasks.write_marker

    def killed(*args, **kwargs):
        raise RuntimeError("killed")

    monkeypatch.setattr(tasks, "write_marker", killed)
    report, error = run_forget(s3)
    assert report["files_failed"] == 4  # replaced, but no markers
    monkeypatch.setattr(tasks, "write_marker", original)
    s3.put_log.clear()
    report, error = run_forget(s3)
    assert error is None and report["status"] == "COMPLETE"
    assert all(f.get("recovered") for f in report["files"])
    assert source_puts(s3) == []  # nothing rewritten twice
    for path, left in EXPECTED_LEFT.items():
        assert [r["TRAN_ID"] for r in read_archive(s3.read(uri_of(path)))[2]] == left


def test_failed_purge_resumes_on_rerun(s3, monkeypatch):
    found_and_cleared(s3)
    original = forget_strategies.purge_old_versions

    def broken(*args, **kwargs):
        raise RuntimeError("throttled")

    monkeypatch.setattr(forget_strategies, "purge_old_versions", broken)
    report, error = run_forget(s3, forget_strategy="REPLACE_PURGE_OLD_VERSIONS")
    assert error is not None and report["files_failed"] == 4
    monkeypatch.setattr(forget_strategies, "purge_old_versions", original)
    s3.put_log.clear()
    report, error = run_forget(s3, forget_strategy="REPLACE_PURGE_OLD_VERSIONS")
    assert error is None and report["files_done"] == 4
    assert source_puts(s3) == []  # rows were already removed; only the purge ran
    for path in EXPECTED_LEFT:
        assert len(s3.versions(uri_of(path))) == 1


def test_unversioned_bucket():
    s3 = FakeS3(versioned=False)
    add_archives(s3, {J1: ARCHIVES[J1]})
    s3.put(INPUT, json.dumps([item("p", PHONE, "PHONE")]).encode())
    run_find(s3)
    report, error = run_forget(s3, forget_strategy="REPLACE_PURGE_OLD_VERSIONS")
    assert error is None and report["files"][0]["versions_deleted"] == 0


def test_no_matches_means_nothing_to_do(s3):
    s3.put(INPUT, json.dumps([item("x", "Nobody", "NAME")]).encode())
    found_and_cleared(s3)
    report, error = run_forget(s3)
    assert error is None and report["files"] == [] and source_puts(s3) == []


def test_parts_outside_prefix_rejected(s3):
    run_find(s3)
    bad = {"schema_version": "3", "uri": "s3://lake/OTHER/x.tar.gz", "etag": '"ab"',
           "version_id": None, "tran_ids": ["1"], "run_ids": []}
    s3.latest(base() + "files.jsonl")["body"] = json.dumps(bad).encode()
    with pytest.raises(ValueError, match="outside"):
        forget.run(s3, forget_args(), local_runner(s3))


def test_missing_report_fails(s3):
    with pytest.raises(Exception):
        forget.run(s3, forget_args(batch_id="NEVER_RAN"), local_runner(s3))
