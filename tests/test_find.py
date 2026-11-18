"""Find: hash matching, per-item results, grouping by part file, month selection."""
import json

import pytest

import common
import find
import find_strategies
from conftest import (
    ADDRESS, F1, INPUT, J1, J2, M1, PHONE, SOURCE, base, find_args, find_outputs, hashed, item, local_runner,
    run_find, uri_of,
)

U_J1, U_J2, U_F1, U_M1 = uri_of(J1), uri_of(J2), uri_of(F1), uri_of(M1)


def by_run(results):
    return {r["run_id"]: r for r in results}


def complete(s3, **overrides):
    summary = run_find(s3, **overrides)
    assert summary["status"] == "COMPLETE"
    return find_outputs(s3, overrides.get("batch_id", "B1"))


# ----------------------------------------------------------------------------- results.json
def test_every_matching_row_is_counted_across_parts(s3):
    _, results, _ = complete(s3)
    phone = by_run(results)["run-phone"]
    assert phone["match_result"] == "MATCH"
    assert phone["match_count"] == 3
    assert phone["data_identifier"] == {U_J1: ["101", "102"], U_F1: ["877"]}


def test_all_four_match_types(s3):
    _, results, _ = complete(s3)
    results = by_run(results)
    assert results["run-email"]["data_identifier"] == {U_J1: ["102"], U_J2: ["140"]}
    assert results["run-name"]["data_identifier"] == {U_M1: ["901"]}
    assert results["run-address"]["data_identifier"] == {U_M1: ["903"]}
    assert results["run-nobody"]["match_result"] == "NO_MATCH"
    assert results["run-nobody"]["match_count"] == 0


def test_invalid_item_is_error_and_others_still_run(s3):
    s3.put(INPUT, json.dumps([item("ok", PHONE, "PHONE"),
                              {"run_id": "bad", "hash_value": "nothex", "match_type": "PHONE"}]).encode())
    summary, results, _ = complete(s3)
    results = by_run(results)
    assert results["ok"]["match_result"] == "MATCH"
    assert results["bad"]["match_result"] == "ERROR" and "64-character" in results["bad"]["error"]
    assert results["bad"]["hash_value"] == "nothex"  # echoed as given
    assert summary["requests_error"] == 1


def test_no_valid_items_scans_nothing(s3):
    s3.put(INPUT, json.dumps([{"run_id": "bad", "hash_value": "x", "match_type": "PHONE"}]).encode())
    summary, results, lines = complete(s3)
    assert summary["files_done"] == 0 and lines == [] and results[0]["match_result"] == "ERROR"


def test_raw_hash_mode_hashes_the_exact_text(s3):
    raw_item = {"run_id": "raw", "hash_value": common.sha256_hex("JANE.DOE@example.com"), "match_type": "EMAIL"}
    # The XML holds "1 MAIN ST, Springfield": only its exact text matches in raw mode.
    s3.put(INPUT, json.dumps([raw_item, item("norm", ADDRESS, "ADDRESS")]).encode())
    _, results, _ = complete(s3, hash_mode="raw")
    results = by_run(results)
    assert results["raw"]["data_identifier"] == {U_J2: ["140"]}
    assert results["norm"]["match_result"] == "NO_MATCH"


def test_bad_hash_mode_rejected(s3):
    with pytest.raises(ValueError, match="hash_mode"):
        run_find(s3, hash_mode="md5")


def test_no_plaintext_in_reports(s3):
    summary, results, lines = complete(s3)
    text = json.dumps([summary, results, lines]).lower()
    for value in ("jane", "main st", "springfield"):  # letters outside hex, so hashes can't collide
        assert value not in text


# ----------------------------------------------------------------------------- files.jsonl grouping
def test_one_line_per_part_merging_all_customers(s3):
    summary, _, lines = complete(s3)
    by_uri = {line["uri"]: line for line in lines}
    assert set(by_uri) == {U_J1, U_J2, U_F1, U_M1}
    assert by_uri[U_J1]["tran_ids"] == ["101", "102"]  # phone + email, row 102 once
    assert by_uri[U_J1]["run_ids"] == ["run-email", "run-phone"]
    assert by_uri[U_M1]["tran_ids"] == ["901", "903"]
    assert summary["files_to_rewrite"] == 4
    assert summary["rows_to_delete"] == 6


def test_part_without_matches_is_not_listed(s3):
    s3.put(INPUT, json.dumps([item("p", PHONE, "PHONE")]).encode())
    _, _, lines = complete(s3)
    assert {line["uri"] for line in lines} == {U_J1, U_F1}


# ----------------------------------------------------------------------------- failures
def test_corrupt_part_is_reported_and_left_out(s3):
    bad = uri_of("2026-04-01/D_04_part_0001")
    s3.put(bad, b"not a gzip file")
    summary, results, lines = complete(s3)
    assert summary["files_failed"] == 1 and summary["failed_files"][0]["uri"] == bad
    assert bad not in {line["uri"] for line in lines}
    results = by_run(results)
    assert results["run-phone"]["match_result"] == "MATCH"  # found elsewhere
    assert results["run-nobody"]["match_result"] == "ERROR"  # can't confirm NO_MATCH
    assert "could not be scanned" in results["run-nobody"]["error"]


def test_wrong_inner_csv_name_fails_that_part(s3):
    from conftest import make_archive, row, xml
    bad = uri_of("2026-04-01/D_04_part_0001")
    s3.put(bad, make_archive("D_04", [row("1", xml())]))  # D_04.csv instead of D_04_part_0001.csv
    summary = run_find(s3)
    assert "D_04_part_0001.csv" in summary["failed_files"][0]["error"]


def test_retry_failed_rescans_after_fix(s3):
    bad = uri_of("2026-04-01/D_04_part_0001")
    s3.put(bad, b"broken")
    assert run_find(s3)["files_failed"] == 1
    from conftest import make_archive, row, xml
    s3.put(bad, make_archive("D_04_part_0001", [row("777", xml(phone="12025550134"))]))
    summary = run_find(s3, retry_failed="true")
    assert summary["status"] == "COMPLETE" and summary["files_failed"] == 0
    _, results, _ = find_outputs(s3)
    assert by_run(results)["run-phone"]["data_identifier"][bad] == ["777"]
    assert by_run(results)["run-nobody"]["match_result"] == "NO_MATCH"


def test_doctype_rejected():
    with pytest.raises(ValueError, match="document types"):
        find_strategies.find_hits('<!DOCTYPE x [<!ENTITY e "y">]><a/>', {}, {}, "normalized")


@pytest.mark.parametrize("batch_id", ["", "has space", "x" * 129])
def test_bad_batch_id_rejected(s3, batch_id):
    with pytest.raises(ValueError, match="batch_id"):
        run_find(s3, batch_id=batch_id)


# ----------------------------------------------------------------------------- months
@pytest.mark.parametrize(
    "month_from, month_to, expected",
    [
        (None, None, [J1, J2, F1, M1]),
        ("2026-02", None, [F1, M1]),
        (None, "2026-01", [J1, J2]),
        ("2026-02", "2026-02", [F1]),
        ("2026-01", "2026-03", [J1, J2, F1, M1]),
        ("2027-01", None, []),
    ],
)
def test_select_archives_by_month(s3, month_from, month_to, expected):
    uris, skipped, _ = common.select_archives(s3, SOURCE, month_from, month_to)
    assert uris == [uri_of(p) for p in expected]
    assert skipped == ["misc"]


def test_month_range_limits_the_scan(s3):
    summary, results, _ = complete(s3, month_from="2026-02", month_to="2026-03")
    assert summary["files_total"] == 2 and summary["month_from"] == "2026-02"
    assert by_run(results)["run-phone"]["data_identifier"] == {U_F1: ["877"]}


def test_inverted_range_rejected(s3):
    with pytest.raises(ValueError, match="after"):
        run_find(s3, month_from="2026-03", month_to="2026-01")


@pytest.mark.parametrize("value", ["2026-13", "2026-1", "Jan", "2026-01-01"])
def test_bad_months_rejected(value):
    with pytest.raises(ValueError):
        common.parse_month(value, "--month_from")


def test_archive_outside_prefix_rejected(s3):
    with pytest.raises(ValueError, match="must be under"):
        run_find(s3, archive_uri="s3://lake/OTHER/")


def test_single_part_file(s3):
    summary, _, lines = complete(s3, archive_uri=U_F1)
    assert summary["files_total"] == 1 and [line["uri"] for line in lines] == [U_F1]


def test_many_parts_in_one_month(s3):
    from conftest import add_archives, row, xml
    parts = {f"2026-05-01/D_05_part_{n:04d}": [row(f"5{n:03d}", xml(phone="12025550134")), row(f"6{n:03d}", xml())]
             for n in range(1, 21)}
    add_archives(s3, parts)
    _, results, lines = complete(s3, month_from="2026-05", month_to="2026-05", files_per_wave="7")
    assert len(lines) == 20
    assert by_run(results)["run-phone"]["match_count"] == 20


def test_hash_helper_matches_known_value():
    # Same value as examples/input.json (PHONE "+1 (202) 555-0134").
    assert hashed(PHONE, "PHONE") == "3382c7633f018e63e40b46a20b112be10d9fa0624d6d8f2f73ef65432475493f"


def test_parse_args_reads_glue_style_arguments():
    args = find.parse_args(["--region", "us-east-1", "--source_prefix", SOURCE, "--result_prefix", "s3://j/r/",
                            "--stop_uri", "s3://j/c/STOP", "--batch_id", "B", "--JOB_ID", "x", "--month_from", "2026-01"])
    assert args.batch_id == "B" and args.month_from == "2026-01" and args.files_per_wave == 100
