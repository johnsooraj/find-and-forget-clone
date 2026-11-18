"""The examples/ files and the Formats code must agree. If a format changes,
edit examples/ first; these tests show what else needs updating."""
import json

import pytest

import formats
from conftest import EXAMPLES


def load(name):
    return json.loads((EXAMPLES / name).read_text())


def lines(name):
    return [line for line in (EXAMPLES / name).read_text().splitlines() if line.strip()]


def test_example_input_parses_without_errors():
    requests = formats.parse_input(load("input.json"))
    assert len(requests) == 4
    assert all(r.error is None for r in requests)
    assert {r.match_type for r in requests} == set(formats.MATCH_TYPES)


def test_example_results_match_result_entry():
    for example in load("results.json"):
        request = formats.Request(0, example["run_id"], example["hash_value"], example["match_type"])
        built = formats.result_entry(request, example["match_result"], example["data_identifier"], example.get("error"))
        assert built == example


def test_example_files_jsonl_round_trips():
    for number, line in enumerate(lines("files.jsonl"), 1):
        assert formats.file_line(formats.parse_file_line(line, number)) == json.loads(line)


def test_example_wave_round_trips():
    for number, line in enumerate(lines("wave-00001.jsonl"), 1):
        assert formats.wave_line(formats.parse_wave_line(line, number)) == json.loads(line)


def test_example_suffix_results_match_builder():
    example = load("results_suffix.json")
    built = formats.suffix_result(example["tran_id_suffix"], example["match_result"], example["data_identifier"],
                                  example.get("error"))
    assert built == example


def test_example_batch_matches_builder():
    example = load("batch.json")
    built = formats.build_batch("B", "HASH_MATCH", {"hash_mode": "normalized"}, "s3://j/i.json", '"e"', "s3://l/a/",
                                "2026-01", "2026-03", 1, [], "HASH_MATCH#abc", 0)
    assert set(built) == set(example)
    assert formats.parse_batch(example, example["batch_id"]) == example


def test_schema_3_batch_is_read_as_hash_match():
    old = {"schema_version": "3", "batch_id": "B", "input_uri": "s3://j/i.json", "input_etag": '"e"',
           "archive_uri": "s3://l/a/", "hash_mode": "raw", "files_total": 1}
    batch = formats.parse_batch(old, "B")
    assert batch["find_strategy"] == "HASH_MATCH" and batch["strategy_settings"] == {"hash_mode": "raw"}
    assert batch["tracker_scope"] is None


def test_example_summary_matches_builder():
    example = load("summary.json")
    batch = load("batch.json")
    counts = {"total": 4, "match": 2, "no_match": 1, "error": 1}
    assert set(formats.build_summary("B", batch, formats.COMPLETE, 0, [], [], counts)) == set(example)
    in_progress = formats.build_summary("B", batch, formats.IN_PROGRESS, 0, [], [], None)
    assert set(in_progress) == set(example) and in_progress["requests_match"] is None


def test_example_remove_summary_matches_builder():
    example = load("remove_summary.json")
    built = formats.build_remove_summary("B", "REPLACE_PURGE_OLD_VERSIONS", example["files"])
    assert set(built) == set(example)
    assert built["status"] == example["status"] == formats.IN_PROGRESS


@pytest.mark.parametrize("statuses, expected", [
    (["DONE", "SKIPPED"], formats.COMPLETE),
    (["DONE", "PENDING"], formats.IN_PROGRESS),
    (["DONE", "FAILED", "PENDING"], formats.INCOMPLETE),
    ([], formats.COMPLETE),
])
def test_remove_summary_status(statuses, expected):
    outcomes = [{"uri": str(i), "status": s} for i, s in enumerate(statuses)]
    assert formats.build_remove_summary("B", "REPLACE_KEEP_VERSIONS", outcomes)["status"] == expected


@pytest.mark.parametrize("document", [{}, "x", [], None])
def test_wrong_overall_shape_fails(document):
    with pytest.raises(ValueError):
        formats.parse_input(document)


def test_bad_items_become_errors_not_failures():
    good = {"run_id": "r1", "hash_value": "A" * 64, "match_type": "phone"}
    requests = formats.parse_input([
        good,
        "not an object",
        {"run_id": "r2", "hash_value": "abc", "match_type": "PHONE"},
        {"run_id": "r3", "hash_value": "a" * 64, "match_type": "SSN"},
        {"hash_value": "a" * 64, "match_type": "EMAIL"},
    ])
    assert requests[0].error is None
    assert requests[0].hash_key == "a" * 64  # uppercase accepted, lowercased
    assert requests[0].match_type == "PHONE"
    assert [r.error is not None for r in requests] == [False, True, True, True, True]
    assert "64-character" in requests[2].error
    assert "match_type" in requests[3].error
    assert "run_id" in requests[4].error


def test_unknown_schema_version_rejected():
    line = json.dumps({"schema_version": "99", "uri": "s3://b/k.tar.gz", "etag": '"ab"',
                       "version_id": None, "tran_ids": ["1"], "run_ids": []})
    with pytest.raises(ValueError, match="schema_version"):
        formats.parse_file_line(line, 1)
