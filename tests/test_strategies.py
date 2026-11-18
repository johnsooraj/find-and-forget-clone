"""Strategy pattern: layer 1 picks the strategy, layer 2 finds with it, layer 3 forgets with it."""
import pytest

import arguments
import find_strategies
import forget_strategies
from conftest import (
    INPUT, FakeS3, add_archives, base, find_args, find_outputs, read_archive, row, run_find, run_forget, uri_of, xml,
)

S1, S2, S3_ = "2026-01-01/S_01_part_0001", "2026-01-01/S_01_part_0002", "2026-02-01/S_02_part_0001"
SUFFIX_ARCHIVES = {
    S1: [row("100_c", xml()), row("101", xml()), row("102_c", xml())],
    S2: [row("200", xml()), row("201_C", xml())],  # case-sensitive: _C is not _c
    S3_: [row("300_c", "<this is not xml"), row("301", "<also not xml")],  # XML is never read
}


@pytest.fixture
def lake():
    client = FakeS3()
    add_archives(client, SUFFIX_ARCHIVES)
    client.put_log.clear()
    return client


def suffix_find(s3, **overrides):
    return run_find(s3, find_strategy="TRAN_ID_SUFFIX", input_uri=None, **overrides)


def source_puts(s3):
    return [uri for uri in s3.put_log if uri.startswith("s3://lake/")]


# ----------------------------------------------------------------------------- layer 1
def test_unknown_find_strategy_rejected(lake):
    with pytest.raises(ValueError, match="find_strategy"):
        run_find(lake, find_strategy="GUESS")


def test_unknown_forget_strategy_rejected(lake):
    with pytest.raises(ValueError, match="forget_strategy"):
        run_forget(lake, forget_strategy="SHRED")


def test_strategy_names_are_case_insensitive():
    config = arguments.find_config(find_args(find_strategy="tran_id_suffix", input_uri=None))
    assert config.find_strategy == "TRAN_ID_SUFFIX"


def test_dynamodb_tracker_needs_a_table():
    with pytest.raises(ValueError, match="tracker_table"):
        arguments.find_config(find_args(tracker="DYNAMODB"))


@pytest.mark.parametrize("suffix", [" _c", "_c/x", "x" * 33])
def test_bad_suffix_rejected(suffix):
    with pytest.raises(ValueError, match="tran_id_suffix"):
        arguments.find_config(find_args(tran_id_suffix=suffix))


def test_empty_suffix_means_not_given():
    assert arguments.find_config(find_args(tran_id_suffix="")).tran_id_suffix is None


def test_glue_style_arguments_for_both_layers():
    fixed = ["--region", "us-east-1", "--source_prefix", "s3://l/a/", "--result_prefix", "s3://j/r/",
             "--stop_uri", "s3://j/c/STOP", "--batch_id", "B", "--JOB_ID", "x"]
    find_config = arguments.parse_find_args(fixed + ["--find_strategy", "TRAN_ID_SUFFIX", "--tran_id_suffix", "_z"])
    assert find_config.find_strategy == "TRAN_ID_SUFFIX" and find_config.tran_id_suffix == "_z"
    forget_config = arguments.parse_forget_args(fixed + ["--forget_strategy", "REPLACE_TAG_FOR_LIFECYCLE"])
    assert forget_config.forget_strategy == "REPLACE_TAG_FOR_LIFECYCLE"
    assert forget_config.base == "s3://j/r/B/"


# ----------------------------------------------------------------------------- TRAN_ID_SUFFIX find
def test_suffix_find_lists_only_tran_ids_ending_in_c(lake):
    summary = suffix_find(lake)
    assert summary["status"] == "COMPLETE" and summary["find_strategy"] == "TRAN_ID_SUFFIX"
    _, results, lines = find_outputs(lake)
    assert results["match_result"] == "MATCH" and results["tran_id_suffix"] == "_c"
    assert results["data_identifier"] == {uri_of(S1): ["100_c", "102_c"], uri_of(S3_): ["300_c"]}
    assert {line["uri"]: line["tran_ids"] for line in lines} == {uri_of(S1): ["100_c", "102_c"],
                                                                 uri_of(S3_): ["300_c"]}
    assert all(line["run_ids"] == [] for line in lines)
    assert summary["rows_to_delete"] == 3 and summary["requests_total"] == 1


def test_suffix_is_configurable(lake):
    add_archives(lake, {"2026-03-01/S_03_part_0001": [row("9_x", xml()), row("9_c", xml())]})
    suffix_find(lake, tran_id_suffix="_x")
    _, results, _ = find_outputs(lake)
    assert results["data_identifier"] == {uri_of("2026-03-01/S_03_part_0001"): ["9_x"]}


def test_suffix_without_rows_is_no_match():
    s3 = FakeS3()
    add_archives(s3, {S2: SUFFIX_ARCHIVES[S2]})
    suffix_find(s3)
    _, results, lines = find_outputs(s3)
    assert results["match_result"] == "NO_MATCH" and lines == []


def test_suffix_with_a_failed_file_cannot_confirm_no_match():
    s3 = FakeS3()
    add_archives(s3, {S2: SUFFIX_ARCHIVES[S2]})
    s3.put(uri_of("2026-04-01/S_04_part_0001"), b"broken")
    suffix_find(s3)
    _, results, _ = find_outputs(s3)
    assert results["match_result"] == "ERROR" and "could not be scanned" in results["error"]


def test_suffix_rejects_hash_only_arguments(lake):
    with pytest.raises(ValueError, match="does not read an input"):
        run_find(lake, find_strategy="TRAN_ID_SUFFIX", input_uri=INPUT)
    with pytest.raises(ValueError, match="hash_mode"):
        suffix_find(lake, hash_mode="raw")


def test_hash_match_rejects_the_suffix_argument(s3):
    with pytest.raises(ValueError, match="tran_id_suffix"):
        run_find(s3, tran_id_suffix="_c")


def test_continuing_with_another_strategy_is_rejected(lake):
    suffix_find(lake, files_per_wave="1", max_waves="1")
    with pytest.raises(ValueError, match="differs"):
        run_find(lake, find_strategy="HASH_MATCH", input_uri=None)
    with pytest.raises(ValueError, match="differs"):
        run_find(lake, input_uri=None, tran_id_suffix="_x")
    # Continuing with only the batch ID keeps the stored strategy.
    assert run_find(lake, input_uri=None)["status"] == "COMPLETE"
    assert lake.read_json(base() + "batch.json")["find_strategy"] == "TRAN_ID_SUFFIX"


def test_suffix_waves_match_a_single_run(lake):
    suffix_find(lake, batch_id="ONE")
    for _ in range(3):
        suffix_find(lake, batch_id="WAVES", files_per_wave="1", max_waves="1")
    assert find_outputs(lake, "ONE")[1:] == find_outputs(lake, "WAVES")[1:]


# ----------------------------------------------------------------------------- forget is strategy-agnostic
def test_suffix_find_then_forget_removes_only_those_rows_one_put_per_file(lake):
    suffix_find(lake)
    lake.put_log.clear()
    report, error = run_forget(lake)
    assert error is None and report["status"] == "COMPLETE" and report["files_done"] == 2
    assert sorted(source_puts(lake)) == sorted([uri_of(S1), uri_of(S3_)])
    assert [r["TRAN_ID"] for r in read_archive(lake.read(uri_of(S1)))[2]] == ["101"]
    assert [r["TRAN_ID"] for r in read_archive(lake.read(uri_of(S3_)))[2]] == ["301"]
    assert len(lake.versions(uri_of(S2))) == 1  # untouched


def test_lifecycle_strategy_tags_only_the_old_version(lake):
    suffix_find(lake)
    report, error = run_forget(lake, forget_strategy="REPLACE_TAG_FOR_LIFECYCLE")
    assert error is None and all(f["lifecycle_tagged"] for f in report["files"])
    assert report["forget_strategy"] == "REPLACE_TAG_FOR_LIFECYCLE"
    old, new = lake.versions(uri_of(S1))
    assert old["tags"] == {forget_strategies.SUPERSEDED_TAG: "true"}
    assert new["tags"] == {}
    assert len(lake.versions(uri_of(S1))) == 2  # kept until the lifecycle rule expires it


def test_lifecycle_tag_keeps_existing_tags(lake):
    old_version = lake.latest(uri_of(S1))
    old_version["tags"] = {"owner": "lake"}
    suffix_find(lake)
    run_forget(lake, forget_strategy="REPLACE_TAG_FOR_LIFECYCLE")
    assert lake.versions(uri_of(S1))[0]["tags"] == {"owner": "lake", forget_strategies.SUPERSEDED_TAG: "true"}


def test_lifecycle_strategy_on_unversioned_bucket():
    s3 = FakeS3(versioned=False)
    add_archives(s3, {S1: SUFFIX_ARCHIVES[S1]})
    suffix_find(s3)
    report, error = run_forget(s3, forget_strategy="REPLACE_TAG_FOR_LIFECYCLE")
    assert error is None and report["files"][0]["lifecycle_tagged"] is False
    assert s3.tag_log == []


def test_switching_forget_strategy_runs_only_the_post_step(lake):
    suffix_find(lake)
    run_forget(lake)  # REPLACE_KEEP_VERSIONS
    lake.put_log.clear()
    report, error = run_forget(lake, forget_strategy="REPLACE_PURGE_OLD_VERSIONS")
    assert error is None and report["files_done"] == 2
    assert source_puts(lake) == []  # no second rewrite
    assert len(lake.versions(uri_of(S1))) == 1
    report, _ = run_forget(lake, forget_strategy="REPLACE_PURGE_OLD_VERSIONS")
    assert report["files_skipped"] == 2


def test_hash_find_then_lifecycle_forget(s3):
    run_find(s3)
    report, error = run_forget(s3, forget_strategy="REPLACE_TAG_FOR_LIFECYCLE")
    assert error is None and report["files_done"] == 4


# ----------------------------------------------------------------------------- registries
def test_registries_list_every_strategy():
    assert set(find_strategies.FIND_STRATEGIES) == {"HASH_MATCH", "TRAN_ID_SUFFIX"}
    assert set(forget_strategies.FORGET_STRATEGIES) == {
        "REPLACE_KEEP_VERSIONS", "REPLACE_PURGE_OLD_VERSIONS", "REPLACE_TAG_FOR_LIFECYCLE"}
    for name, cls in find_strategies.FIND_STRATEGIES.items():
        assert cls.name == name
    for name, cls in forget_strategies.FORGET_STRATEGIES.items():
        assert cls.name == name


def test_fingerprint_differs_by_input_and_settings():
    strategy = find_strategies.get("HASH_MATCH")
    a = strategy.fingerprint({"hash_mode": "normalized"}, '"e1"')
    assert a == strategy.fingerprint({"hash_mode": "normalized"}, '"e1"')
    assert a != strategy.fingerprint({"hash_mode": "normalized"}, '"e2"')
    assert a != strategy.fingerprint({"hash_mode": "raw"}, '"e1"')


def test_default_find_strategy_is_hash_match(s3):
    assert run_find(s3)["find_strategy"] == "HASH_MATCH"
    assert s3.read_json(base() + "batch.json")["strategy_settings"]["hash_mode"] == "normalized"
