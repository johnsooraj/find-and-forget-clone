"""Throttling (waves), resumable batches and the emergency stop switch."""
import json

import pytest

import find
from conftest import (
    INPUT, STOP, add_archives, base, find_args, find_outputs, local_runner, row, run_find, run_forget, uri_of, xml,
)


def wave_files(s3, batch_id="B1"):
    prefix = base(batch_id).replace("s3://jobs/", "") + "waves/"
    return sorted(k for (b, k) in s3.store if b == "jobs" and k.startswith(prefix))


# ----------------------------------------------------------------------------- find waves
def test_max_waves_stops_the_run_and_the_next_run_continues(s3):
    summary = run_find(s3, files_per_wave="1", max_waves="2")
    assert summary["status"] == "IN_PROGRESS"
    assert summary["files_done"] == 2 and summary["files_total"] == 4
    assert summary["requests_match"] is None
    assert not s3.exists(base() + "results.json")
    summary = run_find(s3, files_per_wave="1", max_waves="2")
    assert summary["status"] == "COMPLETE" and summary["files_done"] == 4
    assert len(wave_files(s3)) == 4


def test_five_waves_of_100_cap_a_run_at_500_files(s3):
    parts = {f"2026-06-01/D_06_part_{n:04d}": [row(f"{n}", xml())] for n in range(1, 701)}
    add_archives(s3, parts)
    summary = run_find(s3, month_from="2026-06", month_to="2026-06", files_per_wave="100", max_waves="5")
    assert summary["files_done"] == 500 and summary["files_total"] == 700
    assert len(wave_files(s3)) == 5
    assert run_find(s3, files_per_wave="100", max_waves="5")["status"] == "COMPLETE"


def test_waves_give_the_same_results_as_one_run(s3):
    run_find(s3, batch_id="ONE")
    for _ in range(4):
        run_find(s3, batch_id="WAVES", files_per_wave="1", max_waves="1")
    one, waves = find_outputs(s3, "ONE"), find_outputs(s3, "WAVES")
    assert one[1] == waves[1]  # results.json
    assert one[2] == waves[2]  # files.jsonl


def test_continue_with_only_the_batch_id(s3):
    run_find(s3, files_per_wave="1", max_waves="1", month_from="2026-01", month_to="2026-02")
    summary = run_find(s3, input_uri=None, archive_uri=None, month_from=None, month_to=None)
    assert summary["status"] == "COMPLETE" and summary["files_total"] == 3


def test_changed_settings_rejected_when_continuing(s3):
    run_find(s3, files_per_wave="1", max_waves="1", month_from="2026-01")
    with pytest.raises(ValueError, match="differs"):
        run_find(s3, month_from="2026-02")
    with pytest.raises(ValueError, match="differs"):
        run_find(s3, hash_mode="raw")


def test_changed_input_rejected_when_continuing(s3):
    run_find(s3, files_per_wave="1", max_waves="1")
    s3.put(INPUT, json.dumps([]).encode())
    with pytest.raises(ValueError, match="input file changed"):
        run_find(s3)


def test_complete_batch_is_not_run_again(s3):
    run_find(s3)
    s3.put_log.clear()
    assert run_find(s3)["status"] == "COMPLETE"
    assert s3.put_log == []


def test_new_batch_needs_input_and_archive(s3):
    with pytest.raises(ValueError, match="new batch"):
        run_find(s3, input_uri=None)


@pytest.mark.parametrize("field, value", [("files_per_wave", "0"), ("files_per_wave", "x"), ("max_waves", "-1")])
def test_bad_throttle_values_rejected(s3, field, value):
    with pytest.raises(ValueError, match=field):
        run_find(s3, **{field: value})


# ----------------------------------------------------------------------------- stop switch
def test_stop_switch_blocks_a_new_run(s3):
    s3.put(STOP, b"{}")
    s3.put_log.clear()
    assert run_find(s3)["status"] == "STOPPED"
    assert s3.put_log == []  # nothing written


def test_stop_switch_ends_the_run_after_the_current_wave(s3):
    def press_stop(wave_number):
        if wave_number == 1:
            s3.put(STOP, b"{}")

    summary = run_find(s3, after_wave=press_stop, files_per_wave="1")
    assert summary["status"] == "IN_PROGRESS" and summary["files_done"] == 1
    s3.remove(STOP)  # RESUME
    assert run_find(s3)["status"] == "COMPLETE"
    single = find_outputs(s3)
    run_find(s3, batch_id="REF")
    assert find_outputs(s3, "REF")[1] == single[1]


def test_killed_mid_wave_loses_at_most_that_wave(s3):
    """A KILL stops the Glue run inside a wave: nothing from that wave is saved."""
    class Killed(Exception):
        pass

    def kill(wave_number):
        if wave_number == 2:
            raise Killed()

    with pytest.raises(Killed):
        find.run(s3, find_args(files_per_wave="1"), local_runner(s3, kill))
    assert len(wave_files(s3)) == 1
    assert run_find(s3, files_per_wave="1")["status"] == "COMPLETE"
    assert len(wave_files(s3)) == 4


# ----------------------------------------------------------------------------- forget waves and stop
def test_forget_waves_and_continue(s3):
    run_find(s3)
    report, error = run_forget(s3, files_per_wave="1", max_waves="2")
    assert error is None and report["status"] == "IN_PROGRESS"
    assert report["files_done"] == 2 and report["files_pending"] == 2
    report, error = run_forget(s3, files_per_wave="1", max_waves="2")
    assert report["status"] == "COMPLETE" and report["files_done"] == 2 and report["files_skipped"] == 2


def test_forget_refuses_an_unfinished_find(s3):
    run_find(s3, files_per_wave="1", max_waves="1")
    with pytest.raises(ValueError, match="not COMPLETE"):
        run_forget(s3)


def test_forget_honours_the_stop_switch(s3):
    run_find(s3)
    s3.put(STOP, b"{}")
    s3.put_log.clear()
    from conftest import forget_args
    import forget
    assert forget.run(s3, forget_args(), local_runner(s3))["status"] == "STOPPED"
    assert s3.put_log == []


def test_forget_stop_after_current_wave(s3):
    run_find(s3)

    def press_stop(wave_number):
        if wave_number == 1:
            s3.put(STOP, b"{}")

    report, _ = run_forget(s3, after_wave=press_stop, files_per_wave="1")
    assert report["files_done"] == 1 and report["files_pending"] == 3
    s3.remove(STOP)
    report, _ = run_forget(s3)
    assert report["status"] == "COMPLETE"


def test_retry_failed_not_allowed_after_forget_started(s3):
    bad = uri_of("2026-04-01/D_04_part_0001")
    s3.put(bad, b"broken")
    run_find(s3)
    run_forget(s3)
    s3.put_log.clear()
    assert run_find(s3, retry_failed="true")["status"] == "COMPLETE"
    assert s3.put_log == []  # files.jsonl must not change once deletion has started
