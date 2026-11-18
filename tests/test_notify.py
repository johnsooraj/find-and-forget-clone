"""Notifications (placeholder): one template, three events, never fails a job."""
import pytest

import find
import forget
import notify
from conftest import find_args, forget_args, local_runner


class Recorder(notify.Notifier):
    def __init__(self):
        self.sent = []

    def send(self, subject, body, recipients):
        self.sent.append((subject, body, recipients))


def events(recorder):
    return [subject.split(" ")[2] for subject, _, _ in recorder.sent]


def test_template_renders_every_placeholder():
    subject, body = notify.render(notify.JOB_FINISHED, {"job": "FIND", "batch_id": "B1", "strategy": "HASH_MATCH",
                                                        "status": "COMPLETE", "counts": {"files_done": 5}})
    assert subject == "[drop-glue] FIND JOB_FINISHED - batch B1 (COMPLETE)"
    assert "files_done: 5" in body and "HASH_MATCH" in body
    assert "$" not in subject + body


def test_missing_values_render_as_dash():
    subject, body = notify.render(notify.JOB_STARTED, {})
    assert "batch - (-)" in subject and "$" not in body


def test_unknown_event_rejected():
    with pytest.raises(ValueError):
        notify.render("JOB_EXPLODED", {})


def test_find_sends_started_and_finished(s3):
    recorder = Recorder()
    summary = find.execute(s3, find_args(notify_to="a@x, b@x"), local_runner(s3), recorder)
    assert summary["status"] == "COMPLETE"
    assert events(recorder) == ["JOB_STARTED", "JOB_FINISHED"]
    subject, body, recipients = recorder.sent[-1]
    assert "(COMPLETE)" in subject and recipients == ["a@x", "b@x"]
    assert "rows_to_delete: 6" in body
    assert "jane" not in body.lower()  # never customer data


def test_forget_failure_sends_failed_and_still_raises(s3):
    recorder = Recorder()
    with pytest.raises(Exception):
        forget.execute(s3, forget_args(batch_id="NEVER_RAN"), local_runner(s3), recorder)
    assert events(recorder) == ["JOB_STARTED", "JOB_FAILED"]
    assert "(FAILED)" in recorder.sent[-1][0]


def test_a_broken_notifier_never_fails_the_job(s3):
    class Broken(notify.Notifier):
        def send(self, subject, body, recipients):
            raise RuntimeError("smtp down")

    assert find.execute(s3, find_args(), local_runner(s3), Broken())["status"] == "COMPLETE"


@pytest.mark.parametrize("name", ["SES", "SNS"])
def test_placeholder_transports_do_not_send_yet(name):
    assert notify.notify(notify.make_notifier(name), notify.JOB_STARTED, {"batch_id": "B"}) is False


def test_log_notifier_prints(capsys):
    assert notify.notify(notify.make_notifier("LOG"), notify.JOB_STARTED, {"batch_id": "B"}) is True
    assert "JOB_STARTED" in capsys.readouterr().out
