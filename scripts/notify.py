"""Notifications: one common email template for every job event. PLACEHOLDER.

Events sent from inside the Glue jobs (find.py / forget.py `execute`):
  JOB_STARTED   the run started (after the pipeline triggered it)
  JOB_FINISHED  the run ended normally (status COMPLETE / IN_PROGRESS / STOPPED)
  JOB_FAILED    the run raised an error

A run that is KILLED, times out or is stuck cannot report itself. That is
covered outside the job by the EventBridge "Glue Job State Change" rule in
main.tf (placeholder) and the `post` blocks of the Jenkinsfiles.

TODO (details to come): pick the transport (SES or SNS), the sender, the
recipients and the final wording. Until then the LOG notifier prints the
rendered message to the job log. A notification never fails a job.

The message must never contain customer data: only batch IDs, counts,
statuses and S3 locations of the reports.

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
from abc import ABC, abstractmethod
from string import Template

import common

JOB_STARTED, JOB_FINISHED, JOB_FAILED = "JOB_STARTED", "JOB_FINISHED", "JOB_FAILED"
EVENTS = (JOB_STARTED, JOB_FINISHED, JOB_FAILED)
LOG, SES, SNS = "LOG", "SES", "SNS"
NOTIFIERS = (LOG, SES, SNS)

# ---------------------------------------------------------------- the common template (placeholder wording)
SUBJECT = Template("[drop-glue] $job $event - batch $batch_id ($status)")
BODY = Template("""\
drop-glue notification

Event:      $event
Job:        $job
Batch:      $batch_id
Strategy:   $strategy
Status:     $status
Time (UTC): $timestamp

Counts:
$counts

Details:    $details_uri
Error:      $error

-- TODO: final wording, links to the Jenkins status job and runbook.
""")


def render(event, context):
    """Return (subject, body). Missing values render as '-'."""
    if event not in EVENTS:
        raise ValueError(f"Unknown notification event {event!r}")
    counts = context.get("counts") or {}
    values = {key: "-" for key in ("job", "batch_id", "strategy", "status", "details_uri", "error")}
    values.update({k: str(v) for k, v in context.items() if v not in (None, "") and k != "counts"})
    values.update(event=event, timestamp=common.now(),
                  counts="\n".join(f"  {k}: {v}" for k, v in sorted(counts.items())) or "  -")
    return SUBJECT.safe_substitute(values), BODY.safe_substitute(values)


class Notifier(ABC):
    @abstractmethod
    def send(self, subject, body, recipients):
        """Deliver one message. May raise; `notify` catches it."""


class LogNotifier(Notifier):
    """Default until the transport is decided: print to the job log."""

    def send(self, subject, body, recipients):
        print(f"NOTIFY to={recipients or '-'} | {subject}\n{body}")


class SesNotifier(Notifier):
    def send(self, subject, body, recipients):
        # TODO: boto3.client("ses").send_email(Source=..., Destination={"ToAddresses": recipients}, ...)
        raise NotImplementedError("TODO: SES notifier is a placeholder")


class SnsNotifier(Notifier):
    def send(self, subject, body, recipients):
        # TODO: boto3.client("sns").publish(TopicArn=recipients[0], Subject=subject[:100], Message=body)
        raise NotImplementedError("TODO: SNS notifier is a placeholder")


def make_notifier(name):
    return {LOG: LogNotifier, SES: SesNotifier, SNS: SnsNotifier}[name]()


def notify(notifier, event, context, recipients=""):
    """Render and send; never raises. Returns True when sent."""
    if notifier is None:
        return False
    try:
        subject, body = render(event, context)
        notifier.send(subject, body, [r.strip() for r in recipients.split(",") if r.strip()])
        return True
    except Exception as exc:  # noqa: BLE001 - a notification must never fail the job
        print(f"WARNING: notification {event} not sent: {exc}")
        return False


def run_with_notifications(notifier, recipients, context, run):
    """Send JOB_STARTED, call run(), then JOB_FINISHED with its summary or JOB_FAILED; re-raise errors."""
    notify(notifier, JOB_STARTED, dict(context, status="STARTED"), recipients)
    try:
        summary = run()
    except Exception as exc:
        notify(notifier, JOB_FAILED, dict(context, status="FAILED", error=(str(exc) or type(exc).__name__)[:500]),
               recipients)
        raise
    summary = summary or {}
    counts = {k: summary[k] for k in summary
              if k.startswith(("files_", "rows_", "requests_")) and not isinstance(summary[k], (list, dict))}
    notify(notifier, JOB_FINISHED, dict(context, status=summary.get("status"), counts=counts), recipients)
    return summary
