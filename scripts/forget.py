"""drop-glue FORGET (layer 3): Glue Spark job (driver) that deletes the found records.

Reads ONLY Layer 2's report, <result_prefix><batch_id>/files.jsonl (one line
per part file), so it works the same for every find strategy. For each part,
ONCE: loads it, drops every listed TRAN_ID, and saves a new .tar.gz with the
same S3 key and the same CSV name inside (see tasks.py). What then happens to
the old S3 version is the FORGET STRATEGY (forget_strategies.py), picked with
--forget_strategy (the Jenkins FORGET_STRATEGY drop-down).

One Glue run processes at most MAX_WAVES waves of FILES_PER_WAVE parts (one
Spark task per part), then stops. Running it again with the same BATCH_ID
continues: finished parts have a completion marker and are skipped. Writes
remove_summary.json after every run. Rewritten parts are recorded as CLEANED
in the processed-file tracker. The stop switch is checked at start and before
every wave.
"""
import hashlib
import time

import arguments
import common
import forget_strategies
import formats
import notify
import tasks
import tracking


def read_files(s3, uri, source_prefix):
    entries, seen = [], set()
    for number, line in enumerate(common.get_lines(s3, uri), 1):
        hits = formats.parse_file_line(line, number)
        if not common.under_prefix(hits.uri, source_prefix):
            raise ValueError(f"files.jsonl line {number}: part file is outside {source_prefix}")
        if hits.uri in seen:
            raise ValueError(f"files.jsonl line {number}: duplicate part file")
        seen.add(hits.uri)
        entries.append(hits)
    return entries


def marker_name(uri):
    return hashlib.sha256(uri.encode("utf-8")).hexdigest() + ".json"


def tracker_scope(s3, base, batch_id):
    """The tracker scope Find stored in batch.json (None for old batches)."""
    try:
        return formats.parse_batch(common.get_json(s3, base + "batch.json"), batch_id).get("tracker_scope")
    except Exception as exc:  # noqa: BLE001 - tracking is an optimization
        print(f"WARNING: batch.json unreadable, rewritten files are not tracked: {exc}")
        return None


def cleaned_records(results):
    return [{"uri": o["uri"], "etag": o.get("new_etag"), "version_id": o.get("new_version_id"),
             "stage": tracking.CLEANED}
            for o in results if o["status"] == "DONE" and o.get("new_etag")]


def run(s3, args, runner_factory, tracker=None):
    """args: ForgetConfig, or an argparse-style namespace (validated by layer 1)."""
    config = arguments.forget_config(args)
    strategy = forget_strategies.get(config.forget_strategy)
    if tracker is None:
        tracker = tracking.make_tracker(config, s3)
    base = config.base

    if common.stop_requested(s3, config.stop_uri):
        print(f"STOP switch is on ({config.stop_uri}); not starting. Use Jenkinsfile.stop ACTION=RESUME to clear it.")
        return {"status": "STOPPED"}
    summary = formats.parse_summary(common.get_json(s3, base + "summary.json"), config.batch_id)
    if summary["status"] != formats.COMPLETE:
        raise ValueError(f"Find for batch {config.batch_id} is {summary['status']}, not COMPLETE; finish Find first")
    entries = read_files(s3, base + "files.jsonl", config.source_prefix)
    scope = tracker_scope(s3, base, config.batch_id) if tracker else None

    job_bucket, done_prefix = common.split_uri(base + "done/")
    _, done_keys = common.list_keys(s3, job_bucket, done_prefix)
    done = set(done_keys)
    outcomes, pending = {}, []
    for hits in entries:
        key = done_prefix + marker_name(hits.uri)
        if key in done and not strategy.has_post_step:
            outcomes[hits.uri] = {"uri": hits.uri, "rows_expected": len(hits.tran_ids), "status": "SKIPPED",
                                  "reason": "Already completed by an earlier run"}
        else:
            pending.append({"uri": hits.uri, "etag": hits.etag, "version_id": hits.version_id,
                            "tran_ids": sorted(hits.tran_ids), "marker_uri": f"s3://{job_bucket}/{key}",
                            "already_done": key in done})
    print(f"Batch {config.batch_id}: {len(entries)} part files to rewrite, {len(pending)} still to do; "
          f"forget_strategy={strategy.name}; "
          f"this run: up to {config.max_waves or 'unlimited'} waves of {config.files_per_wave}")

    runner = runner_factory({"batch_id": config.batch_id, "forget_strategy": strategy.name, "region": config.region})
    waves = 0
    while pending and (config.max_waves == 0 or waves < config.max_waves):
        if common.stop_requested(s3, config.stop_uri):
            print("STOP switch is on; stopping before the next wave")
            break
        wave, pending = pending[:config.files_per_wave], pending[config.files_per_wave:]
        started = time.time()
        results = runner.map(tasks.forget_task, wave)
        for outcome in results:
            outcomes[outcome["uri"]] = outcome
        tracking.safe_record(tracker, scope, cleaned_records(results), config.batch_id)
        waves += 1
        failed = sum(o["status"] == "FAILED" for o in results)
        print(f"Wave {waves}: {len(results)} parts ({failed} failed) in {time.time() - started:.0f}s")

    for item in pending:
        outcomes[item["uri"]] = {"uri": item["uri"], "rows_expected": len(item["tran_ids"]), "status": "PENDING"}
    report = formats.build_remove_summary(config.batch_id, strategy.name, [outcomes[hits.uri] for hits in entries])
    common.put_json(s3, base + "remove_summary.json", report, create_only=False)
    print(f"{report['status']}: done {report['files_done']}, skipped {report['files_skipped']}, "
          f"failed {report['files_failed']}, pending {report['files_pending']}")
    for outcome in report["files"]:
        if outcome["status"] == "FAILED":
            print(f"FAILED {outcome['uri']}: {outcome['error']}")
    if report["files_failed"]:
        raise RuntimeError(f"{report['files_failed']} part file(s) failed; see {base}remove_summary.json")
    return report


def execute(s3, config, runner_factory, notifier=None, tracker=None):
    """run() wrapped with the JOB_STARTED / JOB_FINISHED / JOB_FAILED notifications."""
    config = arguments.forget_config(config)
    context = {"job": "FORGET", "batch_id": config.batch_id, "strategy": config.forget_strategy,
               "details_uri": config.base + "remove_summary.json"}
    return notify.run_with_notifications(notifier, config.notify_to, context,
                                         lambda: run(s3, config, runner_factory, tracker))


def parse_args(argv=None):
    return arguments.parse_forget_args(argv)


def main(argv=None):
    from pyspark.context import SparkContext

    config = parse_args(argv)
    spark_context = SparkContext.getOrCreate()
    common.raise_csv_limit()
    execute(common.s3_client(config.region), config, lambda shared: common.SparkRunner(spark_context, shared),
            notify.make_notifier(config.notifier))


if __name__ == "__main__":
    main()
