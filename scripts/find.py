"""drop-glue FIND (layer 2): Glue Spark job (driver) that finds the rows to forget.

Which rows are "found" is decided by the FIND STRATEGY (find_strategies.py),
picked with --find_strategy (the Jenkins FIND_STRATEGY drop-down):
  HASH_MATCH      input hashes vs. hashed XML values (needs --input_uri)
  TRAN_ID_SUFFIX  every row whose TRAN_ID ends with --tran_id_suffix (default _c)

One Glue run processes at most MAX_WAVES waves of FILES_PER_WAVE part files
(one Spark task per file), then stops. Running it again with the same
BATCH_ID continues where the last run stopped. Everything is kept under
<result_prefix><batch_id>/:

  batch.json            fixed settings of the batch (first run)
  selected_files.jsonl  the part files in the month range, with tracker skips (first run)
  waves/wave-NNNNN.jsonl  per-file results, one file per wave
  summary.json          IN_PROGRESS while files remain, then COMPLETE
  results.json          the strategy's results (when COMPLETE)
  files.jsonl           one line per part file to rewrite (when COMPLETE): Forget's only input

The processed-file tracker (tracking.py) is checked when a batch is created.
The emergency stop switch (--stop_uri) is checked at start and before every wave.
"""
import json
import time

import arguments
import common
import find_strategies
import formats
import notify
import tasks
import tracking


# ----------------------------------------------------------------------------- batch setup
def open_batch(s3, config, tracker):
    """Create the batch on the first run; on later runs check nothing changed.

    Returns (batch, strategy, inputs, selected entries).
    """
    base = config.base
    batch_uri = base + "batch.json"
    if common.object_exists(s3, batch_uri):
        batch = formats.parse_batch(common.get_json(s3, batch_uri), config.batch_id)
        given = {"find_strategy": config.find_strategy, "input_uri": config.input_uri,
                 "archive_uri": config.archive_uri, "month_from": config.month_from, "month_to": config.month_to}
        for key, value in given.items():
            if value is not None and value != batch.get(key):
                raise ValueError(f"--{key} {value!r} differs from this batch ({batch.get(key)!r}); "
                                 "leave it empty to continue the batch, or start a new BATCH_ID")
        strategy = find_strategies.get(batch["find_strategy"])
        changed = strategy.changed_settings(config, batch["strategy_settings"])
        if changed:
            raise ValueError(f"--{changed[0]} differs from this batch ({batch['strategy_settings']}); "
                             "leave it empty to continue the batch, or start a new BATCH_ID")
        inputs = []
        if strategy.needs_input:
            document, etag = common.get_json_with_etag(s3, batch["input_uri"])
            if etag != batch["input_etag"]:
                raise ValueError("The input file changed since this batch started; start a new BATCH_ID")
            inputs = strategy.parse_input(document)
        selected = [formats.parse_selected_line(line) for line in common.get_lines(s3, base + "selected_files.jsonl")]
        if len(selected) != batch["files_total"]:
            raise ValueError("selected_files.jsonl does not match batch.json")
        print(f"Continuing {strategy.name} batch {config.batch_id} (started {batch.get('created_at')})")
        return batch, strategy, inputs, selected

    strategy = find_strategies.get(config.find_strategy or find_strategies.DEFAULT)
    settings = strategy.new_settings(config)
    if not config.archive_uri or (strategy.needs_input and not config.input_uri):
        raise ValueError(f"A new batch ({strategy.name}) needs "
                         + ("--input_uri and --archive_uri" if strategy.needs_input else "--archive_uri"))
    inputs, input_etag = [], None
    if strategy.needs_input:
        document, input_etag = common.get_json_with_etag(s3, config.input_uri)
        inputs = strategy.parse_input(document)  # fails fast on a wrong overall shape
    uris, skipped, etags = common.select_archives(s3, config.archive_uri, config.month_from, config.month_to)
    scope = f"{strategy.name}#{strategy.fingerprint(settings, input_etag)}"
    tracked = tracking.skippable(tracker, scope, [(uri, etags.get(uri)) for uri in uris])
    selected = [formats.selected_line(uri, etags.get(uri), uri in tracked) for uri in uris]
    common.put_lines(s3, base + "selected_files.jsonl", selected, create_only=False)
    batch = formats.build_batch(config.batch_id, strategy.name, settings, config.input_uri, input_etag,
                                config.archive_uri, config.month_from, config.month_to, len(uris), skipped, scope,
                                len(tracked))
    common.put_json(s3, batch_uri, batch, create_only=True)
    print(f"New {strategy.name} batch {config.batch_id}: {len(uris)} part files in "
          f"{config.month_from or '(start)'}..{config.month_to or '(end)'}; "
          f"{len(tracked)} skipped by the tracker (already processed, unchanged)")
    if skipped:
        print(f"Skipped non-date folders: {', '.join(skipped)}")
    return batch, strategy, inputs, selected


def load_waves(s3, base):
    """Return ({uri: latest outcome}, next wave number)."""
    bucket, prefix = common.split_uri(base + "waves/")
    _, keys = common.list_keys(s3, bucket, prefix)
    outcomes, last = {}, 0
    for key in sorted(keys):
        name = key.rsplit("/", 1)[-1]
        if not (name.startswith("wave-") and name.endswith(".jsonl")):
            continue
        last = max(last, int(name[5:-6]))
        for number, line in enumerate(common.get_lines(s3, f"s3://{bucket}/{key}"), 1):
            outcome = formats.parse_wave_line(line, f"{name} line {number}")
            outcomes[outcome["uri"]] = outcome
    return outcomes, last + 1


def forget_started(s3, base):
    bucket, prefix = common.split_uri(base)
    _, keys = common.list_keys(s3, bucket, prefix + "done/")
    return bool(keys) or common.object_exists(s3, base + "remove_summary.json")


# ----------------------------------------------------------------------------- results
def aggregate(strategy, inputs, outcomes):
    """Return ({match key: {uri: TRAN_IDs}}, [FileHits per part], failed files)."""
    per_key, by_file, failed = {}, {}, []
    for uri in sorted(outcomes):
        outcome = outcomes[uri]
        if outcome["status"] == formats.FAILED:
            failed.append({"uri": uri, "error": outcome.get("error")})
            continue
        for key, tran_ids in outcome["matches"].items():
            try:
                labels = strategy.labels(inputs, key)
            except ValueError as exc:
                raise ValueError(f"{uri}: {exc}") from None
            per_key.setdefault(key, {})[uri] = tran_ids
            hits = by_file.setdefault(uri, formats.FileHits(uri, outcome["etag"], outcome.get("version_id")))
            hits.tran_ids.update(tran_ids)
            hits.run_ids.update(labels)
    return per_key, [by_file[uri] for uri in sorted(by_file)], failed


def no_match_records(lines):
    """Tracker entries for the files of a wave that were scanned and had no matches."""
    return [{"uri": line["uri"], "etag": line["etag"], "version_id": line.get("version_id"),
             "stage": tracking.SCANNED_NO_MATCH}
            for line in lines if line["status"] == formats.OK and not line["matches"]]


# ----------------------------------------------------------------------------- run
def run(s3, args, runner_factory, tracker=None):
    """args: FindConfig, or an argparse-style namespace (validated by layer 1)."""
    config = arguments.find_config(args)
    if tracker is None:
        tracker = tracking.make_tracker(config, s3)
    base = config.base

    if common.stop_requested(s3, config.stop_uri):
        print(f"STOP switch is on ({config.stop_uri}); not starting. Use Jenkinsfile.stop ACTION=RESUME to clear it.")
        return {"status": "STOPPED"}
    if common.object_exists(s3, base + "summary.json"):
        existing = formats.parse_summary(common.get_json(s3, base + "summary.json"), config.batch_id)
        can_retry = config.retry_failed and existing["files_failed"] and not forget_started(s3, base)
        if existing["status"] == formats.COMPLETE and not can_retry:
            print(f"Batch {config.batch_id} is already COMPLETE; nothing to do")
            return existing

    batch, strategy, inputs, selected = open_batch(s3, config, tracker)
    outcomes, next_wave = load_waves(s3, base)
    for entry in selected:  # skipped by the tracker: counts as scanned with no matches
        if entry["tracked"] and entry["uri"] not in outcomes:
            outcomes[entry["uri"]] = {"uri": entry["uri"], "status": formats.OK, "etag": entry["etag"],
                                      "matches": {}, "tracked": True}
    uris = [entry["uri"] for entry in selected]
    remaining = [uri for uri in uris
                 if uri not in outcomes or (config.retry_failed and outcomes[uri]["status"] == formats.FAILED)]
    match_shared = strategy.shared(inputs, batch["strategy_settings"])
    if match_shared is None:
        print("No valid input items: nothing can match, so no files are scanned")
        remaining = []
    if strategy.needs_input:
        valid = sum(1 for r in inputs if not getattr(r, "error", None))
        print(f"{len(inputs)} input items ({valid} valid)")
    print(f"{strategy.name}: {len(uris) - len(remaining)} of {len(uris)} files already done; "
          f"this run: up to {config.max_waves or 'unlimited'} waves of {config.files_per_wave} files")

    runner = runner_factory({"find_strategy": strategy.name, "match": match_shared, "region": config.region})
    waves = 0
    while remaining and (config.max_waves == 0 or waves < config.max_waves):
        if common.stop_requested(s3, config.stop_uri):
            print("STOP switch is on; stopping before the next wave")
            break
        wave, remaining = remaining[:config.files_per_wave], remaining[config.files_per_wave:]
        started = time.time()
        lines = [formats.wave_line(outcome) for outcome in runner.map(tasks.scan_task, [{"uri": u} for u in wave])]
        name = f"wave-{next_wave:05d}.jsonl"
        common.put_lines(s3, f"{base}waves/{name}", lines, create_only=True)
        for number, line in enumerate(lines, 1):
            outcome = formats.parse_wave_line(json.dumps(line), f"{name} line {number}")
            outcomes[outcome["uri"]] = outcome
        tracking.safe_record(tracker, batch.get("tracker_scope"), no_match_records(lines), config.batch_id)
        next_wave += 1
        waves += 1
        elapsed = time.time() - started
        failed = sum(line["status"] == formats.FAILED for line in lines)
        megabytes = sum(line["bytes"] or 0 for line in lines) / 1e6
        busiest = max((line["seconds"] or 0 for line in lines), default=0)
        print(f"{name}: {len(lines)} files ({failed} failed) in {elapsed:.0f}s, {megabytes:.0f} MB "
              f"({megabytes / max(elapsed, 1):.1f} MB/s), slowest file {busiest:.0f}s; "
              f"{len(outcomes)}/{len(uris)} done")

    per_key, file_hits, failed_files = aggregate(strategy, inputs, outcomes)
    if remaining:
        summary = formats.build_summary(config.batch_id, batch, formats.IN_PROGRESS, len(outcomes), failed_files,
                                        file_hits, None)
        common.put_json(s3, base + "summary.json", summary, create_only=False)
        print(f"IN_PROGRESS: {len(outcomes)}/{len(uris)} files done. "
              f"Run Find again with BATCH_ID={config.batch_id} to continue.")
        return summary

    results, counts = strategy.build_results(inputs, batch["strategy_settings"], per_key, failed_files)
    # Rebuilt deterministically from the wave files, so overwriting is safe.
    common.put_lines(s3, base + "files.jsonl", [formats.file_line(h) for h in file_hits], create_only=False)
    common.put_json(s3, base + "results.json", results, create_only=False)
    summary = formats.build_summary(config.batch_id, batch, formats.COMPLETE, len(outcomes), failed_files,
                                    file_hits, counts)
    common.put_json(s3, base + "summary.json", summary, create_only=False)  # written last
    print(f"COMPLETE: MATCH {summary['requests_match']}, NO_MATCH {summary['requests_no_match']}, "
          f"ERROR {summary['requests_error']}; {summary['rows_to_delete']} rows to delete in "
          f"{summary['files_to_rewrite']} files; {summary['files_failed']} files failed; "
          f"{summary['files_skipped_tracked']} skipped by the tracker")
    return summary


def execute(s3, config, runner_factory, notifier=None, tracker=None):
    """run() wrapped with the JOB_STARTED / JOB_FINISHED / JOB_FAILED notifications."""
    config = arguments.find_config(config)
    context = {"job": "FIND", "batch_id": config.batch_id, "strategy": config.find_strategy or "(batch)",
               "details_uri": config.base + "summary.json"}
    return notify.run_with_notifications(notifier, config.notify_to, context,
                                         lambda: run(s3, config, runner_factory, tracker))


def parse_args(argv=None):
    return arguments.parse_find_args(argv)


def main(argv=None):
    from pyspark.context import SparkContext

    config = parse_args(argv)
    spark_context = SparkContext.getOrCreate()
    common.raise_csv_limit()
    execute(common.s3_client(config.region), config, lambda shared: common.SparkRunner(spark_context, shared),
            notify.make_notifier(config.notifier))


if __name__ == "__main__":
    main()
