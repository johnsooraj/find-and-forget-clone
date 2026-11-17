"""drop-glue FIND: Glue Spark job (driver) that finds customer records by SHA-256 hash.

One Glue run processes at most MAX_WAVES waves of FILES_PER_WAVE part files
(one Spark task per file), then stops. Running it again with the same
BATCH_ID continues where the last run stopped. Everything is kept under
<result_prefix><batch_id>/:

  batch.json            fixed settings of the batch (first run)
  selected_files.jsonl  the part files in the month range (first run)
  waves/wave-NNNNN.jsonl  per-file results, one file per wave
  summary.json          IN_PROGRESS while files remain, then COMPLETE
  results.json          one entry per input item (when COMPLETE)
  files.jsonl           one line per part file to rewrite (when COMPLETE)

The emergency stop switch (--stop_uri) is checked at start and before every wave.
"""
import argparse
import json
import time

import common
import formats
import tasks


# ----------------------------------------------------------------------------- batch setup
def open_batch(s3, args, batch_id, base):
    """Create the batch on the first run; on later runs check nothing changed.

    Returns (batch, requests, selected part URIs).
    """
    month_from = common.parse_month(args.month_from, "--month_from")
    month_to = common.parse_month(args.month_to, "--month_to")
    if month_from and month_to and month_from > month_to:
        raise ValueError("--month_from is after --month_to")
    if args.hash_mode and args.hash_mode not in common.HASH_MODES:
        raise ValueError(f"--hash_mode must be one of {', '.join(common.HASH_MODES)}")
    archive_uri = common.resolve_archive_uri(args.archive_uri, args.source_prefix) if args.archive_uri else None

    batch_uri = base + "batch.json"
    if common.object_exists(s3, batch_uri):
        batch = formats.parse_batch(common.get_json(s3, batch_uri), batch_id)
        given = {"input_uri": args.input_uri or None, "archive_uri": archive_uri, "month_from": month_from,
                 "month_to": month_to, "hash_mode": args.hash_mode or None}
        for key, value in given.items():
            if value is not None and value != batch.get(key):
                raise ValueError(f"--{key} {value!r} differs from this batch ({batch.get(key)!r}); "
                                 "leave it empty to continue the batch, or start a new BATCH_ID")
        document, etag = common.get_json_with_etag(s3, batch["input_uri"])
        if etag != batch["input_etag"]:
            raise ValueError("The input file changed since this batch started; start a new BATCH_ID")
        selected = [formats.parse_selected_line(line) for line in common.get_lines(s3, base + "selected_files.jsonl")]
        if len(selected) != batch["files_total"]:
            raise ValueError("selected_files.jsonl does not match batch.json")
        print(f"Continuing batch {batch_id} (started {batch.get('created_at')})")
    else:
        if not args.input_uri or not archive_uri:
            raise ValueError("A new batch needs --input_uri and --archive_uri")
        hash_mode = args.hash_mode or "normalized"
        document, etag = common.get_json_with_etag(s3, args.input_uri)
        formats.parse_input(document)  # fail fast on a wrong overall shape
        selected, skipped = common.select_archives(s3, archive_uri, month_from, month_to)
        common.put_lines(s3, base + "selected_files.jsonl", [formats.selected_line(u) for u in selected],
                         create_only=False)
        batch = formats.build_batch(batch_id, args.input_uri, etag, archive_uri, month_from, month_to, hash_mode,
                                    len(selected), skipped)
        common.put_json(s3, batch_uri, batch, create_only=True)
        print(f"New batch {batch_id}: {len(selected)} part files in {month_from or '(start)'}..{month_to or '(end)'}")
        if skipped:
            print(f"Skipped non-date folders: {', '.join(skipped)}")
    return batch, formats.parse_input(document), selected


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
def aggregate(requests, outcomes):
    """Return ({item index: {uri: TRAN_IDs}}, [FileHits per part], failed files)."""
    per_request = {r.index: {} for r in requests}
    by_file, failed = {}, []
    for uri in sorted(outcomes):
        outcome = outcomes[uri]
        if outcome["status"] == formats.FAILED:
            failed.append({"uri": uri, "error": outcome.get("error")})
            continue
        for index, tran_ids in outcome["matches"].items():
            if index not in per_request or requests[index].error:
                raise ValueError(f"{uri}: wave result refers to an unknown input item")
            per_request[index][uri] = tran_ids
            hits = by_file.setdefault(uri, formats.FileHits(uri, outcome["etag"], outcome.get("version_id")))
            hits.tran_ids.update(tran_ids)
            hits.run_ids.add(requests[index].run_id)
    return per_request, [by_file[uri] for uri in sorted(by_file)], failed


def decide(request, data_identifier, failed_files):
    """MATCH / NO_MATCH / ERROR for one input item."""
    if request.error:
        return formats.ERROR, request.error
    if data_identifier:
        return formats.MATCH, None
    if failed_files:
        return formats.ERROR, f"{len(failed_files)} file(s) could not be scanned, so NO_MATCH cannot be confirmed"
    return formats.NO_MATCH, None


# ----------------------------------------------------------------------------- run
def run(s3, args, runner_factory):
    batch_id = common.check_batch_id(args.batch_id)
    files_per_wave, max_waves = common.throttle(args)
    retry_failed = common.parse_bool(args.retry_failed, "--retry_failed")
    base = f"{args.result_prefix}{batch_id}/"

    if common.stop_requested(s3, args.stop_uri):
        print(f"STOP switch is on ({args.stop_uri}); not starting. Use Jenkinsfile.stop ACTION=RESUME to clear it.")
        return {"status": "STOPPED"}
    if common.object_exists(s3, base + "summary.json"):
        existing = formats.parse_summary(common.get_json(s3, base + "summary.json"), batch_id)
        can_retry = retry_failed and existing["files_failed"] and not forget_started(s3, base)
        if existing["status"] == formats.COMPLETE and not can_retry:
            print(f"Batch {batch_id} is already COMPLETE; nothing to do")
            return existing

    batch, requests, selected = open_batch(s3, args, batch_id, base)
    outcomes, next_wave = load_waves(s3, base)
    remaining = [uri for uri in selected
                 if uri not in outcomes or (retry_failed and outcomes[uri]["status"] == formats.FAILED)]
    lookup = tasks.build_lookup(requests)
    tag_map = tasks.build_tag_map({t: getattr(args, t.lower() + "_tags") for t in formats.MATCH_TYPES}, lookup)
    if not lookup:
        print("No valid input items: nothing can match, so no files are scanned")
        remaining = []
    valid = sum(1 for r in requests if not r.error)
    print(f"{len(requests)} input items ({valid} valid); {len(selected) - len(remaining)} of {len(selected)} "
          f"files already done; this run: up to {max_waves or 'unlimited'} waves of {files_per_wave} files")

    runner = runner_factory({"lookup": lookup, "tag_map": tag_map, "hash_mode": batch["hash_mode"],
                             "region": args.region})
    waves = 0
    while remaining and (max_waves == 0 or waves < max_waves):
        if common.stop_requested(s3, args.stop_uri):
            print("STOP switch is on; stopping before the next wave")
            break
        wave, remaining = remaining[:files_per_wave], remaining[files_per_wave:]
        started = time.time()
        lines = [formats.wave_line(outcome) for outcome in runner.map(tasks.scan_task, [{"uri": u} for u in wave])]
        name = f"wave-{next_wave:05d}.jsonl"
        common.put_lines(s3, f"{base}waves/{name}", lines, create_only=True)
        for number, line in enumerate(lines, 1):
            outcome = formats.parse_wave_line(json.dumps(line), f"{name} line {number}")
            outcomes[outcome["uri"]] = outcome
        next_wave += 1
        waves += 1
        elapsed = time.time() - started
        failed = sum(line["status"] == formats.FAILED for line in lines)
        megabytes = sum(line["bytes"] or 0 for line in lines) / 1e6
        busiest = max((line["seconds"] or 0 for line in lines), default=0)
        print(f"{name}: {len(lines)} files ({failed} failed) in {elapsed:.0f}s, {megabytes:.0f} MB "
              f"({megabytes / max(elapsed, 1):.1f} MB/s), slowest file {busiest:.0f}s; "
              f"{len(outcomes)}/{len(selected)} done")

    per_request, file_hits, failed_files = aggregate(requests, outcomes)
    if remaining:
        summary = formats.build_summary(batch_id, batch, formats.IN_PROGRESS, len(outcomes), failed_files,
                                        file_hits, None)
        common.put_json(s3, base + "summary.json", summary, create_only=False)
        print(f"IN_PROGRESS: {len(outcomes)}/{len(selected)} files done. "
              f"Run Find again with BATCH_ID={batch_id} to continue.")
        return summary

    results = []
    for request in requests:
        match_result, error = decide(request, per_request[request.index], failed_files)
        results.append(formats.result_entry(request, match_result, per_request[request.index], error))
    # Rebuilt deterministically from the wave files, so overwriting is safe.
    common.put_lines(s3, base + "files.jsonl", [formats.file_line(h) for h in file_hits], create_only=False)
    common.put_json(s3, base + "results.json", results, create_only=False)
    summary = formats.build_summary(batch_id, batch, formats.COMPLETE, len(outcomes), failed_files, file_hits,
                                    results)
    common.put_json(s3, base + "summary.json", summary, create_only=False)  # written last
    print(f"COMPLETE: MATCH {summary['requests_match']}, NO_MATCH {summary['requests_no_match']}, "
          f"ERROR {summary['requests_error']}; {summary['rows_to_delete']} rows to delete in "
          f"{summary['files_to_rewrite']} files; {summary['files_failed']} files failed")
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    common.add_fixed_arguments(parser)
    common.add_throttle_arguments(parser)
    parser.add_argument("--batch_id")
    parser.add_argument("--input_uri")
    parser.add_argument("--archive_uri")
    parser.add_argument("--month_from")
    parser.add_argument("--month_to")
    parser.add_argument("--hash_mode")
    parser.add_argument("--retry_failed", default="false")
    for match_type, default in tasks.DEFAULT_TAGS.items():
        parser.add_argument(f"--{match_type.lower()}_tags", default=default)
    args, _ = parser.parse_known_args(argv)  # Glue adds its own arguments
    common.check_fixed_arguments(args)
    return args


def main(argv=None):
    from pyspark.context import SparkContext

    args = parse_args(argv)
    spark_context = SparkContext.getOrCreate()
    common.raise_csv_limit()
    run(common.s3_client(args.region), args, lambda shared: common.SparkRunner(spark_context, shared))


if __name__ == "__main__":
    main()
