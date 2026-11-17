"""drop-glue FORGET: Glue Spark job (driver) that deletes the found records.

Reads <result_prefix><batch_id>/files.jsonl (one line per part file) and, for
each part, ONCE: loads it, drops every listed TRAN_ID, and saves a new
.tar.gz with the same S3 key and the same CSV name inside (see tasks.py).

One Glue run processes at most MAX_WAVES waves of FILES_PER_WAVE parts (one
Spark task per part), then stops. Running it again with the same BATCH_ID
continues: finished parts have a completion marker and are skipped. Writes
remove_summary.json after every run. The stop switch is checked at start and
before every wave.
"""
import argparse
import hashlib
import time

import common
import formats
import tasks


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


def run(s3, args, runner_factory):
    batch_id = common.check_batch_id(args.batch_id)
    purge = common.parse_bool(args.purge_old_versions, "--purge_old_versions")
    files_per_wave, max_waves = common.throttle(args)
    base = f"{args.result_prefix}{batch_id}/"

    if common.stop_requested(s3, args.stop_uri):
        print(f"STOP switch is on ({args.stop_uri}); not starting. Use Jenkinsfile.stop ACTION=RESUME to clear it.")
        return {"status": "STOPPED"}
    summary = formats.parse_summary(common.get_json(s3, base + "summary.json"), batch_id)
    if summary["status"] != formats.COMPLETE:
        raise ValueError(f"Find for batch {batch_id} is {summary['status']}, not COMPLETE; finish Find first")
    entries = read_files(s3, base + "files.jsonl", args.source_prefix)

    job_bucket, done_prefix = common.split_uri(base + "done/")
    _, done_keys = common.list_keys(s3, job_bucket, done_prefix)
    done = set(done_keys)
    outcomes, pending = {}, []
    for hits in entries:
        key = done_prefix + marker_name(hits.uri)
        if key in done and not purge:
            outcomes[hits.uri] = {"uri": hits.uri, "rows_expected": len(hits.tran_ids), "status": "SKIPPED",
                                  "reason": "Already completed by an earlier run"}
        else:
            pending.append({"uri": hits.uri, "etag": hits.etag, "version_id": hits.version_id,
                            "tran_ids": sorted(hits.tran_ids), "marker_uri": f"s3://{job_bucket}/{key}",
                            "already_done": key in done})
    print(f"Batch {batch_id}: {len(entries)} part files to rewrite, {len(pending)} still to do; "
          f"purge_old_versions={purge}; this run: up to {max_waves or 'unlimited'} waves of {files_per_wave}")

    runner = runner_factory({"batch_id": batch_id, "purge": purge, "region": args.region})
    waves = 0
    while pending and (max_waves == 0 or waves < max_waves):
        if common.stop_requested(s3, args.stop_uri):
            print("STOP switch is on; stopping before the next wave")
            break
        wave, pending = pending[:files_per_wave], pending[files_per_wave:]
        started = time.time()
        results = runner.map(tasks.forget_task, wave)
        for outcome in results:
            outcomes[outcome["uri"]] = outcome
        waves += 1
        failed = sum(o["status"] == "FAILED" for o in results)
        print(f"Wave {waves}: {len(results)} parts ({failed} failed) in {time.time() - started:.0f}s")

    for item in pending:
        outcomes[item["uri"]] = {"uri": item["uri"], "rows_expected": len(item["tran_ids"]), "status": "PENDING"}
    report = formats.build_remove_summary(batch_id, purge, [outcomes[hits.uri] for hits in entries])
    common.put_json(s3, base + "remove_summary.json", report, create_only=False)
    print(f"{report['status']}: done {report['files_done']}, skipped {report['files_skipped']}, "
          f"failed {report['files_failed']}, pending {report['files_pending']}")
    for outcome in report["files"]:
        if outcome["status"] == "FAILED":
            print(f"FAILED {outcome['uri']}: {outcome['error']}")
    if report["files_failed"]:
        raise RuntimeError(f"{report['files_failed']} part file(s) failed; see {base}remove_summary.json")
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    common.add_fixed_arguments(parser)
    common.add_throttle_arguments(parser)
    parser.add_argument("--batch_id")
    parser.add_argument("--purge_old_versions", default="false")
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
