# drop-glue cost estimates

A living record of what drop-glue costs to run. Each scenario lists its assumptions, so the numbers can be re-done when prices, sizes or measured speeds change. **Add a row to [Measured runs](#5-measured-runs) after every pilot or real batch**, and update the per-file speeds in section 1 when they differ from the estimate.

| | |
|---|---|
| Last updated | 2026-11-18 |
| Region | us-east-1 (list prices, no free tier, no discounts) |
| Code version | Find/Forget strategies, processed-file tracker, one module per resource |

---

## 1. Unit prices and assumptions

### 1.1 AWS list prices used

| Service | Item | Price | Source |
|---|---|---|---|
| AWS Glue 5.0 (Spark, standard execution class) | per DPU-hour, billed per second, 1-minute minimum per run | **$0.44** | [Glue pricing](https://aws.amazon.com/glue/pricing/) |
| | G.1X worker | 1 DPU (4 vCPU, 16 GB) | |
| Amazon S3 Standard | GET (and other reads, incl. GetObjectTagging) | $0.0004 per 1,000 | [S3 pricing](https://aws.amazon.com/s3/pricing/) |
| | PUT, COPY, POST, **LIST** (incl. PutObjectTagging, ListObjectVersions) | $0.005 per 1,000 | |
| | DELETE (incl. DeleteObjectVersion) | free | |
| | Storage, first 50 TB | $0.023 per GB-month | |
| | Data transfer S3 ↔ Glue, same region | free | |
| DynamoDB on-demand | write request unit (1 KB) | $0.625 per million | [DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/) |
| | read request unit (eventually consistent: 0.5 RRU per 4 KB) | $0.125 per million | |
| | storage / PITR | $0.25 / $0.20 per GB-month | |
| CloudWatch Logs | ingestion / storage | $0.50 per GB / $0.03 per GB-month | |
| KMS (only if the buckets use SSE-KMS without S3 Bucket Keys) | per request | $0.03 per 10,000 | |
| EventBridge | Glue state-change events on the default bus | free | |

> Check the pricing pages before using these numbers in a budget. Glue 6.0 is advertised at about 30% less per DPU-hour; drop-glue currently pins Glue 5.0 (`modules/glue-job`, `glue_version`).

### 1.2 Data and cluster assumptions

| Assumption | Value | Why / how to check |
|---|---|---|
| Part files | **500** | the scenario |
| Size of each CSV (uncompressed) | **100 MB** | the scenario |
| Size of each `.tar.gz` part | **~20 MB** (assumed 5:1 compression; XML compresses well) | `aws s3 ls` on a month folder. Section 4.2 shows the case where 100 MB is the `.tar.gz` size. |
| Workers | 26 × G.1X = **26 DPU** (1 driver + 25 executors × 4 = 100 files at once) | Terraform defaults |
| Throttling | 100 files per wave, 5 waves per run, so **500 files = 1 Glue run** | Terraform defaults |
| Glue run overhead | **~75 s per run** (Spark start, listing, reports, stop-switch checks) | the driver log's first and last lines |
| Spark scheduling per wave | ~3 s | |
| Files rewritten by Forget | **all 500** (upper bound: every part has at least one `_c` row) | `summary.json` → `files_to_rewrite` |

### 1.3 Per-file speed (estimates: replace with measured values)

`TRAN_ID_SUFFIX` doesn't parse XML. A task does a gzip read plus a Python CSV pass, and checks `TRAN_ID.endswith("_c")` on each row.

| Step | Work per file | Typical | Pessimistic |
|---|---|---|---|
| **Find** (scan) | GET 20 MB, gunzip, `csv.DictReader` over 100 MB | **6 s** | 12 s |
| **Forget** (rewrite) | GET + parse as above, `csv.DictWriter` 100 MB, gzip level 9, PUT 20 MB, marker PUT | **18 s** | 35 s |
| For comparison: `HASH_MATCH` Find | as Find, plus XML parsing and hashing of every row | ~95 s per 100 MB part | measured in a pilot (guide §5) |

A wave takes about as long as its slowest file plus ~3 s, because 100 files run in parallel on 100 slots.

---

## 2. How to calculate

```text
run seconds   = overhead + waves × (slowest file seconds + 3)
DPU-hours     = workers × run seconds / 3600          (G.1X: 1 worker = 1 DPU)
Glue cost     = DPU-hours × $0.44
waves         = ceil(files / files_per_wave)
runs          = ceil(waves / max_waves)               (each run pays the overhead again; max_waves 0 = one run)
```

**Actual cost of a run** (Glue reports seconds and workers):

```text
aws glue get-job-run --job-name drop-glue-find --run-id jr_... \
  --query 'JobRun.{seconds:ExecutionTime,workers:NumberOfWorkers,type:WorkerType}'
# cost = seconds × workers / 3600 × 0.44
```

For the exact bill, activate the cost-allocation tags from `tfvars/<env>.tfvars` (`project`, `environment`) in AWS Billing. Then filter Cost Explorer by `project = drop-glue`.

---

## 3. Scenario A: 500 × 100 MB CSV, `TRAN_ID_SUFFIX`, then Forget

### 3.1 Glue compute (the main cost)

| Job | Waves | Run time (typical) | DPU-hours | **Cost typical** | Cost pessimistic |
|---|---|---|---|---|---|
| Find (`TRAN_ID_SUFFIX`) | 5 | 75 + 5 × (6 + 3) = 120 s | 26 × 120 / 3600 = 0.87 | **$0.38** | 120 + 5 × (12 + 3) = 195 s → 1.41 DPU-h → $0.62 |
| Forget (500 rewrites) | 5 | 75 + 5 × (18 + 3) = 180 s | 26 × 180 / 3600 = 1.30 | **$0.57** | 120 + 5 × (35 + 3) = 310 s → 2.24 DPU-h → $0.99 |
| **Total Glue** | | ~5 min | 2.17 | **$0.95** | $1.61 |

Pessimistic uses 120 s of overhead per run and the slow per-file times from 1.3.

### 3.2 S3 requests

| Job | Requests | Count | Cost |
|---|---|---|---|
| Find | GET part files + reports | ~505 | $0.0002 |
| | LIST (month folders, stop switch ×6, batch/waves checks) | ~20 | $0.0001 |
| | PUT (5 waves, batch, selected, summary, results, files) | ~10 | $0.0001 |
| Forget | GET part files (`If-Match`) + reports | ~505 | $0.0002 |
| | PUT rewritten parts + completion markers + remove_summary | ~1,001 | $0.0050 |
| | LIST (done/, stop switch) | ~10 | $0.0001 |
| Forget strategy extra | `REPLACE_KEEP_VERSIONS` | 0 | $0 |
| | `REPLACE_PURGE_OLD_VERSIONS`: 500 ListObjectVersions + 500 marker PUTs (deletes are free) | 1,000 | $0.0050 |
| | `REPLACE_TAG_FOR_LIFECYCLE`: 500 Get + 500 Put version tagging + 500 marker PUTs | 1,500 | $0.0052 |
| **Total requests** | | | **≈ $0.006 – $0.011** |

### 3.3 Storage of the old versions (versioned data lake bucket)

The rewritten parts replace the current versions at about the same size. The **old** versions (500 × 20 MB = 10 GB) are what changes the bill:

| Forget strategy | Old versions kept | Storage cost |
|---|---|---|
| `REPLACE_KEEP_VERSIONS` | 10 GB, until something else expires them | **$0.23 per month, every month** |
| `REPLACE_PURGE_OLD_VERSIONS` | 0 | $0 |
| `REPLACE_TAG_FOR_LIFECYCLE` (7 days) | 10 GB for 7 days | ≈ $0.05 once |

### 3.4 Everything else

| Item | Usage | Cost |
|---|---|---|
| DynamoDB tracker | Find: 500 lookups × 0.5 RRU + up to 500 no-match writes; Forget: 500 CLEANED writes (~1 KB each) | < $0.001 |
| DynamoDB storage + PITR | ~1,000 items × ~0.4 KB = 0.4 MB | < $0.001 per month |
| CloudWatch Logs | ~5–50 MB of driver/executor logs per run, 2 runs | $0.005 – $0.05 |
| Reports in the job bucket | waves, results, markers: a few MB | < $0.001 per month |
| KMS (SSE-KMS without Bucket Keys only) | ~2,000 requests | $0.006 |
| EventBridge rule, SNS | state-change events and a few notifications | ~$0 |

### 3.5 Total for scenario A

| | Typical | Pessimistic |
|---|---|---|
| Glue | $0.95 | $1.61 |
| S3 requests | $0.01 | $0.01 |
| Logs, DynamoDB, KMS | $0.03 | $0.06 |
| **One-time total (Find + Forget)** | **≈ $1.00** | **≈ $1.70** |
| Plus, with `REPLACE_KEEP_VERSIONS` | + $0.23 per month for the old versions | |

**What drives the cost:** Glue DPU time, and within that the fixed ~75 s overhead per run. S3 requests, DynamoDB and logs are rounding errors at this size.

---

## 3b. Other worked scenarios

| Scenario | Report | Worst-case total |
|---|---|---|
| `TRAN_ID_SUFFIX`, 3 months × 800 parts × 100 MB `.tar.gz`, 100 files/wave, 5 waves/run | [SCENARIO_3_MONTHS_TRAN_ID_SUFFIX.md](SCENARIO_3_MONTHS_TRAN_ID_SUFFIX.md) | 1 month ≈ $4.20; 3 months ≈ $12.10 (+ old-version storage) |
| Same scenario on Step Functions + Lambda (alternative platform, `_c` mode only) | [COMPARISON_GLUE_VS_STEP_FUNCTIONS.md](COMPARISON_GLUE_VS_STEP_FUNCTIONS.md) | 1 month ≈ $2.40; 3 months ≈ $7.20 (arm64) |

---

## 4. Sensitivity

### 4.1 Cluster size for the same 500 files

Bigger clusters finish sooner, but every DPU also pays for the overhead and for the slowest file of each wave.

| Workers (slots) | files_per_wave / max_waves | Find | Forget | Total | Wall time |
|---|---|---|---|---|---|
| 26 (100), default | 100 / 5 | $0.38 | $0.57 | **$0.95** | ~5 min |
| 11 (40) | 40 / 0 (13 waves) | 75 + 13 × 9 = 192 s → $0.26 | 75 + 13 × 21 = 348 s → $0.47 | **$0.73** | ~9 min |
| 6 (20), stage default | 20 / 0 (25 waves) | 75 + 25 × 9 = 300 s → $0.22 | 75 + 25 × 21 = 600 s → $0.44 | **$0.66** | ~15 min |

With `max_waves = 5` on a smaller cluster, the 500 files take several runs, and each run pays the overhead again. Use `MAX_WAVES=0` in Jenkins for a one-off batch of this size.

### 4.2 If 100 MB is the `.tar.gz` size (CSV ≈ 500 MB)

Five times the data to parse per file, so scan and rewrite take about 5× as long.

| | Per file (typical) | Run time | Cost |
|---|---|---|---|
| Find | ~30 s | 75 + 5 × 33 = 240 s | $0.76 |
| Forget | ~90 s | 75 + 5 × 93 = 540 s | $1.72 |
| Old versions kept (`KEEP`) | 50 GB | | $1.15 per month |
| **Total** | | | **≈ $2.50 + $1.15 per month with KEEP** |

### 4.3 Scaling to a full archive (12 months × 1,000 parts = 12,000 files, 100 MB CSV)

| | Waves | max_waves = 5 (24 runs, overhead ×24) | max_waves = 0 (1 run) |
|---|---|---|---|
| Find `TRAN_ID_SUFFIX` | 120 | 24 × 75 + 120 × 9 = 2,880 s → **$9.15** | 75 + 1,080 = 1,155 s → **$3.67** |
| Forget (worst case: all 12,000 rewritten) | 120 | 24 × 75 + 120 × 21 = 4,320 s → **$13.73** | 75 + 2,520 = 2,595 s → **$8.25** |
| Old versions with `KEEP` (12,000 × 20 MB = 240 GB) | | **$5.52 per month** | |

`MAX_WAVES` is there to limit each run's risk and blast radius. On large batches it costs about 75 s × 26 DPU ≈ $0.24 per extra run.

### 4.4 Find strategy comparison (500 × 100 MB)

| Find strategy | Per file | Find cost |
|---|---|---|
| `TRAN_ID_SUFFIX` | ~6 s | **$0.38** |
| `HASH_MATCH` (XML parse + hash) | ~95 s (100 MB part) | 75 + 5 × 98 = 565 s → **$1.80** |

### 4.5 Re-running with the tracker

With the DynamoDB tracker, running the same `TRAN_ID_SUFFIX` batch again skips files recorded as SCANNED_NO_MATCH or CLEANED whose ETag is unchanged. After a complete Find + Forget, a rerun scans **0 of 500** files. It costs only the run overhead (~$0.24 on 26 workers, ~$0.06 on 6) plus 250 DynamoDB read units.

---

## 5. Measured runs

Fill in after each pilot or real batch. Seconds and workers come from `aws glue get-job-run` (section 2); MB/s and the slowest file come from the driver's wave lines in `/aws-glue/jobs/<name>-find`.

| Date | Env | Batch ID | Job | Strategy | Files | Avg CSV MB | Workers | Waves | Run seconds | Slowest file (s) | DPU-hours | Glue cost | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| _yyyy-mm-dd_ | stage | | find | TRAN_ID_SUFFIX | | | | | | | | | |
| _yyyy-mm-dd_ | stage | | forget | REPLACE_KEEP_VERSIONS | | | | | | | | | |

When measured speeds differ from section 1.3, update that table and the numbers in sections 3–4.

---

## 6. Ways to lower the cost

| Option | Saving | Trade-off |
|---|---|---|
| `MAX_WAVES=0` for one-off batches | the overhead of every extra run (~$0.24 each on 26 workers) | a single long run, less chance to stop between runs (the stop switch still works between waves) |
| Fewer workers (e.g. 6–11) for batches up to a few thousand files | 20–30% at this size | longer wall time |
| `REPLACE_PURGE_OLD_VERSIONS` or `REPLACE_TAG_FOR_LIFECYCLE` | the old-version storage ($0.023 per GB-month) | purge can't be undone; lifecycle keeps a recovery window |
| Lower gzip level in `tasks.rewrite_archive` (currently level 9, the default) | maybe 20–40% of Forget time | slightly larger parts (more storage) |
| Glue Flex execution class (~$0.29 per DPU-hour) | ~34% of Glue | start-up can be delayed; needs an `execution_class` setting in `modules/glue-job` |
| Glue 6.0 (advertised ~30% lower per DPU-hour) | ~30% of Glue | needs testing on 6.0 |

**Watch out for these:**
- **Storage class:** if the parts are not in S3 Standard, Forget keeps their storage class. Standard-IA and Glacier Instant Retrieval add a **retrieval fee per GB read** (Find + Forget read every file once each), and rewriting early can trigger **minimum-storage-duration** charges for the replaced versions.
- **`KEEP` piles up:** with `REPLACE_KEEP_VERSIONS`, old versions accumulate cost every month until a lifecycle rule removes them.
