# drop-glue

Find customer records in an S3 data lake of monthly `.tar.gz` part files by **SHA-256 hash**, delete them, and replace each affected part with a clean copy under the same name. It works like [Amazon S3 Find and Forget](https://github.com/awslabs/amazon-s3-find-and-forget), but without Cognito, an API or a web UI: **Jenkins starts AWS Glue Spark jobs**.

1. **Find**: for each input item `{run_id, hash_value, match_type}`, reports MATCH / NO_MATCH / ERROR and every part file and transaction ID where it was found. It scans a month range (e.g. 2026-01 to 2026-03, about 1,000 parts per month), 100 parts at a time, at most 500 per run, and continues across runs.
2. **Review**: the status pipeline shows progress and attaches the reports.
3. **Forget**: rewrites each matched part **once**, under the same name, with the same throttling. It can also purge the old S3 versions.
4. **Stop**: KILL / PAUSE / RESUME at any time.

👉 **Start with the [step-by-step guide](docs/GUIDE.md)**. It has diagrams and covers hashing, throttling and sizing, the emergency stop, setup, troubleshooting and changing formats.

## Repository layout

| Path | What it is |
|---|---|
| `main.tf`, `variables.tf`, `modules/glue-job/` | Terraform: 5 script objects, 2 Glue 5.0 Spark jobs, 2 IAM roles + policies |
| `scripts/find.py` | Find driver: batches, waves, reports |
| `scripts/forget.py` | Forget driver: waves, completion markers, remove summary |
| `scripts/tasks.py` | Work for one part file, run inside Spark tasks (hashing / rewriting) |
| `scripts/common.py` | Shared S3, archive, month selection, stop switch and runner helpers |
| `scripts/formats.py` | All input and report JSON layouts (edit here when formats change) |
| `jenkins/Jenkinsfile.{find,status,forget,stop}` | The four pipelines (fire-and-forget) |
| `examples/` | Sample input and outputs (kept in sync by the tests) |
| `tests/` | `pytest tests/`, which runs locally without AWS or Spark |
| `docs/GUIDE.md` | The guide |

## Quick start

```text
cp terraform.tfvars.example terraform.tfvars   # set buckets and region
terraform init
terraform apply
terraform output
```

Then create the four Jenkins jobs from `jenkins/`, agree the hashing rule ([guide section 4](docs/GUIDE.md#4-how-hashing-works)), run a pilot to size the throttling ([section 5](docs/GUIDE.md#5-throttling-and-sizing)) and follow [section 8](docs/GUIDE.md#8-step-1-find).

## Defaults (all configurable)

| Setting | Default |
|---|---|
| Files per wave | 100 |
| Waves per run | 5 (500 files per run, then stop) |
| Workers | 26 × G.1X (1 driver + 100 parallel tasks) |
| Run timeout | 8 hours |

## Data handling

- Reports hold hashes and transaction IDs. Restrict access to `drop-glue/requests/` and `drop-glue/results/` and expire them with a lifecycle rule.
- On a versioned bucket, deleted rows remain in old object versions unless Forget runs with `PURGE_OLD_VERSIONS=true` or a lifecycle rule expires non-current versions.
