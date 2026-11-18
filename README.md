# drop-glue

A batch tool that finds records in an S3 data lake of monthly `.tar.gz` part files, deletes them, and replaces each affected part with a clean copy under the same name. It works like [Amazon S3 Find and Forget](https://github.com/awslabs/amazon-s3-find-and-forget), but without Cognito, an API or a web UI: **Jenkins starts AWS Glue Spark jobs**, and drop-downs pick the behaviour (**Strategy pattern**).

```text
Layer 1  arguments.py          parse + validate the pipeline parameters
Layer 2  find.py   + FindStrategy    HASH_MATCH | TRAN_ID_SUFFIX         → files.jsonl (one line per part)
Layer 3  forget.py + ForgetStrategy  KEEP | PURGE | TAG_FOR_LIFECYCLE    ← reads only files.jsonl
Shared   processed-file tracker (DynamoDB | S3 tag), notifications (placeholder)
```

1. **Find**, chosen with `FIND_STRATEGY`:
   - `HASH_MATCH`: for each input item `{run_id, hash_value, match_type}`, reports MATCH / NO_MATCH / ERROR and every part file and transaction ID where it was found (SHA-256 of the XML fields).
   - `TRAN_ID_SUFFIX`: no input; lists every transaction ID ending with `_c` (configurable), by part file.

   Either way it scans a month range (e.g. 2026-01 to 2026-03, about 1,000 parts per month), 100 parts at a time, at most 500 per run, and continues across runs. The **processed-file tracker** skips parts already processed for the same strategy and input.
2. **Review**: the status pipeline shows progress and attaches the reports.
3. **Forget**, chosen with `FORGET_STRATEGY`: rewrites each matched part **once**, under the same name (12 files → 12 rewrites, however many rows), then keeps, purges, or tags the old S3 version for a lifecycle rule.
4. **Stop**: KILL / PAUSE / RESUME at any time.

👉 **Start with the [step-by-step guide](docs/GUIDE.md)**. It has diagrams and covers the strategies, the tracker, hashing, throttling and sizing, the emergency stop, setup, troubleshooting and changing formats.

## Repository layout

| Path | What it is |
|---|---|
| `main.tf`, `locals.tf`, `variables.tf`, `outputs.tf`, `versions.tf`, `moved.tf` | Terraform root: wires the modules together (S3 backend, partial config) |
| `modules/` | One module per resource: `s3-bucket` (job bucket), `iam-role`, `glue-job` (+ its log group), `dynamodb-table`, `s3-lifecycle`, `eventbridge-rule` |
| `backend/<env>.s3.tfbackend` | S3 state bucket, workspace prefix and locking per environment (prod, stage, uat) |
| `tfvars/<env>.tfvars` | Variables per environment (prod, stage, uat), used in the workspace of the same name; `tfvars/example.tfvars` lists every variable |
| `scripts/arguments.py` | Layer 1: argument parsing and validation (`FindConfig`, `ForgetConfig`) |
| `scripts/find.py`, `scripts/find_strategies.py` | Layer 2: Find driver (batches, waves, reports) and the find strategies |
| `scripts/forget.py`, `scripts/forget_strategies.py` | Layer 3: Forget driver (waves, markers, remove summary) and the forget strategies |
| `scripts/tasks.py` | Work for one part file, run inside Spark tasks (scan / rewrite) |
| `scripts/tracking.py` | Processed-file tracker (DynamoDB, S3 tag, none) |
| `scripts/notify.py` | Common notification template and senders (placeholder) |
| `scripts/common.py` | Shared S3, archive, month selection, stop switch and runner helpers |
| `scripts/formats.py` | All input and report JSON layouts (edit here when formats change) |
| `Jenkinsfile` | **Provisioning** pipeline: branch, workspace, apply/destroy (+ `DESTROY_ALL`), optional `COPY_SCRIPTS` |
| `jenkins/Jenkinsfile.{find,status,forget,stop}` | The four find-and-forget pipelines (fire-and-forget), used only to run batches |
| `examples/` | Sample input and outputs (kept in sync by the tests) |
| `tests/` | `pytest tests/`, which runs locally without AWS or Spark |
| `docs/GUIDE.md` | The guide |
| `docs/COST_ESTIMATE.md` | AWS cost estimates per scenario, the formulas, and a log of measured runs |
| `docs/COMPARISON_GLUE_VS_STEP_FUNCTIONS.md` | Cost, duration and trade-offs of Step Functions + Lambda vs Glue for `_c` mode |
| `docs/SCENARIO_3_MONTHS_TRAN_ID_SUFFIX.md` | Worked scenario: `_c` mode on 3 months × 800 files (waves, runs, duration, cost; 1 month vs 3 months) |

## Quick start

1. Edit `backend/<env>.s3.tfbackend` (state bucket) and `tfvars/<env>.tfvars` (environment, job bucket base name, archive bucket, region), and commit them.
2. Create a Jenkins job `drop-glue-provision` from the root **`Jenkinsfile`**, set its environment variables `ROLE_NAME`, `ROLE_ACCOUNT`, `EXTERNAL_ID`, `AWS_REGION` and `NOTIFICATION_EMAIL` (every AWS call runs under `withAWS` with that role), and build it with `BRANCH`, `TERRAFORM_WORKSPACE=stage` and `ACTION=apply`. It creates the job bucket `<job_bucket>-stage`, the Glue jobs, roles, log groups and tracker table, and on the first run copies `scripts/*.py` into the bucket. Tick `COPY_SCRIPTS` on later runs when the scripts change. `ACTION=destroy` requires `DESTROY_ALL`. Every build stops for a 10-minute approval after the plan, and `prod` first asks for a change request number.
3. Every resource name ends in the workspace, e.g. `drop-glue-find-prod` and `abcd-job-data-prod` ([guide 7.2](docs/GUIDE.md#72-deploy)).

Then create the four find-and-forget Jenkins jobs from `jenkins/` with the defaults the provisioning build prints, agree the hashing rule ([guide section 4](docs/GUIDE.md#4-how-hashing-works)), run a pilot to size the throttling ([section 5](docs/GUIDE.md#5-throttling-and-sizing)) and follow [section 8](docs/GUIDE.md#8-step-1-find).

## Defaults (all configurable)

| Setting | Default |
|---|---|
| Find strategy | `HASH_MATCH` |
| Forget strategy | `REPLACE_KEEP_VERSIONS` |
| Processed-file tracker | DynamoDB table `<name>-processed-files` |
| Notifications | `LOG` (printed to the job log; SES/SNS are placeholders) |
| Files per wave | 100 |
| Waves per run | 5 (500 files per run, then stop) |
| Workers | 26 × G.1X (1 driver + 100 parallel tasks) |
| Run timeout | 8 hours |

## Data handling

- Reports hold hashes and transaction IDs. Restrict access to `drop-glue/requests/` and `drop-glue/results/` and expire them with a lifecycle rule.
- On a versioned bucket, deleted rows remain in old object versions unless Forget runs with `REPLACE_PURGE_OLD_VERSIONS`, or with `REPLACE_TAG_FOR_LIFECYCLE` plus the lifecycle rule, or a bucket rule expires non-current versions.
- Notifications carry batch IDs, statuses and counts only, never customer data.
