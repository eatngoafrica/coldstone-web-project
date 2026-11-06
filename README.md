# coldstone-web-project

Daily pipeline: Coldstone web API (Nigeria and Kenya) → S3 raw and processed layers → Redshift → Teams card.
It runs on AWS Lambda and Step Functions. It moved off Glue in November 2026, when Glue was blocked for the account.

## How it runs

```
05:00 Lagos   ─► state machine coldstone-extract-ng ─┐
05:30 Nairobi ─► state machine coldstone-extract-ky ─┤  pull API → raw (in parts, each Lambda run < 15 min)
                                                     │  build processed Parquet from raw → SNS "success"
                                                     ▼
                     SNS → SQS coldstone-load-queue → coldstone-load-dispatcher
                                                     ▼
                     Lambda coldstone-load-<cc> (one load per country at a time) → Redshift MERGE → SNS → Teams
```

| Piece | Code | Created by |
|---|---|---|
| Extract Lambdas `coldstone-extract-ng` / `-ky` | [extract_prod_ng.py](extract_prod_ng.py), [extract_prod_ky.py](extract_prod_ky.py), [lambda/pipeline/entry.py](lambda/pipeline/entry.py) | [template.yaml](template.yaml) |
| State machines `coldstone-extract-ng` / `-ky` and their daily schedules | [statemachine/extract.asl.json](statemachine/extract.asl.json) | template.yaml |
| Load Lambdas `coldstone-load-ng` / `-ky` | [load_redshift.py](load_redshift.py) | template.yaml |
| Dispatcher, Teams notifier, S3 bucket, SNS topic, SQS queue, Redshift | [lambda/](lambda/), [sql/redshift/](sql/redshift/) | created earlier in the console; template.yaml only refers to them |

S3 paths and Redshift schemas are unchanged from the Glue version. Nigeria's pull is split into 2 Lambda runs
(`NgParts` in [samconfig.toml](samconfig.toml)) because the whole pull took 13 minutes on Glue.

## Deploying

Pushing to `main` runs CI, then [.github/workflows/deploy.yml](.github/workflows/deploy.yml), which builds the
Lambda code ([deploy/build_lambdas.py](deploy/build_lambdas.py)) and runs `sam deploy`.

### First time only

1. **samconfig.toml**: replace every `CHANGE_ME` with the values the Glue jobs used (AWS Glue → ETL jobs → the job
   → Job details → Advanced properties → Job parameters), plus the dispatcher's role name (Lambda →
   coldstone-load-dispatcher → Configuration → Permissions).
2. **GitHub deploy role**: add [deploy/github-deploy-policy.json](deploy/github-deploy-policy.json) to the IAM role
   `github-deploy` as an inline policy (IAM → Roles → github-deploy → Add permissions → Create inline policy → JSON).
3. **Redshift**: run [sql/redshift/03_grants_lambda_ng.sql](sql/redshift/03_grants_lambda_ng.sql) and
   [03_grants_lambda_ky.sql](sql/redshift/03_grants_lambda_ky.sql) in DBeaver as an admin.
4. **Lambda concurrency**: the load Lambdas each reserve 1 concurrent run (one load per country at a time). In
   November 2026 the account's limit was 40 with 13 unreserved (Lambda console → Dashboard), so this only just fits;
   AWS keeps a minimum unreserved. If the deploy fails with "below its minimum value", it rolls back cleanly: ask
   AWS to raise "Concurrent executions" (Service Quotas → AWS Lambda) to the default 1000.
5. **Encryption**: if the bucket, the SNS topic or the API key parameters use a customer-managed KMS key, compare
   the Glue jobs' roles with template.yaml and add the matching `kms:` permissions.
6. Push to `main`, or run the Deploy workflow by hand.

## Running by hand

- **Extract a date** (and load it, which follows automatically): Step Functions → `coldstone-extract-<cc>` → Start
  execution with `{"date": "2026-11-04"}`. Run one date at a time per country, because both runs would share the
  API's rate limit.
- **Load a date that is already in processed**: Lambda → `coldstone-load-<cc>` → Test with `{"date": "2026-11-04"}`.
- **Full history**: run it on your machine (`python extract_prod_ng.py --S3_BUCKET … --full-history true`), since it
  can take longer than Lambda's 15 minutes, then load the date as above.
- Re-running any date is safe: raw and processed are replaced, and the Redshift MERGE gives the same result.

## Checks

`pytest tests` covers the Parquet schemas against the DDL, the load SQL, split pulls rebuilding the same day as a
single pull, the Lambda entry points, the dispatcher, the Teams cards, and the state machine against the template.
CI also runs `cfn-lint template.yaml`.
