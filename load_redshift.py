"""Load one country's business date from S3 processed Parquet into Redshift (AWS Glue Python shell).

Runs after the country's extract job has checked the files and uploaded them to
s3://S3_BUCKET/S3_PREFIX/processed/<table>/business_date=D/.

Glue job parameters:
    --COUNTRY             required: ng or ky. Selects the schemas and the D-1 timezone
    --S3_BUCKET           required: bucket name (no s3://)
    --REDSHIFT_WORKGROUP  required: Redshift Serverless workgroup name
    --REDSHIFT_DATABASE   database holding the schemas (default: coldstone_web)
    --SCHEMA_BASE         schema name base (default: coldstone_web_ng for ng, coldstone_web_ke for ky). Reports
                          read <base>_analytics; the load stages its files in <base>_staging
    --S3_PREFIX           folder in the bucket (default: coldstone_web_<cc>)
    --COPY_ROLE_ARN       IAM role Redshift uses to read S3 (default: the namespace's default role)
    --date YYYY-MM-DD     business date to load (default: D-1, yesterday in the country's local time)
    --SNS_TOPIC_ARN       optional: publish the result for the Teams card (role needs sns:Publish on it)
    --LOAD_QUEUE_URL      optional: SQS load queue. After 3 quick retries a temporary Redshift error re-queues the
                          load to run again in 10 minutes, up to 3 runs (role needs sqs:SendMessage on it)
    --ATTEMPT             set by the load dispatcher Lambda on re-queued runs (default 1)

Normally the dispatcher (lambda/load_dispatcher) starts this job when an extract succeeds; it can also be run by
hand with --date.

Steps:
  1. stage    for each table (a few at a time): recreate <base>_staging.<table> with <base>_analytics' columns minus
              loaded_at, then COPY that date's Parquet folder in.
  2. compare  rows identical to analytics (same values, by row fingerprint) are dropped from staging, and the new /
              updated rows of the tables with an external_id are counted. Reference data (menu, staff...) is in
              every day's files in full, but only what actually changed gets written.
  3. merge    one transaction, so reports never see a half-loaded day:
                * tables with an external_id: new rows inserted, changed rows updated (MERGE); unchanged rows untouched
                * child / link tables are synced per parent in the files: rows no longer present are removed, rows
                  not yet present are added (e.g. a line removed from an order disappears)
              loaded_at is set only on rows actually written, so it shows when a record last changed. The child
              tables' added / removed counts are read from the merge's own row counts, so they aren't hashed twice.
              Records that drop out of a full list (e.g. a deleted menu item) are kept in the main tables as history.
  4. report   reads a few basic KPIs for the business date from <base>_analytics and publishes them with a
              "loaded and ready" message. A failed load publishes the error instead (analytics is unchanged).
Loading the same date twice gives the same result, so reruns are safe.

SQL runs through the Redshift Data API as the job's IAM role (database user "IAMR:<role name>"), so there is
no database password. Column lists are read from Redshift itself, so this script has no other project imports.
"""
import sys, json, time, argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import boto3

# Each country has two schemas in the coldstone_web database: <base>_analytics, which reports read, and <base>_staging,
# where the load lands each run's files before merging. Staging is recreated every run.
SCHEMA_BASE = {'ng': 'coldstone_web_ng', 'ky': 'coldstone_web_ke'}
TARGET = {cc: f'{base}_analytics' for cc, base in SCHEMA_BASE.items()}
STAGING = {cc: f'{base}_staging' for cc, base in SCHEMA_BASE.items()}
TIMEZONES = {"ng": timezone(timedelta(hours=1)),   # Lagos, WAT
             "ky": timezone(timedelta(hours=3))}   # Nairobi, EAT

# Tables with their own external_id: upserted with MERGE
UPSERT_TABLES = ["branches", "roles", "permissions", "sub_permissions", "members", "menu_categories",
                 "menu_items", "menu_modifiers", "discounts", "wallets", "customers", "orders",
                 "activity_logs", "wallet_transactions"]
# Child / link table -> (its column holding the parent id, parent table). A parent's children are fully
# replaced, so lines removed from an order don't linger.
CHILDREN = {
    "order_items": ("order_external_id", "orders"),
    "order_item_modifiers": ("order_external_id", "orders"),
    "order_timeline": ("order_external_id", "orders"),
    "activity_initiators": ("activity_external_id", "activity_logs"),
    "branch_members": ("member_external_id", "members"),
    "member_additional_branches": ("member_external_id", "members"),
    "member_roles": ("member_external_id", "members"),
    "modifier_items": ("modifier_external_id", "menu_modifiers"),
    "discount_applicable_items": ("discount_external_id", "discounts"),
    "discount_combo_items": ("discount_external_id", "discounts"),
}
TABLES = UPSERT_TABLES + list(CHILDREN)
POLL_SECONDS = 2
MAX_BATCH = 40
# Tables staged at once. Each has its own staging table and S3 folder, so they don't conflict; kept small so the
# Data API and the workgroup aren't flooded with concurrent statements.
STAGE_WORKERS = 4
# A busy Redshift queues work rather than rejecting it, but a few errors are temporary: Data API throttling,
# too many statements at once, a concurrent transaction on the same table, a brief service or connection blip.
# Each step is one all-or-nothing transaction, so it is retried after a growing wait; anything else fails at once.
RETRY_WAITS = [30, 60, 120]  # seconds; 3 quick retries (3.5 minutes) before the load is re-queued
# If a step still fails with a temporary error, the whole load goes back on the SQS load queue and runs again
# REQUEUE_DELAY seconds later, up to MAX_ATTEMPTS runs in total. Permanent errors are never re-queued.
REQUEUE_DELAY = 600
MAX_ATTEMPTS = 3
TRANSIENT_CODES = {"ThrottlingException", "ActiveStatementsExceededException", "ActiveSessionsExceededException",
                   "InternalServerException", "ServiceUnavailableException", "ServiceUnavailable"}
TRANSIENT_ERRORS = ["serializable isolation violation", "concurrent transaction", "could not complete because of conflict",
                    "lock", "throttl", "too many", "timeout", "timed out", "connection", "terminating",
                    "temporarily unavailable", "try again", "insufficient capacity", "service unavailable",
                    "i/o error", "sending to the backend", "connection reset", "broken pipe", "unexpected eof"]


class TransientError(RuntimeError):
    pass


def is_transient(err):
    code = getattr(err, "response", {}).get("Error", {}).get("Code", "")
    return isinstance(err, TransientError) or code in TRANSIENT_CODES


def q(name):
    # Quote every identifier: "permissions" is a reserved word in Redshift
    return f'"{name}"'


# ---------- SQL ----------

PARENTS = sorted({parent for _, parent in CHILDREN.values()})


def ids_table(cc, parent):
    # Copy of the parent ids in this run's files, taken before unchanged parent rows are pruned from staging,
    # so each child table is still synced for every parent the files contain
    return f"{STAGING[cc]}.{q('_ids_' + parent)}"


def row_hash(ref, cols, types):
    # Fingerprint of a row's values, so unchanged rows can be recognised without comparing column by column.
    # NULL becomes '~' so it never equals a real value; booleans are spelled out because Redshift won't cast them.
    parts = [f"CASE WHEN {ref}.{q(c)} THEN 't' WHEN NOT {ref}.{q(c)} THEN 'f' ELSE '~' END" if types[c] == "boolean"
             else f"COALESCE(CAST({ref}.{q(c)} AS VARCHAR(65535)), '~')" for c in cols]
    return "MD5(" + " || '|' || ".join(parts) + ")"


def stage_sql(cc, table, cols, s3_url, copy_role):
    names = ", ".join(q(c) for c in cols)
    role = "default" if copy_role == "default" else f"'{copy_role}'"
    # CTAS with no rows copies analytics' exact column types (VARCHAR lengths included) in table order, which is
    # also the Parquet column order. loaded_at is left out because COPY from Parquet matches columns by position.
    sql = [f"DROP TABLE IF EXISTS {STAGING[cc]}.{q(table)}",
           f"CREATE TABLE {STAGING[cc]}.{q(table)} AS SELECT {names} FROM {TARGET[cc]}.{q(table)} WHERE 1 = 0",
           f"COPY {STAGING[cc]}.{q(table)} FROM '{s3_url}' IAM_ROLE {role} FORMAT AS PARQUET"]
    if table in PARENTS:
        sql += [f"DROP TABLE IF EXISTS {ids_table(cc, table)}",
                f"CREATE TABLE {ids_table(cc, table)} AS SELECT external_id FROM {STAGING[cc]}.{q(table)}"]
    return sql


def prune_sql(cc, table, columns, types):
    # Removes rows from one staging table that are identical to what analytics already holds, so the merge only
    # writes rows that are new or have changed. Runs before the merge, outside its transaction (staging is this
    # job's own).
    target, source = f"{TARGET[cc]}.{q(table)}", f"{STAGING[cc]}.{q(table)}"
    values = [c for c in columns[table] if c != "external_id"]
    return (f"DELETE FROM {source} USING {target} WHERE {source}.external_id = {target}.external_id "
            f"AND {row_hash(source, values, types[table])} = {row_hash(target, values, types[table])}")


def counts_sql(cc):
    # Row count of every staging table in one query, rather than a round trip per table
    return " UNION ALL ".join(f"SELECT '{table}', COUNT(*) FROM {STAGING[cc]}.{q(table)}" for table in TABLES)


def changes_sql(cc):
    # One read-only query giving, per table with an external_id, how many rows the merge will add and update.
    # Staging is already pruned to new and changed rows, so this is a cheap join. The child tables' counts come
    # from the merge itself (see child_changes), so their rows aren't fingerprinted twice.
    parts = []
    for table in UPSERT_TABLES:
        source, target = f"{STAGING[cc]}.{q(table)}", f"{TARGET[cc]}.{q(table)}"
        parts.append(f"SELECT '{table}', SUM(CASE WHEN t.external_id IS NULL THEN 1 ELSE 0 END), "
                     f"SUM(CASE WHEN t.external_id IS NULL THEN 0 ELSE 1 END) "
                     f"FROM {source} s LEFT JOIN {target} t ON t.external_id = s.external_id")
    return " UNION ALL ".join(parts)


def child_changes(result):
    # Added / removed rows per child table, from the merge batch's per-statement row counts. merge_sql puts the
    # upserts first, then a DELETE and an INSERT per child table. The Data API reports -1 when it has no count.
    rows = [max(s.get("ResultRows", -1), 0) for s in result.get("SubStatements", [])][len(UPSERT_TABLES):]
    rows += [0] * (2 * len(CHILDREN) - len(rows))
    return {table: {"new": rows[2 * i + 1], "updated": 0, "removed": rows[2 * i]} for i, table in enumerate(CHILDREN)}


def merge_sql(cc, columns, types):
    # Staging now holds only new and changed rows for the tables with an external_id (see prune_sql)
    upserts, children = [], []
    for table in UPSERT_TABLES:
        cols = columns[table]
        target, source = f"{TARGET[cc]}.{q(table)}", f"{STAGING[cc]}.{q(table)}"
        names = ", ".join(q(c) for c in cols)
        # Redshift's MERGE allows no alias on the target table, so both sides use their full names
        sets = ", ".join(f"{q(c)} = {source}.{q(c)}" for c in cols if c != "external_id")
        values = ", ".join(f"{source}.{q(c)}" for c in cols)
        upserts.append(f"MERGE INTO {target} USING {source} "
                       f"ON {target}.external_id = {source}.external_id "
                       f"WHEN MATCHED THEN UPDATE SET {sets}, loaded_at = GETDATE() "
                       f"WHEN NOT MATCHED THEN INSERT ({names}, loaded_at) VALUES ({values}, GETDATE())")
    for table, (fk, parent) in CHILDREN.items():
        cols, ty = columns[table], types[table]
        target, source = f"{TARGET[cc]}.{q(table)}", f"{STAGING[cc]}.{q(table)}"
        names = ", ".join(q(c) for c in cols)
        # Sync each parent's rows: remove rows no longer in the files, add rows not yet in analytics. A changed row
        # counts as one removed and one added; identical rows are left alone.
        children += [f"DELETE FROM {target} WHERE {target}.{q(fk)} IN (SELECT external_id FROM {ids_table(cc, parent)}) "
                     f"AND {row_hash(target, cols, ty)} NOT IN (SELECT {row_hash('s', cols, ty)} FROM {source} s)",
                     f"INSERT INTO {target} ({names}, loaded_at) SELECT {', '.join('s.' + q(c) for c in cols)}, GETDATE() "
                     f"FROM {source} s WHERE {row_hash('s', cols, ty)} NOT IN (SELECT {row_hash('t', cols, ty)} "
                     f"FROM {target} t WHERE t.{q(fk)} IN (SELECT external_id FROM {ids_table(cc, parent)}))"]
    # Parents first, so changed child rows always sit under an up-to-date parent
    return upserts + children


# ---------- Redshift Data API ----------

class Redshift:
    def __init__(self, workgroup, database):
        self.client = boto3.client("redshift-data")
        self.target = {"WorkgroupName": workgroup, "Database": database}

    def wait(self, sid, label):
        while True:
            d = self.client.describe_statement(Id=sid)
            if d["Status"] == "FINISHED":
                return d
            if d["Status"] in ("FAILED", "ABORTED"):
                error = d.get("Error", "no error message")
                kind = TransientError if any(t in error.lower() for t in TRANSIENT_ERRORS) else RuntimeError
                raise kind(f"{label}: {d['Status']}: {error}")
            time.sleep(POLL_SECONDS)

    def with_retries(self, label, step):
        # Runs one step (submit + wait), retrying temporary failures after a growing wait
        for attempt, pause in enumerate(RETRY_WAITS + [None], 1):
            try:
                return step()
            except Exception as err:
                if pause is None or not is_transient(err):
                    raise
                print(f"  {label}: temporary error ({str(err)[:200]}); retry {attempt}/{len(RETRY_WAITS)} in {pause}s")
                time.sleep(pause)

    def run(self, statements, label):
        # batch_execute_statement runs every statement in one transaction: all of them commit, or none.
        # It takes at most 40 statements; the merge is 34 (14 upserts + 10 child tables x 2).
        if len(statements) > MAX_BATCH:
            raise ValueError(f"{label}: {len(statements)} statements, the Data API allows {MAX_BATCH}")

        def step():
            sid = self.client.batch_execute_statement(Sqls=statements, StatementName=label, **self.target)["Id"]
            return self.wait(sid, label)
        return self.with_retries(label, step)

    def query(self, sql, label):
        def step():
            sid = self.client.execute_statement(Sql=sql, StatementName=label, **self.target)["Id"]
            self.wait(sid, label)
            return sid
        sid = self.with_retries(label, step)
        rows, token = [], None
        while True:
            page = self.client.get_statement_result(Id=sid, **({"NextToken": token} if token else {}))
            # Each field is {"stringValue": ...} / {"longValue": ...} / {"isNull": true}
            rows += [[None if v.get("isNull") else next(iter(v.values())) for v in r] for r in page["Records"]]
            token = page.get("NextToken")
            if not token:
                return rows

    def columns(self, cc):
        # Warehouse column names in table order, minus loaded_at (set by the merge, not in the files), and each
        # column's data type, used to fingerprint rows
        rows = self.query(f"SELECT table_name, column_name, data_type FROM svv_columns "
                          f"WHERE table_schema = '{TARGET[cc]}' AND column_name <> 'loaded_at' "
                          f"ORDER BY table_name, ordinal_position", f"coldstone-{cc}-columns")
        cols, types = {}, {}
        for table, column, data_type in rows:
            cols.setdefault(table, []).append(column)
            types.setdefault(table, {})[column] = (data_type or "").lower()
        missing = [t for t in TABLES if t not in cols]
        if missing:
            raise SystemExit(f"{TARGET[cc]} is missing {missing}: run sql/redshift/01_tables_{cc}.sql in the coldstone_web database, and check "
                             f"02_grants_{cc}.sql was run so this job's user can see the tables")
        return cols, types


# ---------- main ----------

def load_settings(argv):
    # Glue passes job parameters as --NAME value, plus arguments of its own that are ignored here
    parser = argparse.ArgumentParser()
    parser.add_argument("--COUNTRY", required=True, type=str.lower, choices=sorted(TIMEZONES))
    parser.add_argument("--S3_BUCKET", required=True)
    parser.add_argument("--REDSHIFT_WORKGROUP", required=True)
    parser.add_argument("--REDSHIFT_DATABASE", default="coldstone_web")
    parser.add_argument("--SCHEMA_BASE", help="override the schema name base (default: coldstone_web_ng / coldstone_web_ke)")
    parser.add_argument("--S3_PREFIX")
    parser.add_argument("--COPY_ROLE_ARN", default="default")
    parser.add_argument("--date", help="business date YYYY-MM-DD in the country's local time (default: yesterday)")
    parser.add_argument("--SNS_TOPIC_ARN")
    parser.add_argument("--JOB_RUN_ID")  # passed by Glue; included in the alert
    parser.add_argument("--LOAD_QUEUE_URL", help="SQS load queue; a load that hits temporary errors is re-queued here")
    parser.add_argument("--ATTEMPT", type=int, default=1, help="set by the load dispatcher for re-queued loads")
    args, _ = parser.parse_known_args(argv)
    args.S3_BUCKET = args.S3_BUCKET.strip().removeprefix("s3://").strip("/")
    args.S3_PREFIX = (args.S3_PREFIX or f"coldstone_web_{args.COUNTRY}").strip().strip("/")
    if args.SCHEMA_BASE:
        TARGET[args.COUNTRY] = f"{args.SCHEMA_BASE.strip()}_analytics"
        STAGING[args.COUNTRY] = f"{args.SCHEMA_BASE.strip()}_staging"
    return args


def to_int(value):
    # SUMs come back as numeric strings, COUNTs as longs, empty aggregates as None
    return int(float(value)) if value is not None else 0


def kpis(redshift, cc, business_date):
    # Basic KPIs for the business date (a quick snapshot for the alert, not the full report). The day runs from
    # local midnight to midnight, converted to UTC because timestamps are stored in UTC. Amounts are in minor
    # units (kobo / cents), as stored.
    tz = TIMEZONES[cc]
    start = datetime(business_date.year, business_date.month, business_date.day, tzinfo=tz).astimezone(timezone.utc)
    lo, hi = f"{start:%Y-%m-%d %H:%M:%S}", f"{start + timedelta(days=1):%Y-%m-%d %H:%M:%S}"
    day = f"FROM {TARGET[cc]}.orders WHERE created_at >= '{lo}' AND created_at < '{hi}'"
    # Sales count paid orders plus call-centre (cca) orders still marked pending, which are usually paid by bank
    # transfer outside the platform. The CCA pending part is also reported on its own. Other unpaid orders are left out.
    cca_pending = "(LOWER(channel) = 'cca' AND LOWER(payment_status) = 'pending')"
    counted = f"(LOWER(payment_status) = 'paid' OR {cca_pending})"
    (orders, counted_orders, sales, pending_orders, pending_amount, delivered, rejected, branches), = redshift.query(
        f"SELECT COUNT(*), SUM(CASE WHEN {counted} THEN 1 ELSE 0 END), SUM(CASE WHEN {counted} THEN total_price END), "
        f"SUM(CASE WHEN {cca_pending} THEN 1 ELSE 0 END), SUM(CASE WHEN {cca_pending} THEN total_price END), "
        f"SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END), SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END), "
        f"COUNT(DISTINCT branch_external_id) {day}", f"coldstone-{cc}-kpis")
    (new_customers,), = redshift.query(
        f"SELECT COUNT(*) FROM {TARGET[cc]}.customers WHERE created_at >= '{lo}' AND created_at < '{hi}'",
        f"coldstone-{cc}-kpi-customers")
    top = redshift.query(
        f"SELECT COALESCE(branch_name, 'unknown'), COUNT(*), SUM(total_price) {day} AND {counted} "
        f"GROUP BY 1 ORDER BY 3 DESC, 2 DESC LIMIT 3", f"coldstone-{cc}-kpi-branches")
    counted_orders, sales = to_int(counted_orders), to_int(sales)
    return {"orders": to_int(orders), "sales_orders": counted_orders, "sales_minor": sales,
            "avg_order_value_minor": sales // counted_orders if counted_orders else 0,
            "cca_pending_orders": to_int(pending_orders), "cca_pending_minor": to_int(pending_amount),
            "delivered": to_int(delivered), "rejected": to_int(rejected), "active_branches": to_int(branches),
            "new_customers": to_int(new_customers),
            "top_branches": [{"branch": b, "orders": to_int(n), "sales_minor": to_int(s)} for b, n, s in top]}


def publish(args, business_date, status, **extra):
    # Sends the load's result to SNS for the Teams card; an SNS problem must not fail a good load
    if not args.SNS_TOPIC_ARN:
        return
    msg = {"source": "coldstone-pipeline", "stage": "load", "country": args.COUNTRY, "status": status,
           "business_date": f"{business_date:%Y-%m-%d}", "schema": TARGET[args.COUNTRY],
           "finished_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "job_run_id": args.JOB_RUN_ID,
           **extra}
    try:
        boto3.client("sns").publish(TopicArn=args.SNS_TOPIC_ARN, Message=json.dumps(msg),
                                    Subject=f"Coldstone {args.COUNTRY.upper()} Redshift load {status}: {msg['business_date']}")
        print(f"Published {status} result to SNS")
    except Exception as err:
        print(f"warning: could not publish the result to SNS: {err}")


def requeue(args, business_date, err):
    # Puts this load back on the queue to run again after REQUEUE_DELAY. Returns False if it can't.
    if not args.LOAD_QUEUE_URL or args.ATTEMPT >= MAX_ATTEMPTS:
        return False
    body = {"source": "coldstone-load-requeue", "country": args.COUNTRY,
            "business_date": f"{business_date:%Y-%m-%d}", "attempt": args.ATTEMPT + 1}
    try:
        boto3.client("sqs").send_message(QueueUrl=args.LOAD_QUEUE_URL, MessageBody=json.dumps(body),
                                         DelaySeconds=REQUEUE_DELAY)
    except Exception as send_err:
        print(f"warning: could not re-queue the load: {send_err}")
        return False
    print(f"Redshift still busy after {len(RETRY_WAITS)} retries; load re-queued as attempt {args.ATTEMPT + 1}/"
          f"{MAX_ATTEMPTS}, starting in {REQUEUE_DELAY // 60} minutes")
    publish(args, business_date, "queued", error=str(err)[:500], next_attempt=args.ATTEMPT + 1,
            max_attempts=MAX_ATTEMPTS, retry_in_minutes=REQUEUE_DELAY // 60)
    return True


def main():
    args = load_settings(sys.argv[1:])
    business_date = (datetime.strptime(args.date, "%Y-%m-%d") if args.date
                     else datetime.now(TIMEZONES[args.COUNTRY]) - timedelta(days=1))
    print(f"Attempt {args.ATTEMPT}/{MAX_ATTEMPTS}")
    try:
        load(args, business_date)
    except (Exception, SystemExit) as err:  # SystemExit too: the script's own checks stop the run that way
        # analytics is untouched: the merge is one transaction and only commits if every statement succeeds.
        # A temporary error goes back on the queue (the job then ends normally); anything else fails the run.
        if is_transient(err) and requeue(args, business_date, err):
            return
        publish(args, business_date, "failed", error=str(err)[:1000], attempt=args.ATTEMPT, max_attempts=MAX_ATTEMPTS)
        raise


def load(args, business_date):
    cc = args.COUNTRY
    redshift = Redshift(args.REDSHIFT_WORKGROUP, args.REDSHIFT_DATABASE)
    print(f"Loading {cc.upper()} business date {business_date:%Y-%m-%d} from s3://{args.S3_BUCKET}/{args.S3_PREFIX}/ "
          f"into {args.REDSHIFT_WORKGROUP}/{args.REDSHIFT_DATABASE}.{TARGET[cc]}\n")

    columns, types = redshift.columns(cc)

    # 1. stage: the trailing slash stops 2026-10-1 matching 2026-10-12's folder. A table whose file is missing
    # fails its COPY, so a partial upload can't load silently. Tables are staged STAGE_WORKERS at a time; if one
    # fails, the ones not yet started are cancelled and the error ends the run as before.
    def stage(table):
        url = f"s3://{args.S3_BUCKET}/{args.S3_PREFIX}/processed/{table}/business_date={business_date:%Y-%m-%d}/"
        redshift.run(stage_sql(cc, table, columns[table], url, args.COPY_ROLE_ARN), f"coldstone-{cc}-stage-{table}")

    pool = ThreadPoolExecutor(STAGE_WORKERS)
    try:
        list(pool.map(stage, TABLES))
    finally:
        pool.shutdown(cancel_futures=True)
    counts = {table: to_int(n) for table, n in redshift.query(counts_sql(cc), f"coldstone-{cc}-counts")}
    for table in TABLES:
        print(f"  staged {table:28} {counts.get(table, 0)}")

    # Every extract contains the full branch list, so no branches means there were no files for this date (COPY
    # loads nothing without complaining when a folder is empty or missing). Stop rather than report a 0-row success.
    if not counts.get("branches"):
        raise SystemExit(f"No extract found for {business_date:%Y-%m-%d}: s3://{args.S3_BUCKET}/{args.S3_PREFIX}/processed/"
                         f"*/business_date={business_date:%Y-%m-%d}/ has no data (0 branches staged). Run "
                         f"coldstone-extract-{cc} for this date first; the load then starts automatically. "
                         f"{TARGET[cc]} was not changed.")

    # 2. drop unchanged rows from staging, then count what the merge will add and update
    # One table at a time. Skipping unchanged rows is only an optimisation: if it can't be done for a table, that
    # table's rows are all rewritten, as before, rather than failing the load.
    for table in UPSERT_TABLES:
        try:
            redshift.run([prune_sql(cc, table, columns, types)], f"coldstone-{cc}-skip-unchanged-{table}")
        except Exception as err:
            print(f"warning: could not skip unchanged {table} rows, so all of them will be rewritten "
                  f"({str(err)[:200]})")
    changes = {table: {"new": to_int(n), "updated": to_int(u), "removed": 0}
               for table, n, u in redshift.query(changes_sql(cc), f"coldstone-{cc}-changes")}

    # 3. merge: a failure here rolls back the whole day, and analytics stays as it was
    result = redshift.run(merge_sql(cc, columns, types), f"coldstone-{cc}-merge-{business_date:%Y%m%d}")
    changes.update(child_changes(result))
    totals = {k: sum(c[k] for c in changes.values()) for k in ("new", "updated", "removed")}
    print(f"\nMerged into {TARGET[cc]}. Checked {sum(counts.values())} staged rows: {totals['new']} new, "
          f"{totals['updated']} updated, {totals['removed']} removed; everything else unchanged")
    for table, c in changes.items():
        if any(c.values()):
            print(f"  {table:28} new {c['new']}, updated {c['updated']}, removed {c['removed']}")

    # 4. report: the KPI queries only read, so a problem with them doesn't undo the load; it's noted instead
    try:
        figures = kpis(redshift, cc, business_date)
        print(f"Basic KPIs for {business_date:%Y-%m-%d}: {json.dumps(figures)}")
    except Exception as err:
        figures = {"error": f"KPIs unavailable: {str(err)[:300]}"}
        print(f"warning: {figures['error']}")
    publish(args, business_date, "success", rows_checked=sum(counts.values()), changes=totals,
            changes_by_table={t: c for t, c in changes.items() if any(c.values())}, kpis=figures)


if __name__ == "__main__":
    main()
