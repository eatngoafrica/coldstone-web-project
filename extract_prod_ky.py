"""Coldstone Kenya production extract (AWS Glue Python shell): API -> S3 raw layer -> S3 processed layer.

Glue job parameters:
    --S3_BUCKET            required: landing bucket name (no s3://)
    --S3_PREFIX            folder in the bucket (default: coldstone_web_ky)
    --COLDSTONE_KEY_PARAM  SSM parameter holding the KY API key (default: COLDSTONE_KEY-KY)
    --date YYYY-MM-DD      rerun / backfill a business date (default: D-1, yesterday in Nairobi time)
    --full-history true    ONE-OFF initial load: every record created from the first order to the end of the
                           business date (default: yesterday), filed under that date. Load that date into
                           Redshift straight after; daily runs afterwards are D-1.
    --from-raw true        rebuild processed from the date's latest complete raw run, no API calls
The job's role needs s3:PutObject on <prefix>/*, s3:GetObject and s3:ListBucket on <prefix>/raw/*, and
ssm:GetParameter on the key parameter, and sns:Publish on the topic if --SNS_TOPIC_ARN is set.
    --SNS_TOPIC_ARN        optional: publish the run's result (row counts / problems) for the Teams card

Business date (D-1): records created on the business date (00:00 to 24:00 Nairobi), plus older records
updated on or after it, are pulled; anything created later waits for the next day's run. The warehouse MERGE makes
overlapping runs safe. Chowdeck orders are handled by a separate pipeline and are not extracted here.

Layers (under S3_PREFIX, both filed under the business date):
    raw/business_date=D/<endpoint>.jsonl.gz
        every API response exactly as received, one line per request, plus manifest.json saying whether
        the run completed. A rerun replaces the date's files (the bucket's versioning keeps the previous copy
        for 30 days). Raw holds whole API pages, so it includes records outside the window; the
        transform filters them out.
    processed/<table>/business_date=D/<table>.parquet
        the 24 tables, typed and checked. Column names, order and types match the Redshift tables in
        sql/redshift/01_tables.sql (minus loaded_at). Timestamps are UTC. A rerun replaces the date's files.
Raw is uploaded even if the pull or the checks fail; processed only when every data check passes.
Amounts stay in KES minor units (cents) exactly as the API returns them; no currency conversion.
"""
import os, sys, json, gzip, time, tempfile, argparse, requests
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

COUNTRY = 'ky'
BASE = "https://api.coldstonecreamery.co.ke/v1/public"
NAIROBI = timezone(timedelta(hours=3))  # EAT, no daylight saving
PAGE_SIZE = 200
DELAY = 1.1  # seconds between requests, keeps us under 60 req/min
RETRIES = 5
# The API always sorts newest-created first (sortBy is ignored), so records created before the cutoff
# but updated after it are only found by paging further back. Orders, customers and wallet transactions
# change after creation (status updates).
UPDATE_LOOKBACK_DAYS = 3
# --full-history cutoff: earlier than any Coldstone record, so every page is pulled
HISTORY_START = datetime(2000, 1, 1, tzinfo=timezone.utc)
# Glue's local disk is temporary: files are written here, then uploaded
WORK_DIR = os.path.join(tempfile.gettempdir(), "coldstone_web_ky")

# Parquet types match the Redshift column types, so COPY needs no conversion
STR, BOOL, F64, TS = pa.string(), pa.bool_(), pa.float64(), pa.timestamp("us")
I16, I32, I64 = pa.int16(), pa.int32(), pa.int64()

SCHEMAS = {t: pa.schema(cols) for t, cols in {
    "branches": [("external_id", STR), ("branch_code", STR), ("name", STR), ("address", STR),
                 ("latitude", F64), ("longitude", F64), ("is_active", BOOL), ("is_headquarters", BOOL),
                 ("is_private", BOOL), ("longest_distance", I32), ("customer_external_id", STR),
                 ("created_at", TS), ("updated_at", TS)],
    "roles": [("external_id", STR), ("name", STR), ("description", STR), ("created_at", TS),
              ("updated_at", TS)],
    "permissions": [("external_id", STR), ("name", STR), ("description", STR), ("created_at", TS),
                    ("updated_at", TS)],
    "sub_permissions": [("external_id", STR), ("name", STR), ("description", STR),
                        ("permission_external_id", STR), ("is_enabled", BOOL), ("created_at", TS),
                        ("updated_at", TS)],
    "members": [("external_id", STR), ("email", STR), ("username", STR), ("first_name", STR),
                ("last_name", STR), ("role_name", STR), ("is_deactivated", BOOL), ("verified", BOOL),
                ("phone_number", STR), ("customer_external_id", STR), ("branch_external_id", STR),
                ("created_at", TS), ("updated_at", TS)],
    "branch_members": [("branch_external_id", STR), ("member_external_id", STR)],
    "member_additional_branches": [("member_external_id", STR), ("branch_external_id", STR)],
    "member_roles": [("member_external_id", STR), ("role_external_id", STR)],
    "menu_categories": [("external_id", STR), ("name", STR)],
    "menu_items": [("external_id", STR), ("slug", STR), ("name", STR), ("description", STR), ("price", I64),
                   ("original_price", I64), ("lagos_price", I64), ("regional_price", I64), ("quantity", I32),
                   ("track_quantity", BOOL), ("is_cup_size", BOOL), ("cup_size", STR), ("has_sku", BOOL),
                   ("sku", STR), ("low_stock_threshold", I32), ("image", STR), ("channel", STR),
                   ("active", BOOL), ("out_of_stock", BOOL), ("customer_external_id", STR),
                   ("branch_external_id", STR), ("category_external_id", STR), ("created_at", TS),
                   ("updated_at", TS)],
    "menu_modifiers": [("external_id", STR), ("menu_item_external_id", STR), ("title", STR), ("position", I32),
                       ("quantity", I32), ("unit", STR), ("modifier_type", STR), ("channel", STR)],
    "modifier_items": [("modifier_external_id", STR), ("menu_item_external_id", STR)],
    "discounts": [("external_id", STR), ("code", STR), ("type", STR), ("target", STR), ("channel", STR),
                  ("category", STR), ("value", I64), ("start_date", TS), ("end_date", TS),
                  ("is_first_time_user_only", BOOL), ("minimum_order_amount", I64),
                  ("min_quantity_per_item", I32), ("partial_combo_threshold", pa.decimal128(5, 2)),
                  ("usage_limit", I32), ("usage_count", I32), ("active", BOOL),
                  ("partial_combo_allowed", BOOL), ("customer_external_id", STR), ("branch_external_id", STR),
                  ("created_at", TS), ("updated_at", TS)],
    "discount_applicable_items": [("discount_external_id", STR), ("item_external_id", STR)],
    "discount_combo_items": [("discount_external_id", STR), ("item_external_id", STR)],
    "wallets": [("external_id", STR), ("reference", STR), ("customer_external_id", STR),
                ("account_external_id", STR), ("branch_external_id", STR), ("balance", I64),
                ("current_balance", I64), ("temp_balance", I64), ("active", BOOL), ("account_name", STR),
                ("account_number", STR), ("bank_name", STR), ("created_at", TS), ("updated_at", TS)],
    "customers": [("external_id", STR), ("first_name", STR), ("last_name", STR), ("email", STR),
                  ("phone_number", STR), ("customer_external_id", STR), ("branch_external_id", STR),
                  ("active", BOOL), ("unsubscribed", BOOL), ("channel", STR), ("created_at", TS),
                  ("updated_at", TS)],
    "orders": [("external_id", STR), ("reference", STR), ("payment_method", STR), ("payment_status", STR),
               ("status", STR), ("channel", STR), ("subtotal", I64), ("delivery_fee", I64),
               ("total_price", I64), ("phone_number", STR), ("branch_external_id", STR), ("branch_name", STR),
               ("created_at", TS), ("updated_at", TS), ("customer_external_id", STR), ("customer_name", STR),
               ("delivery_type", STR)],
    "order_items": [("order_external_id", STR), ("line_no", I16), ("product_external_id", STR),
                    ("product_name", STR), ("price", I64), ("quantity", I32)],
    "order_item_modifiers": [("order_external_id", STR), ("line_no", I16), ("modifier_no", I16),
                             ("product_external_id", STR), ("modifier_external_id", STR),
                             ("modifier_name", STR), ("price", I64)],
    "order_timeline": [("order_external_id", STR), ("step_no", I16), ("title", STR), ("description", STR),
                       ("status", BOOL), ("created_at", TS), ("updated_at", TS)],
    "activity_logs": [("external_id", STR), ("description", STR), ("customer_external_id", STR),
                      ("branch_external_id", STR), ("initiator_type", STR), ("created_at", TS),
                      ("updated_at", TS)],
    "activity_initiators": [("activity_external_id", STR), ("initiator_external_id", STR), ("first_name", STR),
                            ("last_name", STR), ("username", STR), ("email", STR), ("role_name", STR),
                            ("initiator_type", STR)],
    "wallet_transactions": [("external_id", STR), ("reference", STR), ("amount", I64), ("type", STR),
                            ("status", STR), ("customer_external_id", STR), ("branch_external_id", STR),
                            ("description", STR), ("created_at", TS), ("updated_at", TS)],
}.items()}
COLUMNS = {t: s.names for t, s in SCHEMAS.items()}


# ---------- helpers ----------

def true_or_false(value):
    v = value.strip().lower()
    if v not in ("true", "false"):
        raise argparse.ArgumentTypeError(f"expected true or false, got {value!r}")
    return v == "true"


def load_settings(argv):
    # Glue passes job parameters as --NAME value, plus arguments of its own (--JOB_ID, --scriptLocation...)
    # that are ignored here
    parser = argparse.ArgumentParser()
    parser.add_argument("--S3_BUCKET", required=True)
    parser.add_argument("--S3_PREFIX", default="coldstone_web_ky")
    parser.add_argument("--COLDSTONE_KEY_PARAM", default="COLDSTONE_KEY-KY")
    parser.add_argument("--SNS_TOPIC_ARN")
    parser.add_argument("--JOB_RUN_ID")  # passed by Glue; included in the alert
    parser.add_argument("--date", help="business date YYYY-MM-DD in Nairobi time (default: yesterday)")
    # Glue job parameters always carry a value, so --from-raw takes an optional true/false
    parser.add_argument("--full-history", nargs="?", const="true", default="false", type=true_or_false,
                        help="true: one-off initial load, every record from the first order to the end of the date")
    parser.add_argument("--from-raw", nargs="?", const="true", default="false", type=true_or_false,
                        help="true: rebuild processed files from the date's latest complete raw run, no API calls")
    args, _ = parser.parse_known_args(argv)
    # Tolerate "s3://bucket/" or stray spaces typed into the console
    args.S3_BUCKET = args.S3_BUCKET.strip().removeprefix("s3://").strip("/")
    args.S3_PREFIX = args.S3_PREFIX.strip().strip("/")
    return args


_headers = None
_key_param = None


def headers():
    # The API key is read from SSM Parameter Store (SecureString) on first use and never written anywhere
    global _headers
    if _headers is None:
        key = boto3.client("ssm").get_parameter(Name=_key_param, WithDecryption=True)["Parameter"]["Value"]
        _headers = {"Authorization": f"Bearer {key}"}
    return _headers


def fix_ids(obj):
    # The API serialises ObjectIds and dates as {"0": "6", "1": "9", ...}; join them back into strings
    if isinstance(obj, dict):
        if obj and all(k.isdigit() for k in obj) and "0" in obj:
            return "".join(obj[str(i)] for i in range(len(obj)))
        return {k: fix_ids(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [fix_ids(v) for v in obj]
    return obj


def parse_ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def ts(value):
    # ISO UTC -> "YYYY-MM-DD HH:MM:SS" rounded to the nearest second, as stored in the database
    d = parse_ts(value)
    return (d + timedelta(milliseconds=500)).strftime("%Y-%m-%d %H:%M:%S") if d else None


def ref(value):
    # A related record may come back as an id string or an embedded object
    return value.get("_id") if isinstance(value, dict) else value


def b(value):
    return None if value is None else int(bool(value))


def clean(value):
    # Trim stray spaces (e.g. branch " satellite") and store empty strings as empty cells
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def changed_since(record, cutoff):
    d = parse_ts(record.get("updatedAt") or record.get("createdAt"))
    return d is not None and d >= cutoff


class RawRecorder:
    # Saves every API response untouched, one gzipped JSON Lines file per endpoint, streamed to disk so the
    # big menu pulls never sit in memory. manifest.json marks whether the run finished.
    def __init__(self, root, business_date, run_id, mode):
        # One folder per business date: a rerun overwrites the same files (S3 versioning keeps the old copy)
        self.rel_dir = f"raw/business_date={business_date:%Y-%m-%d}"
        self.dir = os.path.join(root, *self.rel_dir.split("/"))
        self.business_date, self.run_id, self.mode = business_date, run_id, mode
        self.handles, self.requests = {}, 0
        os.makedirs(self.dir, exist_ok=True)

    def record(self, path, params, body):
        name = path.replace("/", "_")
        if name not in self.handles:
            self.handles[name] = gzip.open(os.path.join(self.dir, f"{name}.jsonl.gz"), "wt", encoding="utf-8")
        line = {"path": path, "params": params, "fetched_at": datetime.now(timezone.utc).isoformat(),
                "response": body}
        self.handles[name].write(json.dumps(line, ensure_ascii=False) + "\n")
        self.requests += 1

    def close(self, complete):
        for h in self.handles.values():
            h.close()
        manifest = {"business_date": f"{self.business_date:%Y-%m-%d}", "run_id": self.run_id,
                    "mode": self.mode, "complete": complete, "requests": self.requests,
                    "endpoints": sorted(f"{n}.jsonl.gz" for n in self.handles)}
        with open(os.path.join(self.dir, "manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        return {f"{self.rel_dir}/{n}": os.path.join(self.dir, n) for n in os.listdir(self.dir)
                if os.path.isfile(os.path.join(self.dir, n))}


class RawReplay:
    # Serves saved responses instead of calling the API (--from-raw). The fetch logic asks each endpoint for
    # the same pages in the same order as when they were recorded, so every endpoint file is read one line
    # at a time as requests arrive: memory stays small however big the day's menus are.
    def __init__(self, run_dir):
        self.dir, self.readers = run_dir, {}

    def response(self, path, params):
        name = path.replace("/", "_")
        if name not in self.readers:
            file = os.path.join(self.dir, f"{name}.jsonl.gz")
            self.readers[name] = gzip.open(file, "rt", encoding="utf-8") if os.path.exists(file) else iter(())
        line = next(self.readers[name], None)
        x = json.loads(line) if line else None
        if not x or x["path"] != path or x["params"] != params:
            raise SystemExit(f"--from-raw: the saved run has no {path} {params} at this point; the fetch logic "
                             f"has changed since it was recorded, so pull from the API instead")
        return x["response"]


_raw = None  # RawRecorder for a normal run, RawReplay for --from-raw
_until = None  # --full-history: only records created before this (the end of the business date)


def created_before(record, until):
    d = parse_ts(record.get("createdAt"))
    return until is None or d is None or d < until


def retry_after(value, default=30):
    # Retry-After may be a number of seconds or an HTTP date
    if not value:
        return default
    try:
        return max(0, int(value))
    except ValueError:
        try:
            return max(0, int((parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()))
        except (TypeError, ValueError):
            return default


def get(path, **params):
    if isinstance(_raw, RawReplay):
        return fix_ids(_raw.response(path, params))["data"]
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(f"{BASE}/{path}", headers=headers(), params=params, timeout=60)
        except (requests.ConnectionError, requests.Timeout) as err:
            if attempt == RETRIES:
                raise
            print(f"  {path}: {err.__class__.__name__}, retry {attempt}")
            time.sleep(5 * attempt)
            continue
        if r.status_code == 429:
            wait = retry_after(r.headers.get("Retry-After"))
            print(f"  rate limited, waiting {wait}s")
            time.sleep(wait)
            continue
        if r.status_code >= 500 and attempt < RETRIES:
            print(f"  {path}: HTTP {r.status_code}, retry {attempt}")
            time.sleep(5 * attempt)
            continue
        time.sleep(DELAY)
        r.raise_for_status()
        body = r.json()
        if _raw:
            _raw.record(path, params, body)
        return fix_ids(body)["data"]
    raise RuntimeError(f"{path}: gave up after {RETRIES} attempts")


def page_items(data, list_key):
    return (data.get(list_key) or []) if isinstance(data, dict) else (data or [])


def fetch_all(path, list_key, **params):
    # Full snapshot: every page until the API runs out
    rows, page = [], 1
    while True:
        data = get(path, page=page, limit=PAGE_SIZE, **params)
        items = page_items(data, list_key)
        rows += items
        meta = data.get("meta") if isinstance(data, dict) else None
        more = meta.get("hasNextPage") if meta else len(items) >= PAGE_SIZE
        if not items or not more:
            return rows
        page += 1


def fetch_since(path, list_key, cutoff, lookback_days=0, **params):
    # Pages newest-created first and keeps records created or updated since the cutoff. Paging continues
    # lookback_days past the cutoff (by createdAt) to catch older records that were updated recently.
    stop_at = cutoff - timedelta(days=lookback_days)
    rows, page = [], 1
    while True:
        data = get(path, page=page, limit=PAGE_SIZE, **params)
        items = page_items(data, list_key)
        rows += [x for x in items if changed_since(x, cutoff) and created_before(x, _until)]
        created = [parse_ts(x.get("createdAt")) for x in items if x.get("createdAt")]
        has_next = isinstance(data, dict) and (data.get("meta") or {}).get("hasNextPage")
        if not items or not has_next or (created and min(created) < stop_at):
            return rows
        page += 1


def to_int(value):
    # Whole numbers only: a fractional amount would be silently truncated in a BIGINT column
    if isinstance(value, int):
        return int(value)
    f = float(value)
    if not f.is_integer():
        raise ValueError(f"{value!r} is not a whole number")
    return int(f)


def convert(value, typ):
    # Values arrive as API strings / numbers / 0-1 flags; turn them into the column's Parquet type
    if value is None:
        return None
    if pa.types.is_string(typ):
        return str(value)
    if pa.types.is_boolean(typ):
        return bool(value)
    if pa.types.is_integer(typ):
        return to_int(value)
    if pa.types.is_floating(typ):
        return float(value)
    if pa.types.is_decimal(typ):
        return Decimal(str(value))
    if pa.types.is_timestamp(typ):
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    raise TypeError(f"no converter for {typ}")


class Tables:
    def __init__(self):
        self.rows = {t: {} for t in COLUMNS}

    def add(self, table, key, **values):
        # Keyed so the same record seen via several branches is written once
        self.rows[table][key] = [clean(values.get(c)) for c in COLUMNS[table]]

    def to_arrow(self):
        # Returns ({table: pyarrow Table}, problems). A value that doesn't fit its column is a problem
        # (the run fails) and is left empty so the rest of the table can still be built for inspection.
        tables, problems = {}, []
        for table, schema in SCHEMAS.items():
            rows = list(self.rows[table].values())
            arrays = []
            for i, field in enumerate(schema):
                values, bad = [], []
                for r in rows:
                    try:
                        values.append(convert(r[i], field.type))
                    except (ValueError, TypeError) as err:
                        values.append(None)
                        bad.append(str(err))
                try:
                    arrays.append(pa.array(values, type=field.type))
                except (pa.ArrowInvalid, OverflowError) as err:
                    arrays.append(pa.nulls(len(values), type=field.type))
                    bad.append(str(err))
                if bad:
                    problems.append(f"{table}.{field.name}: {len(bad)} values don't fit {field.type} "
                                    f"(e.g. {bad[0]})")
            tables[table] = pa.Table.from_arrays(arrays, schema=schema)
        return tables, problems

    def write(self, tables, business_date, root):
        # One folder per table and business date, the same layout as S3; a rerun overwrites it.
        # Returns {path relative to root: local file}.
        files = {}
        for table, data in tables.items():
            rel = f"processed/{table}/business_date={business_date:%Y-%m-%d}/{table}.parquet"
            local = os.path.join(root, *rel.split("/"))
            os.makedirs(os.path.dirname(local), exist_ok=True)
            pq.write_table(data, local, compression="snappy")
            files[rel] = local
        return files


def upload(files, bucket, prefix, layer):
    s3 = boto3.client("s3")
    for rel, local in files.items():
        s3.upload_file(local, bucket, f"{prefix}/{rel}")
    print(f"Uploaded {len(files)} {layer} files to s3://{bucket}/{prefix}/{layer}/")


def latest_raw_run(business_date, root, bucket, prefix):
    # Downloads the date's raw files (one set per date; a rerun replaces them) and checks the run completed
    date_dir = f"raw/business_date={business_date:%Y-%m-%d}"
    local = os.path.join(root, *date_dir.split("/"))
    os.makedirs(local, exist_ok=True)
    s3, key_prefix = boto3.client("s3"), f"{prefix}/{date_dir}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key_prefix):
        for o in page.get("Contents", []):
            name = o["Key"][len(key_prefix):]
            if name and "/" not in name:  # skips run=... folders left by the earlier per-run layout
                s3.download_file(bucket, o["Key"], os.path.join(local, name))
    manifest = os.path.join(local, "manifest.json")
    if not os.path.exists(manifest):
        raise SystemExit(f"--from-raw: no raw run for {business_date:%Y-%m-%d} under s3://{bucket}/{key_prefix}")
    with open(manifest, encoding="utf-8") as f:
        if not json.load(f)["complete"]:
            raise SystemExit(f"--from-raw: the raw run for {business_date:%Y-%m-%d} did not complete; pull from the API instead")
    return local

def publish(args, business_date, mode, status, t, problems):
    # Sends the run's result to SNS for the Teams card. An SNS outage must not fail a good load, so errors
    # are only logged; a failed run still exits with an error afterwards.
    if not args.SNS_TOPIC_ARN:
        return
    msg = {'source': 'coldstone-pipeline', 'stage': 'extract', 'country': COUNTRY, 'mode': mode, 'status': status,
           'business_date': f'{business_date:%Y-%m-%d}',
           'finished_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
           'job_run_id': args.JOB_RUN_ID, 'row_counts': {k: len(v) for k, v in t.rows.items()},
           'problems': problems}
    try:
        boto3.client('sns').publish(TopicArn=args.SNS_TOPIC_ARN, Message=json.dumps(msg),
                                    Subject=f'Coldstone {COUNTRY.upper()} {status}: {msg["business_date"]}')
        print(f'Published {status} result to SNS')
    except Exception as err:
        print(f'warning: could not publish the result to SNS: {err}')


# ---------- table builders ----------

def add_branch(t, x):
    t.add("branches", x["_id"], external_id=x["_id"], branch_code=x.get("branchCode") or None,
          name=x.get("name"), address=x.get("address"), latitude=x.get("lat"), longitude=x.get("lng"),
          is_active=b(x.get("isActive")), is_headquarters=b(x.get("isHeadquarters")),
          is_private=b(x.get("private")), longest_distance=x.get("longestDistance"),
          customer_external_id=ref(x.get("customerId")), created_at=ts(x.get("createdAt")),
          updated_at=ts(x.get("updatedAt")))


def add_customer(t, x):
    t.add("customers", x["_id"], external_id=x["_id"], first_name=x.get("firstName"),
          last_name=x.get("lastName"), email=x.get("email"), phone_number=x.get("phoneNumber"),
          customer_external_id=ref(x.get("customer")), branch_external_id=ref(x.get("branch")),
          active=b(x.get("active")), unsubscribed=b(x.get("unsubscribed")), channel=x.get("channel"),
          created_at=ts(x.get("createdAt")), updated_at=ts(x.get("updatedAt")))


def add_member(t, x):
    mid, branch = x["_id"], ref(x.get("branch"))
    t.add("members", mid, external_id=mid, email=x.get("email"), username=x.get("username"),
          first_name=x.get("firstName"), last_name=x.get("lastName"), role_name=x.get("role"),
          is_deactivated=b(x.get("isDeactivated")), verified=b(x.get("verified")),
          phone_number=x.get("phoneNumber"), customer_external_id=ref(x.get("customer")),
          branch_external_id=branch, created_at=ts(x.get("createdAt")), updated_at=ts(x.get("updatedAt")))
    if branch:
        t.add("branch_members", (branch, mid), branch_external_id=branch, member_external_id=mid)
    for extra in x.get("additionalBranches") or []:
        t.add("member_additional_branches", (mid, ref(extra)), member_external_id=mid,
              branch_external_id=ref(extra))
    for role in x.get("roles") or []:
        t.add("member_roles", (mid, ref(role)), member_external_id=mid, role_external_id=ref(role))


def add_role(t, x):
    t.add("roles", x["_id"], external_id=x["_id"], name=x.get("name"), description=x.get("description"),
          created_at=ts(x.get("createdAt")), updated_at=ts(x.get("updatedAt")))
    for p in x.get("permissions") or []:
        if isinstance(p, dict):
            t.add("permissions", p["_id"], external_id=p["_id"], name=p.get("name"),
                  description=p.get("description"), created_at=ts(p.get("createdAt")),
                  updated_at=ts(p.get("updatedAt")))
    for s in x.get("enabledSubPermissions") or []:
        if isinstance(s, dict):
            t.add("sub_permissions", s["_id"], external_id=s["_id"], name=s.get("name"),
                  description=s.get("description"), permission_external_id=ref(s.get("permission")),
                  is_enabled=b(s.get("isEnabled")), created_at=ts(s.get("createdAt")),
                  updated_at=ts(s.get("updatedAt")))


def add_menu_item(t, x):
    iid = x["_id"]
    t.add("menu_items", iid, external_id=iid, slug=x.get("slug"), name=x.get("name"),
          description=x.get("description"), price=x.get("price"), original_price=x.get("originalPrice"),
          lagos_price=x.get("lagosPrice"), regional_price=x.get("regionalPrice"), quantity=x.get("quantity"),
          track_quantity=b(x.get("trackQuantity")), is_cup_size=b(x.get("isCupSize")),
          cup_size=x.get("cupSize"), has_sku=b(x.get("hasSku")), sku=x.get("sku"),
          low_stock_threshold=x.get("lowStockThreshold"), image=x.get("image"), channel=x.get("channel"),
          active=b(x.get("active")), out_of_stock=b(x.get("outOfStock")),
          customer_external_id=ref(x.get("customer")), branch_external_id=ref(x.get("branch")),
          category_external_id=ref(x.get("category")), created_at=ts(x.get("createdAt")),
          updated_at=ts(x.get("updatedAt")))
    for m in x.get("modifiers") or []:
        if not isinstance(m, dict):
            continue
        t.add("menu_modifiers", m["_id"], external_id=m["_id"], menu_item_external_id=iid,
              title=m.get("title"), position=m.get("position"), quantity=m.get("quantity"),
              unit=m.get("unit"), modifier_type=m.get("modifierType"), channel=m.get("channel"))
        for item in m.get("menuItemId") or []:
            t.add("modifier_items", (m["_id"], ref(item)), modifier_external_id=m["_id"],
                  menu_item_external_id=ref(item))


def add_category(t, x):
    t.add("menu_categories", x["_id"], external_id=x["_id"], name=x.get("name"))


def add_discount(t, x):
    did = x["_id"]
    t.add("discounts", did, external_id=did, code=x.get("code"), type=x.get("type"), target=x.get("target"),
          channel=x.get("channel"), category=x.get("category"), value=x.get("value"),
          start_date=ts(x.get("startDate")), end_date=ts(x.get("endDate")),
          is_first_time_user_only=b(x.get("isFirstTimeUserOnly")),
          minimum_order_amount=x.get("minimumOrderAmount"), min_quantity_per_item=x.get("minQuantityPerItem"),
          partial_combo_threshold=x.get("partialComboThreshold"), usage_limit=x.get("usageLimit"),
          usage_count=x.get("usageCount"), active=b(x.get("active")),
          partial_combo_allowed=b(x.get("partialComboAllowed")), customer_external_id=ref(x.get("customer")),
          branch_external_id=ref(x.get("branch")), created_at=ts(x.get("createdAt")),
          updated_at=ts(x.get("updatedAt")))
    for item in x.get("applicableItems") or []:
        t.add("discount_applicable_items", (did, ref(item)), discount_external_id=did, item_external_id=ref(item))
    for item in x.get("comboItems") or []:
        t.add("discount_combo_items", (did, ref(item)), discount_external_id=did, item_external_id=ref(item))


def add_order(t, x):
    oid, branch, cust = x["_id"], x.get("branch") or {}, x.get("customer") or {}
    t.add("orders", oid, external_id=oid, reference=x.get("reference"), payment_method=x.get("paymentMethod"),
          payment_status=x.get("paymentStatus"), status=x.get("status"), channel=x.get("channel"),
          subtotal=x.get("subtotal"), delivery_fee=x.get("deliveryFee"), total_price=x.get("totalPrice"),
          phone_number=x.get("phoneNumber"), branch_external_id=ref(branch),
          branch_name=branch.get("name") if isinstance(branch, dict) else None,
          created_at=ts(x.get("createdAt")), updated_at=ts(x.get("updatedAt")),
          customer_external_id=ref(cust),
          customer_name=" ".join(filter(None, [cust.get("firstName"), cust.get("lastName")]))
          if isinstance(cust, dict) else None,
          delivery_type=x.get("deliveryType"))
    for i, p in enumerate(x.get("products") or []):
        pid = ref(p.get("product"))
        t.add("order_items", (oid, i), order_external_id=oid, line_no=i + 1, product_external_id=pid,
              product_name=(p.get("productDetails") or {}).get("name"), price=p.get("price"),
              quantity=p.get("quantity"))
        for j, m in enumerate(p.get("selectedModifiersProducts") or []):
            t.add("order_item_modifiers", (oid, i, j), order_external_id=oid, line_no=i + 1, modifier_no=j + 1,
                  product_external_id=pid,
                  modifier_external_id=m.get("_id"), modifier_name=m.get("name"), price=m.get("price"))
    for i, s in enumerate(x.get("timeline") or []):
        t.add("order_timeline", (oid, i), order_external_id=oid, step_no=i + 1, title=s.get("title"),
              description=s.get("description"), status=b(s.get("status")),
              created_at=ts(s.get("createdAt")), updated_at=ts(s.get("updatedAt")))


def add_activity(t, x):
    aid = x["_id"]
    t.add("activity_logs", aid, external_id=aid, description=x.get("description"),
          customer_external_id=ref(x.get("customer")), branch_external_id=ref(x.get("branch")),
          initiator_type=x.get("initiatorType"), created_at=ts(x.get("createdAt")),
          updated_at=ts(x.get("updatedAt")))
    for who in x.get("initiator") or []:
        if isinstance(who, dict):
            t.add("activity_initiators", (aid, who["_id"]), activity_external_id=aid,
                  initiator_external_id=who["_id"], first_name=who.get("firstName"),
                  last_name=who.get("lastName"), username=who.get("username"), email=who.get("email") or None,
                  role_name=who.get("role"), initiator_type=x.get("initiatorType"))


def add_wallet(t, x):
    t.add("wallets", x["_id"], external_id=x["_id"], reference=x.get("reference"),
          customer_external_id=ref(x.get("customer")), account_external_id=ref(x.get("account")),
          branch_external_id=ref(x.get("branch")), balance=x.get("balance"),
          current_balance=x.get("currentBalance"), temp_balance=x.get("tempBalance"), active=b(x.get("active")),
          account_name=x.get("accountName"), account_number=x.get("accountNumber"), bank_name=x.get("bankName"),
          created_at=ts(x.get("createdAt")), updated_at=ts(x.get("updatedAt")))


def add_wallet_tx(t, x):
    t.add("wallet_transactions", x["_id"], external_id=x["_id"], reference=x.get("reference"),
          amount=x.get("amount"), type=x.get("type"), status=x.get("status"),
          customer_external_id=ref(x.get("customer")), branch_external_id=ref(x.get("branch")),
          description=x.get("description"), created_at=ts(x.get("createdAt")), updated_at=ts(x.get("updatedAt")))


# ---------- main ----------

def extract(t, cutoff):
    # Transactions: incremental. Orders, customers and wallet transactions page back further for late updates.
    for path, list_key, builder, lookback in [("orders", "orders", add_order, UPDATE_LOOKBACK_DAYS),
                                              ("customers", "users", add_customer, UPDATE_LOOKBACK_DAYS),
                                              ("activity-logs", "activities", add_activity, 0)]:
        rows = fetch_since(path, list_key, cutoff, lookback)
        for x in rows:
            builder(t, x)
        print(f"{path}: {len(rows)}")

    # Reference data: small, so a full snapshot every run keeps the warehouse complete
    for path, list_key, builder in [("discounts", "discounts", add_discount), ("roles", "roles", add_role)]:
        rows = fetch_all(path, list_key)
        for x in rows:
            builder(t, x)
        print(f"{path}: {len(rows)} (full)")

    # Branches come back in one unpaged list. A count landing exactly on a typical page size suggests the
    # API cut the list short, which would silently drop every later branch's menu, staff and wallet.
    all_branches = get("branches")
    if len(all_branches) in (50, 100, 200, 500, 1000):
        raise SystemExit(f"branches: got exactly {len(all_branches)}, which looks like a truncated page; "
                         f"check whether the branches endpoint needs paging")
    for x in all_branches:
        add_branch(t, x)
    print(f"branches: {len(all_branches)} (full)")

    # Branch-scoped endpoints: menu, staff and wallets in full; wallet transactions incremental
    for n, branch in enumerate(all_branches, 1):
        bid = branch["_id"]
        print(f"[{n}/{len(all_branches)}] {branch['name'].strip()}")
        for x in fetch_all("members", None, branchId=bid):
            add_member(t, x)
        for x in fetch_all("menus", "products", branchId=bid):
            add_menu_item(t, x)
        for x in fetch_all("modifiers", "products", branchId=bid):
            add_menu_item(t, x)
        for x in fetch_all("categories", "categories", branchId=bid):
            add_category(t, x)
        wallet = get("wallet/balance", branchId=bid)
        if isinstance(wallet, dict) and wallet.get("_id"):
            add_wallet(t, wallet)
        for x in fetch_since("wallet/transactions", "data", cutoff, UPDATE_LOOKBACK_DAYS, branchId=bid):
            add_wallet_tx(t, x)


def main():
    global _raw, _key_param, _until
    args = load_settings(sys.argv[1:])
    bucket, prefix, root = args.S3_BUCKET, args.S3_PREFIX, WORK_DIR
    _key_param = args.COLDSTONE_KEY_PARAM
    mode = 'full_history' if args.full_history else 'd-1'

    # D-1: the business date is yesterday in Nairobi time unless --date is given. Everything created or
    # updated from 00:00 Nairobi on that date to the end of that date is kept.
    now = datetime.now(NAIROBI)
    start = (datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=NAIROBI) if args.date
             else (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0))
    cutoff = start.astimezone(timezone.utc)
    # Every run stops at the end of the business date: records created later wait for the next day's run
    _until = (start + timedelta(days=1)).astimezone(timezone.utc)
    print(f"Target: s3://{bucket}/{prefix}/   API key: SSM {_key_param}")

    t = Tables()
    if args.from_raw:
        run_dir = latest_raw_run(start, root, bucket, prefix)
        with open(os.path.join(run_dir, "manifest.json"), encoding="utf-8") as f:
            if json.load(f).get("mode") == "full_history":
                cutoff = HISTORY_START  # replay with the same window the run was recorded with
                mode = 'full_history'
        print(f"Business date {start:%Y-%m-%d}: rebuilding from raw run {os.path.basename(run_dir)} (no API calls)\n")
        _raw = RawReplay(run_dir)
        extract(t, cutoff)
    else:
        if args.full_history:
            # One-off initial load: everything since the first order, filed under this run's business date so
            # the normal load step picks it up. Daily runs afterwards are plain D-1.
            cutoff = HISTORY_START
            print(f"Business date {start:%Y-%m-%d}: FULL HISTORY, every record created from the first order to the end of {start:%Y-%m-%d}\n")
        else:
            print(f"Business date {start:%Y-%m-%d}: pulling records created or updated since "
                  f"{start:%Y-%m-%d %H:%M} Nairobi ({cutoff:%Y-%m-%d %H:%M} UTC)\n")
        _raw = RawRecorder(root, start, datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
                           "full_history" if args.full_history else "d-1")
        complete = False
        try:
            extract(t, cutoff)
            complete = True
        finally:
            # Raw is kept even when the pull or the checks fail: that is when it is needed to investigate
            raw_files = _raw.close(complete)
            print(f"\nRaw: {_raw.requests} API responses saved")
            upload(raw_files, bucket, prefix, "raw")

    problems = check_quality(t)
    tables, type_problems = t.to_arrow()
    problems += type_problems

    files = t.write(tables, start, root)
    print(f"\nProcessed: business_date={start:%Y-%m-%d}")
    for table, rows in t.rows.items():
        print(f"  {table:28} {len(rows)}")
    print("  messages / message_recipients: not available from the API")

    if problems:
        print("\nData quality problems (processed files not uploaded):")
        for p in problems:
            print(f"  - {p}")
        publish(args, start, mode, 'failed', t, problems)
        raise SystemExit(1)
    print("\nData quality checks passed")
    upload(files, bucket, prefix, "processed")
    publish(args, start, mode, 'success', t, [])


def check_quality(t):
    # Hard failures only: a blank key or a child row whose parent is missing means the load would be wrong
    problems = []
    for table, cols in COLUMNS.items():
        if cols[0] == "external_id":
            blank = sum(1 for r in t.rows[table].values() if not r[0])
            if blank:
                problems.append(f"{table}: {blank} rows with no external_id")

    def ids(table):
        return {r[0] for r in t.rows[table].values()}

    for child, col, parent in [("order_items", "order_external_id", "orders"),
                               ("order_item_modifiers", "order_external_id", "orders"),
                               ("order_timeline", "order_external_id", "orders"),
                               ("activity_initiators", "activity_external_id", "activity_logs"),
                               ("menu_modifiers", "menu_item_external_id", "menu_items"),
                               ("modifier_items", "modifier_external_id", "menu_modifiers"),
                               ("discount_applicable_items", "discount_external_id", "discounts"),
                               ("discount_combo_items", "discount_external_id", "discounts"),
                               ("member_roles", "member_external_id", "members"),
                               ("member_additional_branches", "member_external_id", "members"),
                               ("branch_members", "member_external_id", "members")]:
        i, parent_ids = COLUMNS[child].index(col), ids(parent)
        orphans = sum(1 for r in t.rows[child].values() if r[i] not in parent_ids)
        if orphans:
            problems.append(f"{child}: {orphans} rows point to a missing {parent} record")

    # Warnings: worth knowing, but they come from the source data rather than the extract
    branch_ids = ids("branches")
    for table in ["orders", "wallets", "wallet_transactions", "activity_logs"]:
        i = COLUMNS[table].index("branch_external_id")
        unknown = sum(1 for r in t.rows[table].values() if r[i] and r[i] not in branch_ids)
        if unknown:
            print(f"warning: {table} has {unknown} rows for branches not in the branch list")
    return problems


if __name__ == "__main__":
    main()
