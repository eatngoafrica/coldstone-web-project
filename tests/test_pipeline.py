"""CI checks that the pipeline's pieces still agree with each other, without touching AWS.

  * each extract script's Parquet schemas match its country's Redshift DDL (same tables, same columns in the
    same order, compatible types), so COPY won't fail
  * the NG and KY extracts write identical table layouts
  * the load job's table lists cover exactly the extract's tables, and its schema names exist in the DDL
  * the load dispatcher and Teams notifier Lambdas parse the messages the pipeline sends
  * a pull split into parts (to fit Lambda's 15 minutes) rebuilds exactly the same day as a single pull
  * the Lambda entry points pass settings through, and the state machine matches template.yaml
"""
import importlib.util, json, os, re, sys
from pathlib import Path

import pyarrow as pa
import pytest

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")  # the Lambdas create boto3 clients on import


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


extract = {"ng": load("extract_prod_ng", "extract_prod_ng.py"), "ky": load("extract_prod_ky", "extract_prod_ky.py")}
loader = load("load_redshift", "load_redshift.py")
dispatcher = load("load_dispatcher", "lambda/load_dispatcher/handler.py")
notifier = load("teams_notifier", "lambda/teams_notifier/handler.py")
entry = load("entry", "lambda/pipeline/entry.py")

SQL_TYPES = {"VARCHAR": pa.string(), "BOOLEAN": pa.bool_(), "DOUBLE PRECISION": pa.float64(),
             "TIMESTAMP": pa.timestamp("us"), "SMALLINT": pa.int16(), "INTEGER": pa.int32(), "BIGINT": pa.int64()}
TABLE_RE = re.compile(r"CREATE TABLE IF NOT EXISTS (\w+)\.\"?(\w+)\"?\s*\((.*?)\n\)", re.S)
COLUMN_RE = re.compile(r"^\s+\"?([a-z_]+)\"?\s+(DOUBLE PRECISION|[A-Z]+)(?:\((\d+)(?:,\s*(\d+))?\))?", re.M)


def sql_type(name, precision, scale):
    if name == "DECIMAL":
        return pa.decimal128(int(precision), int(scale))
    return SQL_TYPES[name]


def ddl(country):
    """{schema: {table: [(column, arrow type)]}} from sql/redshift/01_tables_<country>.sql"""
    text = (ROOT / f"sql/redshift/01_tables_{country}.sql").read_text(encoding="utf-8")
    tables = {}
    for schema, table, body in TABLE_RE.findall(text):
        tables.setdefault(schema, {})[table] = [(col, sql_type(*rest)) for col, *rest in COLUMN_RE.findall(body)]
    return tables


@pytest.mark.parametrize("country", ["ng", "ky"])
def test_parquet_matches_redshift(country):
    analytics = ddl(country)[loader.TARGET[country]]
    schemas = extract[country].SCHEMAS
    assert set(schemas) == set(analytics), "extract tables and Redshift tables differ"
    for table, schema in schemas.items():
        redshift = [(col, typ) for col, typ in analytics[table] if col != "loaded_at"]
        parquet = [(field.name, field.type) for field in schema]
        assert parquet == redshift, f"{table}: Parquet columns/types don't match 01_tables_{country}.sql"


def test_ng_and_ky_extracts_write_the_same_tables():
    assert extract["ng"].SCHEMAS == extract["ky"].SCHEMAS


@pytest.mark.parametrize("country", ["ng", "ky"])
def test_extract_country_settings(country):
    module = extract[country]
    assert module.COUNTRY == country
    assert f"coldstone_web_{country}" in module.WORK_DIR


@pytest.mark.parametrize("country", ["ng", "ky"])
def test_load_job_schemas_exist(country):
    text = (ROOT / f"sql/redshift/01_tables_{country}.sql").read_text(encoding="utf-8")
    schemas = set(re.findall(r"CREATE SCHEMA IF NOT EXISTS (\w+);", text))
    assert {loader.TARGET[country], loader.STAGING[country]} <= schemas
    assert country in loader.TIMEZONES


def test_load_job_tables_match_extract():
    tables = set(extract["ng"].SCHEMAS)
    assert len(loader.TABLES) == len(set(loader.TABLES)), "a table is listed twice in load_redshift.py"
    assert set(loader.TABLES) == tables
    for table in loader.UPSERT_TABLES:
        assert "external_id" in extract["ng"].COLUMNS[table], f"{table} is upserted but has no external_id"
    for child, (column, parent) in loader.CHILDREN.items():
        assert column in extract["ng"].COLUMNS[child], f"{child} has no {column}"
        assert parent in loader.UPSERT_TABLES, f"{child}'s parent {parent} isn't upserted"


def sqs_body(msg, wrap_in_sns=False):
    body = json.dumps(msg)
    return json.dumps({"Type": "Notification", "TopicArn": "arn:aws:sns:x", "Message": body}) if wrap_in_sns else body


@pytest.mark.parametrize("wrap", [False, True])
def test_dispatcher_starts_load_after_extract_success(wrap):
    msg = {"source": "coldstone-pipeline", "stage": "extract", "status": "success", "country": "NG",
           "business_date": "2026-10-14"}
    assert dispatcher.load_request(sqs_body(msg, wrap)) == ("ng", "2026-10-14", 1)


def test_dispatcher_requeued_load():
    msg = {"source": "coldstone-load-requeue", "country": "ky", "business_date": "2026-10-14", "attempt": 2}
    assert dispatcher.load_request(sqs_body(msg)) == ("ky", "2026-10-14", 2)


def test_dispatcher_ignores_other_messages():
    msg = {"source": "coldstone-pipeline", "stage": "extract", "status": "failed", "country": "ng"}
    assert dispatcher.load_request(sqs_body(msg)) is None


def test_dispatcher_rejects_unknown_country():
    msg = {"source": "coldstone-load-requeue", "country": "ke", "business_date": "2026-10-14"}
    with pytest.raises(ValueError):
        dispatcher.load_request(sqs_body(msg))


def test_dispatcher_and_load_function_names_agree():
    assert dispatcher.COUNTRIES == set(loader.SCHEMA_BASE) == set(notifier.COUNTRIES)


@pytest.mark.parametrize("job, country", [("coldstone-extract-ng", "ng"), ("coldstone-load-ky", "ky"),
                                          ("something-else", None)])
def test_notifier_country_of(job, country):
    assert notifier.country_of(job) == country


PIPELINE_TOPIC = "arn:aws:sns:us-east-1:655103423742:coldstone-pipeline-alerts"


def notify(monkeypatch, msg, subject="x"):
    """Runs the notifier on one SNS message; returns (Teams cards, emails as (topic, subject, text))"""
    teams, emails = [], []

    class FakeSNS:
        def publish(self, TopicArn, Subject, Message):
            emails.append((TopicArn, Subject, Message))

    monkeypatch.setattr(notifier, "post", teams.append)
    monkeypatch.setattr(notifier.boto3, "client", lambda service: FakeSNS())
    monkeypatch.delenv("OPS_TOPIC_ARN", raising=False)
    body = msg if isinstance(msg, str) else json.dumps(msg)
    notifier.lambda_handler({"Records": [{"Sns": {"Message": body, "Subject": subject,
                                                  "TopicArn": PIPELINE_TOPIC}}]}, None)
    return teams, emails


LOAD_SUCCESS = {"source": "coldstone-pipeline", "stage": "load", "status": "success", "country": "ng",
                "business_date": "2026-11-04", "kpis": {"orders": 12, "sales_minor": 500000, "sales_orders": 10}}


def test_notifier_sends_only_the_load_success_to_teams(monkeypatch):
    teams, emails = notify(monkeypatch, LOAD_SUCCESS)
    assert len(teams) == 1 and emails == []
    assert "loaded into Redshift and ready to use" in json.dumps(teams[0], ensure_ascii=False)


@pytest.mark.parametrize("msg, words", [
    (dict(LOAD_SUCCESS, status="failed", error="COPY failed: Cannot find default IAM role"), "Cannot find default IAM role"),
    (dict(LOAD_SUCCESS, status="queued", error="Redshift busy", next_attempt=2, max_attempts=3), "queued"),
    ({"source": "coldstone-pipeline", "stage": "extract", "status": "failed", "country": "ky",
      "business_date": "2026-11-04", "row_counts": {}, "problems": ["pull part 1 failed: Sandbox.Timedout"]},
     "Sandbox.Timedout"),
    ({"AlarmName": "coldstone-load-requests-stuck", "NewStateValue": "ALARM", "AlarmDescription": "stuck",
      "NewStateReason": "Threshold crossed", "StateChangeTime": "2026-11-05T08:37:00.000+0000"},
     "coldstone-load-requests-stuck"),
    ("plain text from somewhere", "plain text from somewhere"),
])
def test_notifier_emails_everything_else(monkeypatch, msg, words):
    teams, emails = notify(monkeypatch, msg)
    assert teams == [] and len(emails) == 1
    topic, subject, text = emails[0]
    assert topic == "arn:aws:sns:us-east-1:655103423742:coldstone-ops-alerts"
    assert words in text and "{" not in text.split("\n")[0], "a readable email, not JSON"
    assert subject.isascii() and 0 < len(subject) < 100, "SNS email subjects must be short ASCII"


@pytest.mark.parametrize("setting, cards", [(None, 0), ("off", 0), ("on", 1)])
def test_notifier_extract_success(monkeypatch, setting, cards):
    if setting:
        monkeypatch.setenv("EXTRACT_SUCCESS_CARDS", setting)
    else:
        monkeypatch.delenv("EXTRACT_SUCCESS_CARDS", raising=False)
    msg = {"source": "coldstone-pipeline", "stage": "extract", "status": "success", "country": "ky", "mode": "d-1",
           "business_date": "2026-11-04", "row_counts": {"orders": 9}, "problems": []}
    teams, emails = notify(monkeypatch, msg)
    assert len(teams) == cards and emails == []


def test_notifier_never_emails_its_own_topic(monkeypatch):
    monkeypatch.setenv("OPS_TOPIC_ARN", PIPELINE_TOPIC)
    monkeypatch.setattr(notifier, "post", lambda body: None)
    with pytest.raises(RuntimeError, match="own topic"):
        notifier.lambda_handler({"Records": [{"Sns": {"Message": "x", "Subject": "x", "TopicArn": PIPELINE_TOPIC}}]},
                                None)


def fake_columns():
    cols = {t: ["external_id", "name"] for t in loader.UPSERT_TABLES}
    cols.update({t: [fk, "name"] for t, (fk, _) in loader.CHILDREN.items()})
    return cols, {t: {c: "character varying" for c in cs} for t, cs in cols.items()}


def test_child_changes_follow_merge_order():
    cols, types = fake_columns()
    merge = loader.merge_sql("ng", cols, types)
    assert len(merge) <= loader.MAX_BATCH
    # ResultRows = the statement's position, so each count shows which statement it was read from
    changes = loader.child_changes({"SubStatements": [{"ResultRows": i} for i in range(len(merge))]})
    for table, c in changes.items():
        assert merge[c["removed"]].startswith("DELETE FROM") and f'."{table}"' in merge[c["removed"]]
        assert merge[c["new"]].startswith("INSERT INTO") and f'."{table}"' in merge[c["new"]]
    assert loader.child_changes({"SubStatements": [{"ResultRows": -1}]})["order_items"] == \
        {"new": 0, "updated": 0, "removed": 0}


def test_counts_sql_covers_every_table():
    sql = loader.counts_sql("ng")
    assert sql.count("UNION ALL") == len(loader.TABLES) - 1
    assert all(f"'{t}'" in sql for t in loader.TABLES)


def test_load_stages_every_table_and_reports_changes(monkeypatch):
    staged, published = [], []

    class FakeRedshift:
        def __init__(self, *_):
            pass

        def columns(self, cc):
            return fake_columns()

        def run(self, statements, label):
            if "-stage-" in label:
                staged.append(label.rsplit("-stage-", 1)[1])
            return {"SubStatements": [{"ResultRows": 1} for _ in statements]}

        def query(self, sql, label):
            if label.endswith("-counts"):
                return [[t, 5] for t in loader.TABLES]
            if label.endswith("-changes"):
                return [[t, "2", "1"] for t in loader.UPSERT_TABLES]
            raise RuntimeError("no KPIs in this test")

    monkeypatch.setattr(loader, "Redshift", FakeRedshift)
    monkeypatch.setattr(loader, "publish", lambda *a, **kw: published.append(kw))
    args = loader.load_settings(["--COUNTRY", "ng", "--S3_BUCKET", "b", "--REDSHIFT_WORKGROUP", "w"])
    loader.load(args, loader.datetime(2026, 10, 20))
    assert sorted(staged) == sorted(loader.TABLES)
    result = published[0]
    assert result["rows_checked"] == 5 * len(loader.TABLES)
    assert result["changes"] == {"new": 2 * len(loader.UPSERT_TABLES) + len(loader.CHILDREN),
                                 "updated": len(loader.UPSERT_TABLES), "removed": len(loader.CHILDREN)}


def test_load_card_shows_no_row_counts():
    msg = {"source": "coldstone-pipeline", "stage": "load", "status": "success", "country": "ky",
           "business_date": "2026-10-21", "rows_checked": 52632, "changes": {"new": 11816, "updated": 375, "removed": 0},
           "kpis": {"orders": 0}}
    text = json.dumps(notifier.load_card(msg))
    assert "Rows" not in text and "11,816" not in text and "52,632" not in text


# ---------- Lambda: split pulls, entry points, dispatcher, notifier, state machine ----------

def fake_api(module, monkeypatch, branches=5, hq_sees_all_staff=True):
    """Stands in for the Coldstone API: a few records per endpoint, every branch with its own menu, staff and wallet.
    As in the real API, the headquarters branch (b0) returns every branch's staff, unless hq_sees_all_staff=False."""
    recent = "2026-11-04T10:00:00.000Z"
    ids = [f"b{i}" for i in range(branches)]

    def members(bid):
        if bid == "b0" and hq_sees_all_staff:
            return [{"_id": f"m_{b}", "branch": b} for b in ids]
        return [{"_id": f"m_{bid}", "branch": bid}]

    def data(path, params):
        bid = params.get("branchId")
        return {
            "orders": {"orders": [{"_id": "o1", "createdAt": recent, "updatedAt": recent,
                                   "branch": {"_id": "b0", "name": "B0"},
                                   "products": [{"product": "p_b0", "price": 100, "quantity": 1}]}],
                       "meta": {"hasNextPage": False}},
            "customers": {"users": [{"_id": "u1", "createdAt": recent}], "meta": {"hasNextPage": False}},
            "activity-logs": {"activities": [], "meta": {"hasNextPage": False}},
            "discounts": {"discounts": []},
            "roles": {"roles": [{"_id": "r1", "name": "admin"}]},
            "branches": [{"_id": b, "name": f"Branch {b}", "isHeadquarters": b == "b0"} for b in ids],
            "members": members(bid),
            "menus": {"products": [{"_id": f"p_{bid}", "branch": bid, "price": 100}]},
            "modifiers": {"products": []},
            "categories": {"categories": [{"_id": f"c_{bid}", "name": "Cups"}]},
            "wallet/balance": {"_id": f"w_{bid}", "branch": bid, "balance": 5},
            "wallet/transactions": {"data": [{"_id": f"t_{bid}", "createdAt": recent, "amount": 5}],
                                    "meta": {"hasNextPage": False}},
        }[path]

    class Response:
        status_code, headers = 200, {}

        def __init__(self, body):
            self.body = body

        def json(self):
            return {"data": self.body}

        def raise_for_status(self):
            pass

    calls = []

    def get(url, headers=None, params=None, timeout=None):
        path = url[len(module.BASE) + 1:]
        calls.append(path)
        return Response(data(path, params or {}))

    monkeypatch.setattr(module.requests, "get", get)
    monkeypatch.setattr(module, "headers", lambda: {})
    monkeypatch.setattr(module, "DELAY", 0)
    monkeypatch.setattr(module, "_until", None)
    return calls


@pytest.mark.parametrize("hq_sees_all_staff, member_calls", [(True, 1), (False, 1 + 5)])
def test_staff_come_from_one_headquarters_call(hq_sees_all_staff, member_calls, monkeypatch):
    # One members call when headquarters returns everyone; otherwise every branch is asked, as before
    m = extract["ng"]
    calls = fake_api(m, monkeypatch, hq_sees_all_staff=hq_sees_all_staff)
    monkeypatch.setattr(m, "_raw", None)
    t = m.Tables()
    m.extract(t, m.datetime(2026, 11, 4, tzinfo=m.timezone.utc))
    assert calls.count("members") == member_calls
    assert sorted(t.rows["members"]) == [f"m_b{i}" for i in range(5)]
    assert len(t.rows["branch_members"]) == 5


@pytest.mark.parametrize("country", ["ng", "ky"])
@pytest.mark.parametrize("parts", [1, 2, 3])
@pytest.mark.parametrize("hq_sees_all_staff", [True, False])
def test_split_pull_rebuilds_the_same_day(country, parts, hq_sees_all_staff, tmp_path, monkeypatch):
    m = extract[country]
    calls = fake_api(m, monkeypatch, hq_sees_all_staff=hq_sees_all_staff)
    day = m.datetime(2026, 11, 4, tzinfo=m.timezone.utc)

    monkeypatch.setattr(m, "_raw", None)
    whole = m.Tables()
    m.extract(whole, day)
    single_pull_calls = len(calls)

    # The same day pulled in parts, recorded the way the Lambda runs record them
    calls.clear()
    branches = None
    for k in range(1, parts + 1):
        recorder = m.RawRecorder(str(tmp_path), day, "run1", "d-1", "" if k == 1 else f".part{k}")
        monkeypatch.setattr(m, "_raw", recorder)
        branches = m.extract(m.Tables(), day, k, parts, branches)
        recorder.close(True, part=k, parts=parts)
    assert len(calls) == single_pull_calls, "the parts together should make exactly the single pull's API calls"

    # --from-raw reads every part back as one pull, with no API calls
    run_dir = tmp_path / "raw" / "business_date=2026-11-04"
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert m.raw_parts(str(run_dir), manifest) == parts
    calls.clear()
    monkeypatch.setattr(m, "_raw", m.RawReplay(str(run_dir), parts))
    rebuilt = m.Tables()
    m.extract(rebuilt, day)
    assert calls == []
    assert rebuilt.rows == whole.rows
    assert len(whole.rows["menu_items"]) == 5 and len(whole.rows["wallets"]) == 5


def test_state_machine_sequence_through_main(tmp_path, monkeypatch):
    # The three Lambda runs the NG state machine makes (part 1, part 2, build from raw), through main(), with a
    # local folder standing in for S3
    import shutil
    m = extract["ng"]
    fake_api(m, monkeypatch)
    s3 = tmp_path / "s3"
    published = []

    def upload(files, bucket, prefix, layer):
        for rel, local in files.items():
            (s3 / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(local, s3 / rel)

    monkeypatch.setattr(m, "upload", upload)
    monkeypatch.setattr(m, "first_part_manifest",
                        lambda day, b, p: json.loads((s3 / f"raw/business_date={day:%Y-%m-%d}/manifest.json").read_text()))
    monkeypatch.setattr(m, "latest_raw_run", lambda day, root, b, p: str(s3 / f"raw/business_date={day:%Y-%m-%d}"))
    monkeypatch.setattr(m, "publish", lambda args, day, mode, status, t, problems: published.append((status, problems)))
    for event in [{"part": 1, "parts": 2}, {"part": 2, "parts": 2}, {"from_raw": True}]:
        monkeypatch.setattr(m, "WORK_DIR", str(tmp_path / "work"))  # a fresh /tmp each run, as on Lambda
        shutil.rmtree(tmp_path / "work", ignore_errors=True)
        monkeypatch.setattr(sys, "argv", ["x"] + entry.build_argv([], entry.EXTRACT_OPTIONS,
                                                                  dict(event, date="2026-11-04"), None) +
                            ["--S3_BUCKET", "bucket"])
        m.main()
    assert published == [("success", [])], "only the build step publishes, once"
    assert sorted(p.name for p in (s3 / "raw/business_date=2026-11-04").glob("manifest*")) == \
        ["manifest.json", "manifest.part2.json"]
    assert (s3 / "processed/menu_items/business_date=2026-11-04/menu_items.parquet").exists()
    assert len(json.loads((s3 / "raw/business_date=2026-11-04/manifest.json").read_text())["branches"]) == 5


@pytest.mark.parametrize("country", ["ng", "ky"])
def test_requests_are_paced_from_start_to_start(country, monkeypatch):
    # The API's response time counts towards the gap: a 0.5 s answer leaves only the rest of DELAY to wait
    m = extract[country]
    clock, sleeps = [100.0], []
    monkeypatch.setattr(m.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(m.time, "sleep", lambda s: (sleeps.append(round(s, 3)), clock.__setitem__(0, clock[0] + s)))
    monkeypatch.setattr(m, "_last_request", 0.0)
    m.pace()                       # first request: no wait
    clock[0] += 0.5                # the API takes 0.5 s to answer
    m.pace()                       # waits only the remaining 0.55 s
    clock[0] += 2.0                # a slow answer, longer than DELAY
    m.pace()                       # no wait
    assert sleeps == [round(m.DELAY - 0.5, 3)]
    assert 60 / m.DELAY < 60, "must stay under the API's 60 requests a minute"


def test_split_pull_rejects_a_part_from_another_run(tmp_path):
    m = extract["ng"]
    (tmp_path / "manifest.part2.json").write_text(json.dumps({"complete": True, "run_id": "older"}), encoding="utf-8")
    with pytest.raises(SystemExit, match="earlier run"):
        m.raw_parts(str(tmp_path), {"business_date": "2026-11-04", "run_id": "run1", "parts": 2})
    with pytest.raises(SystemExit, match="part 3 of 3"):
        m.raw_parts(str(tmp_path), {"business_date": "2026-11-04", "run_id": "older", "parts": 3})


class FakeContext:
    aws_request_id = "req-1"


def test_entry_passes_environment_and_event_to_the_extract(monkeypatch):
    for name in entry.EXTRACT_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:1:topic")
    event = {"date": "2026-11-04", "part": 2, "parts": 2, "from_raw": False, "full_history": None}
    argv = entry.build_argv(entry.EXTRACT_SETTINGS, entry.EXTRACT_OPTIONS, event, FakeContext())
    args = extract["ng"].load_settings(argv)
    assert (args.S3_BUCKET, args.date, args.part, args.parts, args.from_raw, args.JOB_RUN_ID) == \
        ("bucket", "2026-11-04", 2, 2, False, "req-1")
    assert args.S3_PREFIX == "coldstone_web_ng" and args.SNS_TOPIC_ARN.endswith(":topic")
    args = extract["ky"].load_settings(entry.build_argv(entry.EXTRACT_SETTINGS, entry.EXTRACT_OPTIONS,
                                                        {"from_raw": True}, None))
    assert args.from_raw and args.part is None


def test_entry_passes_environment_and_event_to_the_load(monkeypatch):
    for name in entry.LOAD_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    for name, value in {"COUNTRY": "ky", "S3_BUCKET": "bucket", "REDSHIFT_WORKGROUP": "wg"}.items():
        monkeypatch.setenv(name, value)
    args = loader.load_settings(entry.build_argv(entry.LOAD_SETTINGS, entry.LOAD_OPTIONS,
                                                 {"date": "2026-11-04", "attempt": 2}, FakeContext()))
    assert (args.COUNTRY, args.S3_PREFIX, args.date, args.ATTEMPT) == ("ky", "coldstone_web_ky", "2026-11-04", 2)


def raiser(error):
    def main():
        raise error
    return main


@pytest.mark.parametrize("raised, expected", [(SystemExit(1), "ReportedFailure"),
                                              (SystemExit("branches: got exactly 100"), "RuntimeError"),
                                              (ValueError("API down"), "ValueError")])
def test_entry_extract_errors(monkeypatch, tmp_path, raised, expected):
    # Only the data-check failure (exit 1) was published by the script; the state machine reports the rest
    monkeypatch.setenv("COUNTRY", "ng")
    monkeypatch.setattr(extract["ng"], "WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setattr(extract["ng"], "main", raiser(raised))
    with pytest.raises(Exception) as err:
        entry.extract_handler({}, FakeContext())
    assert type(err.value).__name__ == expected


@pytest.mark.parametrize("raised, expected", [(RuntimeError("merge failed"), "ReportedFailure"),
                                              (SystemExit("No extract found"), "ReportedFailure"),
                                              (SystemExit(2), "RuntimeError")])
def test_entry_load_errors(monkeypatch, raised, expected):
    monkeypatch.setattr(loader, "main", raiser(raised))
    with pytest.raises(Exception) as err:
        entry.load_handler({}, FakeContext())
    assert type(err.value).__name__ == expected


def test_dispatcher_invokes_the_country_load_lambda(monkeypatch):
    invoked = []

    class FakeLambda:
        def invoke(self, **kw):
            if kw["FunctionName"].endswith("-ky"):
                raise RuntimeError("throttled")
            invoked.append(kw)

    monkeypatch.setattr(dispatcher, "lambda_client", FakeLambda())
    ok = {"source": "coldstone-pipeline", "stage": "extract", "status": "success", "country": "ng",
          "business_date": "2026-11-04"}
    bad = dict(ok, country="ky")
    result = dispatcher.lambda_handler({"Records": [{"messageId": "1", "body": json.dumps(ok)},
                                                    {"messageId": "2", "body": json.dumps(bad)}]}, None)
    assert invoked[0]["FunctionName"] == "coldstone-load-ng" and invoked[0]["InvocationType"] == "Event"
    assert json.loads(invoked[0]["Payload"]) == {"date": "2026-11-04", "attempt": 1}
    assert result == {"batchItemFailures": [{"itemIdentifier": "2"}]}


@pytest.mark.parametrize("error_type, sent", [("ReportedFailure", 0), (None, 1)])
def test_notifier_lambda_failure_destination(monkeypatch, error_type, sent):
    # A load timeout is emailed; a failure the load already reported isn't sent twice
    payload = {"errorMessage": "Task timed out after 900.00 seconds"}
    if error_type:
        payload["errorType"] = error_type
    msg = {"version": "1.0", "timestamp": "2026-11-05T04:30:00.000Z",
           "requestContext": {"functionArn": "arn:aws:lambda:us-east-1:1:function:coldstone-load-ng:$LATEST",
                              "condition": "RetriesExhausted"},
           "requestPayload": {"date": "2026-11-04", "attempt": 1}, "responsePayload": payload}
    teams, emails = notify(monkeypatch, msg, subject=None)
    assert teams == [] and len(emails) == sent
    if sent:
        text = emails[0][2]
        assert "coldstone-load-ng" in text and "2026-11-04" in text and "timed out" in text


def test_state_machine_matches_template():
    definition = (ROOT / "statemachine/extract.asl.json").read_text(encoding="utf-8")
    states = json.loads(definition)["States"]
    placeholders = set(re.findall(r"\$\{(\w+)\}", definition))
    template = (ROOT / "template.yaml").read_text(encoding="utf-8")
    blocks = re.findall(r"DefinitionSubstitutions:\n((?:        \w+: .*\n)+)", template)
    assert len(blocks) == 2, "expected the NG and KY state machines"
    for block in blocks:
        assert set(re.findall(r"^        (\w+):", block, re.M)) == placeholders
    targets = {c["Next"] for s in states.values() for c in s.get("Catch", [])} | \
              {s["Next"] for s in states.values() if "Next" in s}
    assert targets <= set(states)
    # the extract Lambdas raise ReportedFailure when they already published the failure
    assert all(s["Catch"][0]["ErrorEquals"] == [entry.ReportedFailure.__name__]
               for s in states.values() if "Catch" in s)
