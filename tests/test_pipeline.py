"""CI checks that the pipeline's pieces still agree with each other, without touching AWS.

  * each extract script's Parquet schemas match its country's Redshift DDL (same tables, same columns in the
    same order, compatible types), so COPY won't fail
  * the NG and KY extracts write identical table layouts
  * the load job's table lists cover exactly the extract's tables, and its schema names exist in the DDL
  * the load dispatcher and Teams notifier Lambdas parse the messages the pipeline sends
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


def test_dispatcher_and_glue_job_names_agree():
    assert dispatcher.COUNTRIES == set(loader.SCHEMA_BASE) == set(notifier.COUNTRIES)


@pytest.mark.parametrize("job, country", [("coldstone-extract-ng", "ng"), ("coldstone-load-ky", "ky"),
                                          ("something-else", None)])
def test_notifier_country_of(job, country):
    assert notifier.country_of(job) == country


def test_notifier_skips_extract_success_when_off(monkeypatch):
    posted = []
    monkeypatch.setattr(notifier, "post", posted.append)
    monkeypatch.setenv("EXTRACT_SUCCESS_CARDS", "off")
    msg = {"source": "coldstone-pipeline", "stage": "extract", "status": "success", "country": "ng",
           "business_date": "2026-10-14", "row_counts": {}, "problems": []}
    notifier.lambda_handler({"Records": [{"Sns": {"Message": json.dumps(msg), "Subject": "x"}}]}, None)
    assert posted == []


@pytest.mark.parametrize("msg", [
    {"source": "coldstone-pipeline", "stage": "extract", "status": "success", "country": "ky", "mode": "d-1",
     "business_date": "2026-10-14", "finished_at": "2026-10-15T04:28:00Z", "job_run_id": "jr_1",
     "row_counts": {"orders": 109}, "problems": []},
    {"source": "coldstone-pipeline", "stage": "extract", "status": "failed", "country": "ng",
     "business_date": "2026-10-14", "row_counts": {}, "problems": ["orders: API error"]},
])
def test_notifier_builds_extract_cards(monkeypatch, msg):
    posted = []
    monkeypatch.setattr(notifier, "post", posted.append)
    monkeypatch.setenv("EXTRACT_SUCCESS_CARDS", "on")
    notifier.lambda_handler({"Records": [{"Sns": {"Message": json.dumps(msg), "Subject": "x"}}]}, None)
    assert len(posted) == 1
    json.dumps(posted[0])  # the card must be valid JSON for Teams


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
