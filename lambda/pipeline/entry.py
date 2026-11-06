"""Lambda entry points for the Coldstone pipeline scripts (extract_prod_ng.py, extract_prod_ky.py, load_redshift.py).

The scripts take their settings as --NAME value arguments, so they still run by hand exactly as before. On Lambda
the arguments are built here from the function's environment variables (fixed settings, set in template.yaml) and
the invocation event (per run settings, all optional):

  extract_handler  {"date": "YYYY-MM-DD", "full_history": true, "from_raw": true, "part": 1, "parts": 2}
                   sent by the state machine coldstone-extract-<cc>; COUNTRY picks the script
  load_handler     {"date": "YYYY-MM-DD", "attempt": 2}
                   sent by the load dispatcher, or by hand to load a date

A failure the script has already published to SNS (so its Teams card is on the way) is raised as ReportedFailure.
The state machine and the load's failure destination skip that error type, so one problem posts one card.
"""
import importlib, os, shutil, sys

EXTRACT_SETTINGS = ["S3_BUCKET", "S3_PREFIX", "COLDSTONE_KEY_PARAM", "SNS_TOPIC_ARN"]
EXTRACT_OPTIONS = {"date": "--date", "full_history": "--full-history", "from_raw": "--from-raw",
                   "part": "--part", "parts": "--parts"}
LOAD_SETTINGS = ["COUNTRY", "S3_BUCKET", "S3_PREFIX", "REDSHIFT_WORKGROUP", "REDSHIFT_DATABASE", "SCHEMA_BASE",
                 "COPY_ROLE_ARN", "SNS_TOPIC_ARN", "LOAD_QUEUE_URL"]
LOAD_OPTIONS = {"date": "--date", "attempt": "--ATTEMPT"}


class ReportedFailure(Exception):
    """The run failed and the script already published the failure to SNS."""


def build_argv(settings, options, event, context):
    argv = []
    for name in settings:
        if os.environ.get(name):
            argv += [f"--{name}", os.environ[name]]
    event = event if isinstance(event, dict) else {}
    for key, flag in options.items():
        value = event.get(key)
        if value is None or value is False or value == "":
            continue
        argv += [flag, str(value).lower() if isinstance(value, bool) else str(value)]
    if context is not None:
        argv += ["--JOB_RUN_ID", context.aws_request_id]
    return argv


def run(module, argv):
    saved = sys.argv
    sys.argv = [module.__name__] + argv
    try:
        module.main()
    finally:
        sys.argv = saved


def extract_handler(event, context):
    module = importlib.import_module(f"extract_prod_{os.environ['COUNTRY'].lower()}")
    shutil.rmtree(module.WORK_DIR, ignore_errors=True)  # /tmp survives between runs of a warm Lambda
    try:
        run(module, build_argv(EXTRACT_SETTINGS, EXTRACT_OPTIONS, event, context))
    except SystemExit as err:
        if err.code in (None, 0):
            return {"status": "ok"}
        if err.code == 1:  # the data checks failed; main() published the problems before exiting
            raise ReportedFailure("data quality checks failed (problems published to SNS); see this run's log") from None
        raise RuntimeError(str(err.code)) from None  # the script's own stop messages
    return {"status": "ok"}


def load_handler(event, context):
    module = importlib.import_module("load_redshift")
    try:
        run(module, build_argv(LOAD_SETTINGS, LOAD_OPTIONS, event, context))
    except SystemExit as err:
        if err.code in (None, 0):
            return {"status": "ok"}
        if err.code == 2:  # argparse: a missing setting, raised before the load starts, so nothing was published
            raise RuntimeError("bad settings for load_redshift.py; see this run's log") from None
        raise ReportedFailure(str(err.code)[:1000]) from None
    except Exception as err:
        # load_redshift.main() publishes every failure of the load itself before raising it
        raise ReportedFailure(f"{err.__class__.__name__}: {err}"[:1000]) from None
    return {"status": "ok"}
