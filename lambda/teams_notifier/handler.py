"""Post Coldstone pipeline notifications from SNS to a Microsoft Teams channel.

Subscribed to the pipeline's SNS topic. Handles these kinds of message:
  * results published by the Redshift load jobs (load_redshift.py, "stage": "load"): "loaded and ready" with
    basic KPIs for the day, or the load's error
  * results published by the extract jobs (extract_prod_ng.py / extract_prod_ky.py), e.g.
      {"source": "coldstone-pipeline", "country": "ng", "mode": "d-1", "status": "success",
       "business_date": "2026-10-13", "finished_at": "2026-10-14T04:28:00Z", "job_run_id": "jr_...",
       "row_counts": {"orders": 109, ...}, "problems": []}
  * Glue events forwarded by an EventBridge rule: job FAILED / TIMEOUT / ERROR / STOPPED, and the
    "running late" notice from the job's delay notification threshold
  * CloudWatch alarm notifications

Environment variables:
    TEAMS_WEBHOOK_PARAM    SSM Parameter Store (SecureString) name holding the Teams Workflows webhook URL
    EXTRACT_SUCCESS_CARDS  on (default) / off. Off = one report per country per day: successful extracts post
                           nothing and only the final Redshift "loaded and ready" card appears. Failures always post.
A failed post raises, so Lambda's async retries try again.
"""
import json, os, urllib.request
from datetime import datetime, timedelta, timezone
import boto3

COUNTRIES = {"ng": ("Nigeria", "Lagos", timezone(timedelta(hours=1))),
             "ky": ("Kenya", "Nairobi", timezone(timedelta(hours=3)))}
HEADLINE_TABLES = ["orders", "order_items", "customers", "activity_logs", "wallet_transactions"]
# Pulled in full on every run, so 0 rows means something broke. The other tables can legitimately be
# empty on a quiet day (e.g. Kenya often has no orders, and neither country uses discount combos).
NEVER_EMPTY = ["branches", "members", "roles", "menu_items", "menu_categories", "wallets"]
_webhook_url = None


def webhook_url():
    global _webhook_url
    if _webhook_url is None:
        ssm = boto3.client("ssm")
        _webhook_url = ssm.get_parameter(Name=os.environ["TEAMS_WEBHOOK_PARAM"], WithDecryption=True)["Parameter"]["Value"]
    return _webhook_url


def local_time(iso, tz=COUNTRIES["ng"][2]):
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(tz).strftime("%a %d %b, %I:%M %p")
    except (AttributeError, ValueError):
        return iso


def country_of(job_name):
    # Job names end in -ng / -ky (coldstone-extract-ng, coldstone-load-ky ...)
    return next((cc for cc in COUNTRIES if (job_name or "").lower().endswith(f"-{cc}")), None)


def title(text, ok):
    return {"type": "TextBlock", "size": "Large", "weight": "Bolder", "wrap": True,
            "color": "Good" if ok else "Attention", "text": text}


def pipeline_card(msg):
    ok = msg.get("status") == "success"
    cc = (msg.get("country") or "ng").lower()
    name, city, tz = COUNTRIES.get(cc, (cc.upper(), "UTC", timezone.utc))
    day = msg.get("business_date", "?")
    full = msg.get("mode") == "full_history"
    what = f"Coldstone {name} {'full history to' if full else 'data for'} {day}"
    facts = [{"title": "Business date", "value": f"{day} ({'full history' if full else 'D-1'}, {city} time)"},
             {"title": "Finished", "value": f"{local_time(msg.get('finished_at'), tz)} ({city})"}]
    counts = msg.get("row_counts") or {}
    facts += [{"title": t, "value": f"{counts[t]:,}"} for t in HEADLINE_TABLES if t in counts]
    empty = [t for t in NEVER_EMPTY if t in counts and not counts[t]]
    if empty:
        facts.append({"title": "⚠️ Loaded 0 rows", "value": ", ".join(empty)})
    body = [title(f"✅ {what} extracted to S3" if ok else f"❌ {what} failed", ok), {"type": "FactSet", "facts": facts}]
    if ok:
        body.append({"type": "TextBlock", "wrap": True, "isSubtle": True,
                     "text": "Step 1 of 2 done. Loading into Redshift next; a second card confirms when it's ready to use."})
    problems = msg.get("problems") or []
    if problems:
        body.append({"type": "TextBlock", "weight": "Bolder", "text": "Problems", "wrap": True})
        body += [{"type": "TextBlock", "text": f"• {p}", "wrap": True, "spacing": "None"} for p in problems[:10]]
        if len(problems) > 10:
            body.append({"type": "TextBlock", "text": f"… and {len(problems) - 10} more (see the job's logs)",
                         "wrap": True, "isSubtle": True})
    if not ok:
        body.append({"type": "TextBlock", "wrap": True, "isSubtle": True,
                     "text": f"Nothing was uploaded to processed. Fix the cause, then run coldstone-extract-{cc} "
                             f"with --date {day}. Raw responses were saved for investigation."})
    return body


def money(minor, cc):
    # Amounts are stored in minor units (kobo / cents); shown in each country's own currency, never converted
    symbol = {"ng": "₦", "ky": "KES "}.get(cc, "")
    return f"{symbol}{(minor or 0) / 100:,.2f}"


def load_card(msg):
    # Row counts (rows_checked, changes) stay in the message and the job log for troubleshooting, but aren't shown:
    # they count every table, not sales, and confused readers next to the KPIs.
    ok = msg.get("status") == "success"
    cc = (msg.get("country") or "ng").lower()
    name, city, tz = COUNTRIES.get(cc, (cc.upper(), "UTC", timezone.utc))
    day, schema = msg.get("business_date", "?"), msg.get("schema", f"analytics_{cc}")
    finished = f"{local_time(msg.get('finished_at'), tz)} ({city})"
    if msg.get("status") == "queued":
        return [title(f"⏳ Coldstone {name} Redshift load for {day} is queued: Redshift busy", False),
                {"type": "FactSet", "facts": [
                    {"title": "Business date", "value": day},
                    {"title": "Queued at", "value": finished},
                    {"title": "Next attempt", "value": f"{msg.get('next_attempt')} of {msg.get('max_attempts')}, "
                                                       f"in about {msg.get('retry_in_minutes', 10)} minutes"}]},
                {"type": "TextBlock", "wrap": True, "text": msg.get("error") or ""},
                {"type": "TextBlock", "wrap": True, "isSubtle": True,
                 "text": f"No action needed: the load retries itself. {schema} is unchanged until a load succeeds."}]
    if not ok:
        attempts = (f"{msg.get('attempt')} of {msg.get('max_attempts')}" if msg.get("attempt") else None)
        return [title(f"❌ Coldstone {name} Redshift load for {day} failed", False),
                {"type": "FactSet", "facts": [{"title": "Business date", "value": day},
                                              {"title": "Failed at", "value": finished}] +
                                             ([{"title": "Attempt", "value": attempts}] if attempts else [])},
                {"type": "TextBlock", "wrap": True, "text": msg.get("error") or "No error message"},
                {"type": "TextBlock", "wrap": True, "isSubtle": True,
                 "text": f"{schema} was not changed (the load commits all or nothing). Fix the cause, then run "
                         f"coldstone-load-{cc} with --date {day}."}]
    body = [title(f"✅ Coldstone {name} data for {day} is loaded into Redshift and ready to use", True),
            {"type": "TextBlock", "wrap": True,
             "text": f"All data was successfully loaded into Redshift ({schema}) and is ready to be used."},
            {"type": "FactSet", "facts": [{"title": "Business date", "value": f"{day} ({city} time)"},
                                          {"title": "Loaded at", "value": finished}]}]
    k = msg.get("kpis") or {}
    body.append({"type": "TextBlock", "weight": "Bolder", "wrap": True, "spacing": "Large",
                 "text": f"📊 Basic KPIs for {day}"})
    body.append({"type": "TextBlock", "wrap": True, "isSubtle": True, "spacing": "None",
                 "text": "A quick snapshot to confirm the day looks right. These are basic KPIs only, not the full report."})
    if k.get("error"):
        body.append({"type": "TextBlock", "wrap": True, "text": k["error"]})
        return body
    # Sales and orders include call-centre (cca) orders still pending payment; that part is also shown on its own.
    # Kenya has no call-centre channel, so its card keeps the simpler labels unless such orders ever appear.
    pending_n, pending_amount = k.get("cca_pending_orders", 0), k.get("cca_pending_minor", 0)
    with_cca = cc == "ng" or pending_n
    facts = [{"title": "Orders", "value": f"{k.get('orders', 0):,}" + (f" (of which {pending_n:,} CCA pending)" if with_cca else "")},
             {"title": "Sales (paid + CCA pending)" if with_cca else "Sales (paid)", "value": money(k.get("sales_minor"), cc)}]
    if with_cca:
        facts.append({"title": "  of which CCA pending", "value": money(pending_amount, cc)})
    facts += [{"title": "Average order value", "value": money(k.get("avg_order_value_minor"), cc)},
              {"title": "Delivered / rejected", "value": f"{k.get('delivered', 0):,} / {k.get('rejected', 0):,}"},
              {"title": "Branches with orders", "value": f"{k.get('active_branches', 0):,}"},
              {"title": "New customers", "value": f"{k.get('new_customers', 0):,}"}]
    body.append({"type": "FactSet", "facts": facts})
    if with_cca:
        body.append({"type": "TextBlock", "wrap": True, "isSubtle": True, "spacing": "None",
                     "text": "CCA pending = call-centre orders not yet marked paid in the platform. They are included in "
                             "sales, the average and the top branches."})
    top = k.get("top_branches") or []
    if top:
        body.append({"type": "TextBlock", "weight": "Bolder", "wrap": True, "text": "Top branches by sales"})
        body.append({"type": "FactSet", "facts": [
            {"title": f"{i}. {b['branch']}",
             "value": f"{money(b['sales_minor'], cc)} ({b['orders']} order{'' if b['orders'] == 1 else 's'})"}
            for i, b in enumerate(top, 1)]})
    elif not k.get("orders"):
        body.append({"type": "TextBlock", "wrap": True, "isSubtle": True, "text": "No orders were placed on this day."})
    return body


def glue_card(event):
    d = event.get("detail") or {}
    job, state, run = d.get("jobName", "?"), d.get("state", "?"), d.get("jobRunId", "")
    cc = country_of(job)
    name, city, tz = COUNTRIES.get(cc, ("", "Lagos", COUNTRIES["ng"][2]))
    late = event.get("detail-type") == "Glue Job Run Status"
    text = (f"⏳ {job} is running late" if late else f"🚨 {job} {state.lower()}")
    facts = [{"title": "Job", "value": job}, {"title": "State", "value": state},
             {"title": "When", "value": f"{local_time(event.get('time'), tz)} ({city})"}]
    if late:
        minutes = (d.get("notificationCondition") or {}).get("NotifyDelayAfter")
        facts.append({"title": "Running for", "value": f"over {minutes} minutes" if minutes else "longer than expected"})
    if run:
        facts.append({"title": "Run ID", "value": run})
    body = [title(text, False), {"type": "FactSet", "facts": facts}]
    if d.get("message"):
        body.append({"type": "TextBlock", "wrap": True, "text": d["message"]})
    body.append({"type": "TextBlock", "wrap": True, "isSubtle": True,
                 "text": "Glue console → ETL jobs → the job → Runs → this run → Error logs." if not late else
                         "Usually a slow or rate-limited API. If it hits the 60-minute timeout a failure card follows."})
    return body


def alarm_card(msg):
    firing = msg.get("NewStateValue") == "ALARM"
    return [title(f"{'🚨' if firing else '✅'} {msg.get('AlarmName', 'Coldstone alarm')}{'' if firing else ' resolved'}",
                  not firing),
            {"type": "TextBlock", "wrap": True, "text": msg.get("AlarmDescription") or ""},
            {"type": "FactSet", "facts": [
                {"title": "Reason", "value": msg.get("NewStateReason", "")},
                {"title": "When", "value": f"{local_time(msg.get('StateChangeTime'))} (Lagos)"}]}]


def text_card(subject, text):
    return [{"type": "TextBlock", "weight": "Bolder", "wrap": True, "text": subject or "Coldstone pipeline"},
            {"type": "TextBlock", "wrap": True, "text": text}]


def post(body):
    payload = {"type": "message", "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard", "version": "1.4", "body": body}}]}
    req = urllib.request.Request(webhook_url(), data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        if r.status >= 300:
            raise RuntimeError(f"Teams webhook returned HTTP {r.status}")


def lambda_handler(event, context):
    for record in event.get("Records", []):
        sns = record["Sns"]
        try:
            msg = json.loads(sns["Message"])
        except (TypeError, ValueError):
            msg = None
        if isinstance(msg, dict) and msg.get("source") == "coldstone-pipeline" and msg.get("stage") == "load":
            post(load_card(msg))
        elif isinstance(msg, dict) and msg.get("source") == "coldstone-pipeline":
            # One report per country per day: once the Redshift load jobs run, set EXTRACT_SUCCESS_CARDS=off so
            # a good extract stays quiet and only the final "loaded and ready" card is posted. Failures always post.
            if msg.get("status") == "success" and os.environ.get("EXTRACT_SUCCESS_CARDS", "on").lower() == "off":
                print(f"Extract success for {msg.get('country')} {msg.get('business_date')}: card skipped")
                continue
            post(pipeline_card(msg))
        elif isinstance(msg, dict) and msg.get("source") == "aws.glue":
            post(glue_card(msg))
        elif isinstance(msg, dict) and "AlarmName" in msg:
            post(alarm_card(msg))
        else:
            post(text_card(sns.get("Subject"), sns["Message"]))
