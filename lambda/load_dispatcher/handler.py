"""Start Coldstone Redshift loads from the SQS load queue (coldstone-load-queue).

Two kinds of message arrive (raw message delivery, so the body is the JSON itself):
  * an extract job's success, forwarded by an SNS subscription filtered to stage=extract, status=success:
      {"source": "coldstone-pipeline", "stage": "extract", "status": "success", "country": "ng",
       "business_date": "2026-10-14", ...}                                   -> attempt 1
  * a load that hit temporary Redshift errors and re-queued itself with a 10-minute delay (load_redshift.py):
      {"source": "coldstone-load-requeue", "country": "ng", "business_date": "2026-10-14", "attempt": 2}

Each one starts the Glue job coldstone-load-<country> with --date and --ATTEMPT. If that job is already running
(it allows one run at a time) or Glue can't start it right now, the message is left on the queue and comes back
after the queue's visibility timeout (10 minutes). After the queue's maximum receives it moves to the dead-letter
queue, which has its own alarm.

Environment variables:
    LOAD_JOB_PREFIX   Glue job name before the country code (default: coldstone-load-)
"""
import json, os
import boto3

glue = boto3.client("glue")
COUNTRIES = {"ng", "ky"}


def load_request(body):
    msg = json.loads(body)
    if isinstance(msg, dict) and "Message" in msg and "TopicArn" in msg:
        msg = json.loads(msg["Message"])  # in case raw message delivery was left off on the subscription
    country, day = (msg.get("country") or "").lower(), msg.get("business_date")
    if msg.get("source") == "coldstone-pipeline" and msg.get("stage") == "extract" and msg.get("status") == "success":
        attempt = 1
    elif msg.get("source") == "coldstone-load-requeue":
        attempt = int(msg.get("attempt") or 1)
    else:
        return None
    if country not in COUNTRIES or not day:
        raise ValueError(f"load request without a valid country / business_date: {msg}")
    return country, day, attempt


def lambda_handler(event, context):
    failed = []
    for record in event.get("Records", []):
        try:
            request = load_request(record["body"])
            if request is None:
                print(f"Ignored a message that isn't a load request: {record['body'][:300]}")
                continue
            country, day, attempt = request
            job = f"{os.environ.get('LOAD_JOB_PREFIX', 'coldstone-load-')}{country}"
            run = glue.start_job_run(JobName=job, Arguments={"--date": day, "--ATTEMPT": str(attempt)})
            print(f"Started {job} for {day}, attempt {attempt}: {run['JobRunId']}")
        except glue.exceptions.ConcurrentRunsExceededException:
            # A load for this country is still running: leave the request queued and try again later
            print(f"{record['messageId']}: load already running, left on the queue")
            failed.append({"itemIdentifier": record["messageId"]})
        except Exception as err:
            print(f"{record['messageId']}: could not start the load ({err}); left on the queue")
            failed.append({"itemIdentifier": record["messageId"]})
    # Only the failed messages go back on the queue (the trigger has "Report batch item failures" on)
    return {"batchItemFailures": failed}
