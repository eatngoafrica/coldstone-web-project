"""Start Coldstone Redshift loads from the SQS load queue (coldstone-load-queue).

Two kinds of message arrive (raw message delivery, so the body is the JSON itself):
  * an extract's success, forwarded by an SNS subscription filtered to stage=extract, status=success:
      {"source": "coldstone-pipeline", "stage": "extract", "status": "success", "country": "ng",
       "business_date": "2026-10-14", ...}                                   -> attempt 1
  * a load that hit temporary Redshift errors and re-queued itself with a 10-minute delay (load_redshift.py):
      {"source": "coldstone-load-requeue", "country": "ng", "business_date": "2026-10-14", "attempt": 2}

Each one invokes the Lambda coldstone-load-<country> asynchronously with {"date": ..., "attempt": ...}. Each load
Lambda has a concurrency of 1, so a load that arrives while the country's previous one is still running waits in
Lambda's own queue (up to 6 hours) instead of clashing with it on the staging tables. If the invoke itself fails, the
message is left on the queue and comes back after the queue's visibility timeout (10 minutes); after the queue's
maximum receives it moves to the dead-letter queue, which has its own alarm.

Environment variables:
    LOAD_FUNCTION_PREFIX   load Lambda name before the country code (default: coldstone-load-)
"""
import json, os
import boto3

lambda_client = boto3.client("lambda")
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
            function = f"{os.environ.get('LOAD_FUNCTION_PREFIX', 'coldstone-load-')}{country}"
            lambda_client.invoke(FunctionName=function, InvocationType="Event",
                                 Payload=json.dumps({"date": day, "attempt": attempt}).encode())
            print(f"Started {function} for {day}, attempt {attempt}")
        except Exception as err:
            print(f"{record['messageId']}: could not start the load ({err}); left on the queue")
            failed.append({"itemIdentifier": record["messageId"]})
    # Only the failed messages go back on the queue (the trigger has "Report batch item failures" on)
    return {"batchItemFailures": failed}
