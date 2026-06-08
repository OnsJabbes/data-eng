"""
SQS event queue for the ETL pipeline.

Publishes a message after each ETL run so downstream consumers can react.
Introduces SQS as a new cloud resource for the pipeline (in addition to
S3, Glue, and RDS).
"""
import logging

import boto3

logger = logging.getLogger("sqs_events")

EVENTS_QUEUE = "etl-events"
DLQ_QUEUE = "etl-events-dlq"


def ensure_dead_letter_queue(region: str = "us-east-1") -> str:
    """Create the SQS dead-letter queue for failed ETL events; return its URL."""
    sqs = boto3.client("sqs", region_name=region)
    logger.info("Ensuring SQS dead-letter queue %s", DLQ_QUEUE)
    resp = sqs.create_queue(QueueName=DLQ_QUEUE)
    return resp["QueueUrl"]


def ensure_events_queue(region: str = "us-east-1") -> str:
    """Create the ETL events SQS queue if it does not already exist; return its URL."""
    sqs = boto3.client("sqs", region_name=region)
    logger.info("Ensuring SQS queue %s", EVENTS_QUEUE)
    resp = sqs.create_queue(QueueName=EVENTS_QUEUE)
    return resp["QueueUrl"]


def publish_event(queue_url: str, job_name: str, status: str) -> None:
    """Send one ETL completion event to the SQS queue."""
    sqs = boto3.client("sqs")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=f'{{"job": "{job_name}", "status": "{status}"}}',
    )
    logger.info("Published SQS event for job %s (%s)", job_name, status)
