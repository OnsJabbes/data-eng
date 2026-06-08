"""
DynamoDB metadata store for ETL pipeline runs.

Records each ETL execution (run id, job name, status, row count, timestamp)
into a DynamoDB table so pipeline runs can be audited. This introduces
DynamoDB as a new cloud resource for the pipeline (in addition to S3, Glue,
and RDS).
"""
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger("dynamodb_metadata")

METADATA_TABLE = "etl_run_metadata"


def ensure_metadata_table(region: str = "us-east-1") -> None:
    """Create the ETL run-metadata DynamoDB table if it does not already exist."""
    dynamodb = boto3.client("dynamodb", region_name=region)
    existing = dynamodb.list_tables().get("TableNames", [])
    if METADATA_TABLE in existing:
        logger.info("Metadata table %s already exists", METADATA_TABLE)
        return

    logger.info("Creating DynamoDB table %s", METADATA_TABLE)
    dynamodb.create_table(
        TableName=METADATA_TABLE,
        KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "run_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


def record_run(run_id: str, job_name: str, status: str, row_count: int) -> None:
    """Write one ETL run record to the metadata table."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(METADATA_TABLE)
    table.put_item(
        Item={
            "run_id": run_id,
            "job_name": job_name,
            "status": status,
            "row_count": row_count,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    logger.info("Recorded run %s for job %s", run_id, job_name)
