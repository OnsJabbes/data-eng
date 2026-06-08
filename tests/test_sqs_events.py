import unittest
from unittest.mock import MagicMock, patch

from src.jobs.sqs_events import (
    DLQ_QUEUE,
    EVENTS_QUEUE,
    ensure_dead_letter_queue,
    ensure_events_queue,
    publish_event,
)


class TestSqsEvents(unittest.TestCase):
    @patch("boto3.client")
    def test_ensure_events_queue_returns_url(self, mock_boto):
        mock_sqs = MagicMock()
        mock_sqs.create_queue.return_value = {"QueueUrl": "https://sqs/etl-events"}
        mock_boto.return_value = mock_sqs

        url = ensure_events_queue(region="us-east-1")

        self.assertEqual(url, "https://sqs/etl-events")
        mock_sqs.create_queue.assert_called_once_with(QueueName=EVENTS_QUEUE)

    @patch("boto3.client")
    def test_ensure_dead_letter_queue_returns_url(self, mock_boto):
        mock_sqs = MagicMock()
        mock_sqs.create_queue.return_value = {"QueueUrl": "https://sqs/etl-events-dlq"}
        mock_boto.return_value = mock_sqs

        url = ensure_dead_letter_queue()

        self.assertEqual(url, "https://sqs/etl-events-dlq")
        mock_sqs.create_queue.assert_called_once_with(QueueName=DLQ_QUEUE)

    @patch("boto3.client")
    def test_publish_event_sends_message(self, mock_boto):
        mock_sqs = MagicMock()
        mock_boto.return_value = mock_sqs

        publish_event("https://sqs/etl-events", "etl_sales", "success")

        mock_sqs.send_message.assert_called_once()
        kwargs = mock_sqs.send_message.call_args.kwargs
        self.assertEqual(kwargs["QueueUrl"], "https://sqs/etl-events")
        self.assertIn("etl_sales", kwargs["MessageBody"])
        self.assertIn("success", kwargs["MessageBody"])


if __name__ == "__main__":
    unittest.main()
