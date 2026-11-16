output "processing_queue_url" {
  description = "Primary SQS queue for pipeline ingestion"
  value       = aws_sqs_queue.pipeline_queue.url
}

output "dlq_url" {
  description = "Dead Letter Queue URL for failed events"
  value       = aws_sqs_queue.pipeline_dlq.url
}

output "dynamodb_table_name" {
  description = "Processed events DynamoDB table name"
  value       = aws_dynamodb_table.events_table.name
}

output "data_lake_bucket" {
  description = "S3 data lake archival bucket name"
  value       = aws_s3_bucket.data_lake.id
}
