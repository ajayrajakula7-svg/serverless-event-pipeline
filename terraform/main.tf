terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "Serverless-Event-Driven-Pipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# S3 DATA LAKE (RAW ARCHIVE & PARQUET STORE)
# ---------------------------------------------------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-data-lake-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake_lifecycle" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "archive-to-glacier"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# DYNAMODB NO-SQL EVENT STORE
# ---------------------------------------------------------------------------------------------------------------------
resource "aws_dynamodb_table" "events_table" {
  name         = "${var.project_name}-events-${var.environment}"
  billing_mode = "PAY_PER_REQUEST" # Serverless on-demand scaling
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "correlation_id"
    type = "S"
  }

  global_secondary_index {
    name            = "CorrelationIdIndex"
    hash_key        = "correlation_id"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# SQS QUEUES & DEAD LETTER QUEUE (DLQ)
# ---------------------------------------------------------------------------------------------------------------------
resource "aws_sqs_queue" "pipeline_dlq" {
  name                      = "${var.project_name}-processing-dlq-${var.environment}"
  message_retention_seconds = 1209600 # 14 days retention for inspection
}

resource "aws_sqs_queue" "pipeline_queue" {
  name                       = "${var.project_name}-processing-queue-${var.environment}"
  visibility_timeout_seconds = 300 # Must be >= Lambda execution timeout
  message_retention_seconds  = 345600 # 4 days

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.pipeline_dlq.arn
    maxReceiveCount     = 3 # Re-try up to 3 times before moving to DLQ
  })
}

# ---------------------------------------------------------------------------------------------------------------------
# IAM ROLE & EXECUTION POLICIES
# ---------------------------------------------------------------------------------------------------------------------
resource "aws_iam_role" "lambda_exec" {
  name = "${var.project_name}-lambda-exec-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:SendMessage"
        ]
        Resource = [aws_sqs_queue.pipeline_queue.arn, aws_sqs_queue.pipeline_dlq.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query"
        ]
        Resource = [aws_dynamodb_table.events_table.arn, "${aws_dynamodb_table.events_table.arn}/index/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "${aws_s3_bucket.data_lake.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# ---------------------------------------------------------------------------------------------------------------------
# LAMBDA PROCESSOR FUNCTION & SQS EVENT SOURCE MAPPING
# ---------------------------------------------------------------------------------------------------------------------
resource "aws_lambda_function" "event_processor" {
  function_name = "${var.project_name}-worker-${var.environment}"
  role          = aws_iam_role.lambda_exec.arn
  runtime       = "python3.11"
  handler       = "handlers.process_handler.lambda_handler"
  timeout       = 60
  memory_size   = 512

  filename         = "${path.module}/build/placeholder.zip"
  source_code_hash = filebase64sha256("${path.module}/build/placeholder.zip")

  environment {
    variables = {
      DYNAMODB_TABLE = aws_dynamodb_table.events_table.name
      S3_BUCKET      = aws_s3_bucket.data_lake.id
      ENVIRONMENT    = var.environment
    }
  }
}

resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn                   = aws_sqs_queue.pipeline_queue.arn
  function_name                      = aws_lambda_function.event_processor.arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]
}
