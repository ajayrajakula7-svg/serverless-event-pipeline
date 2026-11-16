variable "aws_region" {
  description = "AWS deployment region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Target deployment environment"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Identifier prefix for pipeline resources"
  type        = string
  default     = "cloud-processing-pipeline"
}
