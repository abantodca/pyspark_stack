terraform {
  backend "s3" {
    bucket         = "pyspark-stack-tfstate-tu-sufijo-2026"   # el mismo del bootstrap
    key            = "pyspark-stack-prod/terraform.tfstate"
    region         = "us-east-1"
    use_lockfile   = true   # lock nativo de S3 (conditional writes); reemplaza a dynamodb_table
    encrypt        = true
  }
}