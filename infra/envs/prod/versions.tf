terraform {
  required_version = ">= 1.10" # use_lockfile (backend.tf) no existe antes de 1.10
  required_providers {
    # >= 6.16: scheduler_configuration de EMR Serverless (sección 6.4) no existía en provider 5.x.
    aws     = { source = "hashicorp/aws", version = ">= 6.16, < 7.0" }
    random  = { source = "hashicorp/random", version = "~> 3.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.0" } # para zippear la Lambda
  }
}
provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { Project = "pyspark-stack", ManagedBy = "terraform", Env = "prod" }
  }
}
