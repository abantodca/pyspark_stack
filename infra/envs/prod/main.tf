data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

module "network" {
  source            = "../../modules/network"
  name_prefix       = var.name_prefix
  availability_zone = var.availability_zone
  my_ip_cidr        = var.my_ip_cidr
  airflow_domain    = var.airflow_domain
}