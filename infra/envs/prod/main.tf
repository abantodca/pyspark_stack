data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region
}

module "network" {
  source            = "../../modules/network"
  name_prefix       = var.name_prefix
  availability_zone = var.availability_zone
  my_ip_cidr        = var.my_ip_cidr
  airflow_domain    = var.airflow_domain
}

module "orchestrator" {
  source            = "../../modules/orchestrator"
  name_prefix       = var.name_prefix
  instance_type     = var.instance_type
  ami_id            = var.ami_id
  root_volume_gb    = var.root_volume_gb
  data_volume_gb    = var.data_volume_gb
  availability_zone = var.availability_zone
  ssh_public_key    = var.ssh_public_key
  subnet_id         = module.network.subnet_id
  security_group_id = module.network.security_group_id
}

module "scheduler" {
  source          = "../../modules/scheduler"
  name_prefix     = var.name_prefix
  account_id      = local.account_id
  region          = local.region
  instance_id     = module.orchestrator.instance_id
  start_cron      = var.start_cron
  stop_cron       = var.stop_cron
  lambdas_src_dir = "${path.module}/../../lambdas"
}

module "https" {
  source              = "../../modules/https"
  name_prefix         = var.name_prefix
  airflow_domain      = var.airflow_domain
  dns_zone            = var.dns_zone
  letsencrypt_email   = var.letsencrypt_email
  manage_registrar_ns = var.manage_registrar_ns
  public_ip           = module.orchestrator.public_ip
  instance_role_name  = module.orchestrator.instance_role_name
}