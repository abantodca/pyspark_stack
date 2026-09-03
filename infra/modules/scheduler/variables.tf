variable "name_prefix" { type = string }
variable "instance_id" { type = string }
variable "account_id" { type = string }
variable "region" { type = string }
variable "start_cron" { type = string }
variable "stop_cron" { type = string }

variable "lambdas_src_dir" {
  description = "Ruta a infra/lambdas/ desde el entorno que compone."
  type        = string
}

variable "log_retention_days" {
  type    = number
  default = 14
}