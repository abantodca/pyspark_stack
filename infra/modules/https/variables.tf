variable "name_prefix" { type = string }
variable "airflow_domain" { type = string }
variable "dns_zone" { type = string }
variable "letsencrypt_email" { type = string }

variable "manage_registrar_ns" {
  description = "true = el dominio está registrado en Route 53 Domains de esta cuenta y Terraform re-delega los NS."
  type        = bool
  default     = false
}

variable "public_ip" {
  description = "EIP de la EC2: destino del registro A."
  type        = string
}

variable "instance_role_name" {
  description = "Rol de la EC2 al que se le cuelga el permiso DNS-01."
  type        = string
}
