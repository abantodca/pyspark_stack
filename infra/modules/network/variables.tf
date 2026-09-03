
variable "name_prefix" {
  type = string
}

variable "availability_zone" {
  description = "AZ fija de subnet, EC2 y EBS."
  type        = string
}

variable "my_ip_cidr" {
  description = "IP /32 del operador: única fuente permitida para SSH y la web."
  type        = string
}

variable "airflow_domain" {
  description = "Vacío = sin regla 443 (sección 5.6)."
  type        = string
  default     = ""
}
