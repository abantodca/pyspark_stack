variable "name_prefix" { type = string }

variable "instance_type" { type = string }
variable "ami_id" { type = string }
variable "root_volume_gb" { type = number }
variable "data_volume_gb" { type = number }
variable "availability_zone" { type = string }
variable "ssh_public_key" { type = string }

# Del módulo network: entran como valor, no como referencia cruzada.
variable "subnet_id" { type = string }
variable "security_group_id" { type = string }
