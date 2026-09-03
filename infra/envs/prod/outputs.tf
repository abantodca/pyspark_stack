output "name_prefix" { value = var.name_prefix }
output "aws_region" { value = var.aws_region }
output "account_id" { value = local.account_id }

# Lo consume scripts/update-sg-ip.sh (más abajo, Opción B), para no buscar el SG por nombre.
output "security_group_id" { value = module.network.security_group_id }