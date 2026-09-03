output "name_prefix" { value = var.name_prefix }
output "aws_region" { value = var.aws_region }
output "account_id" { value = local.account_id }

# Lo consume scripts/update-sg-ip.sh (más abajo, Opción B), para no buscar el SG por nombre.
output "security_group_id" { value = module.network.security_group_id }

# Contrato público del módulo orchestrator. Los outputs de un módulo hijo no se
# consultan desde el directorio del entorno hasta que se re-publican aquí.
output "instance_id" { value = module.orchestrator.instance_id }
output "public_ip" { value = module.orchestrator.public_ip }
output "data_volume_id" { value = module.orchestrator.data_volume_id }
output "key_name" { value = module.orchestrator.key_name }
output "instance_role_name" { value = module.orchestrator.instance_role_name }
output "instance_role_arn" { value = module.orchestrator.instance_role_arn }

# Automatización publicada por la sección 5.4. Estos outputs deben existir antes
# de recargar scripts/prod-env.sh para operar la Lambda por su nombre real.
output "lambda_startstop_name" { value = module.scheduler.lambda_startstop_name }
output "schedule_start_name" { value = module.scheduler.schedule_start_name }
output "schedule_stop_name" { value = module.scheduler.schedule_stop_name }

# CONTRATO CON LA LÍNEA DE COMANDOS. scripts/prod-env.sh exporta cada uno de estos outputs
# como variable en MAYÚSCULAS (public_ip → $PUBLIC_IP). Regla: si un comando de la guía lo
# necesita, se define aquí. No incluya secretos: se almacenan en SSM (sección 13).

# ── Cómputo/red adicional
output "availability_zone" { value = var.availability_zone }

# Los outputs de la automatización de la sección 5.4 ya fueron publicados allí;
# no los vuelva a declarar porque Terraform rechazaría nombres duplicados.

# ── Comodidad: comandos listos para pegar, ya resueltos con los valores reales.
output "tunnel_command" {
  # Solo Airflow (8082). Spark ya no corre en la EC2 (EMR Serverless), así que no hay UI 8081/9870
  # que tunelizar, y Jupyter no se usa en producción. Con HTTPS (sección 5.6), se accede directamente a
  # https://${var.airflow_domain}; el túnel a 8082 queda opcional y genera una advertencia
  # de cert en localhost:8082, porque el api-server ya sirve TLS del FQDN).
  #
  # Usa la clave y el usuario predeterminados: Terraform no conoce las rutas del equipo local. Si
  # cambió SSH_KEY o SSH_USER en prod.env, el comando canónico es `task prod:tunnel`
  # ($SSH -L 8082:localhost:8082 "$SSH_TARGET"), que sí los respeta. Este output es comodidad.
  value = "ssh -i ~/.ssh/pyspark_stack -L 8082:localhost:8082 ec2-user@${module.orchestrator.public_ip}"
}

# los pegues en terraform.tfvars: outputs van en un output "..." { value = ... }, tfvars son
# asignaciones sueltas (bloque de abajo) — mezclarlos rompe el parseo de Terraform.
output "airflow_domain" { value = module.https.airflow_domain }
output "airflow_url" { value = module.https.airflow_url }
# Lo consume el comando de emisión del cert (abajo), para no repetir el email a mano.
output "letsencrypt_email" { value = module.https.letsencrypt_email }
# La zona la crea el módulo https (sección 5.6). Los NS se publican para poder delegar a mano
# cuando el dominio no está registrado en Route 53 Domains.
# $DNS_ZONE lo usa el `dig` de verificación de la delegación (sección 5.6).
output "dns_zone" { value = var.dns_zone }
output "dns_zone_id" { value = module.https.dns_zone_id }
output "dns_zone_name_servers" { value = module.https.dns_zone_name_servers }
