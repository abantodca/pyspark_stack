output "airflow_domain" { value = var.airflow_domain }
output "airflow_url" {
  value = var.airflow_domain == "" ? "(no expuesto: solo túnel SSH)" : "https://${var.airflow_domain}"
}
output "letsencrypt_email" { value = var.letsencrypt_email }

# Para delegar a mano cuando el dominio no está en Route 53 Domains (manage_registrar_ns = false).
output "dns_zone_id" {
  value = var.airflow_domain == "" ? "" : aws_route53_zone.main[0].zone_id
}
output "dns_zone_name_servers" {
  value = var.airflow_domain == "" ? [] : aws_route53_zone.main[0].name_servers
}
