# public_ip sale de la EIP (estable entre stop/start), no de la IP efímera de la instancia.
output "public_ip"   { value = aws_eip.pyspark.public_ip }
output "instance_id" { value = aws_instance.pyspark.id }
output "tunnel_command" {
  # Solo Airflow (8082). Spark ya no corre en la EC2 (EMR Serverless), así que no hay UI 8081/9870
  # que tunelear, y no hay Jupyter en prod (no se usa acá, §5.5). Si exponés la web por HTTPS (§5.6),
  # entrás directo a https://${var.airflow_domain} y este túnel a 8082 es opcional (y daría warning
  # de cert en localhost:8082, porque el api-server ya sirve TLS del FQDN).
  value = "ssh -i ~/.ssh/pyspark_stack -L 8082:localhost:8082 ec2-user@${aws_eip.pyspark.public_ip}"
}

output "airflow_domain" { value = var.airflow_domain }
output "airflow_url" {
  value = var.airflow_domain == "" ? "(no expuesto: solo túnel SSH)" : "https://${var.airflow_domain}"
}
# Lo consume el comando de emisión del cert (abajo), para no repetir el email a mano.
output "letsencrypt_email" { value = var.letsencrypt_email }