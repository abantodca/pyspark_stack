# infra/prod/dns.tf
data "aws_route53_zone" "main" {
  count = var.airflow_domain == "" ? 0 : 1
  name  = var.dns_zone                # p.ej. "midominio.com" (la hosted zone, sin punto final)
}

# A record airflow.midominio.com -> EIP estable de la EC2 (§5.3). TTL corto por si rotás la IP.
resource "aws_route53_record" "airflow" {
  count   = var.airflow_domain == "" ? 0 : 1
  zone_id = data.aws_route53_zone.main[0].zone_id
  name    = var.airflow_domain
  type    = "A"
  ttl     = 300
  records = [aws_eip.pyspark.public_ip]
}

# Deja que certbot (en la EC2, con el rol de instancia) resuelva el reto DNS-01 tocando SOLO esta
# zona. La política va en un .json aparte y se inyecta el zone_id con templatefile (bloque de abajo).
resource "aws_iam_role_policy" "ec2_route53_certbot" {
  count = var.airflow_domain == "" ? 0 : 1
  name  = "ec2-route53-certbot"
  role  = aws_iam_role.ec2.id
  policy = templatefile("${path.module}/policies/route53-certbot.json.tftpl", {
    zone_id = data.aws_route53_zone.main[0].zone_id
  })
}