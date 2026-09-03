# La hosted zone se CREA acá, no se busca con un `data`: tener el dominio registrado en Route 53
# Domains no implica que exista la zona (un destroy se la lleva y el registro del dominio queda),
# y un `data "aws_route53_zone"` falla con "no matching Route53Zone found" antes de crear nada.
resource "aws_route53_zone" "main" {
  count   = var.airflow_domain == "" ? 0 : 1
  name    = var.dns_zone
  comment = "${var.name_prefix} - delegacion de ${var.dns_zone}"

  # Recrearla asigna OTROS 4 nameservers y obliga a re-delegar en el registrador, con la
  # propagación del TLD de por medio (hasta 48 h). Mismo criterio que el EBS de datos: no es
  # un recurso desechable, y `prevent_destroy` aborta el plan entero (sección 21.4).
  lifecycle { prevent_destroy = true }
}

# Delegación: apunta los nameservers del registrador a los de la zona de arriba. Sin esto la zona
# existe pero nadie la consulta, y `dig` no devuelve nada aunque el A record esté creado.
# Solo aplica si el dominio está registrado en Route 53 Domains de ESTA cuenta; en otro
# registrador deje manage_registrar_ns = false y copie el output dns_zone_name_servers a mano.
# La API de route53domains vive únicamente en us-east-1: con aws_region distinta, este recurso
# necesita un provider con alias en esa región.
resource "aws_route53domains_registered_domain" "main" {
  count       = var.airflow_domain == "" || !var.manage_registrar_ns ? 0 : 1
  domain_name = var.dns_zone

  dynamic "name_server" {
    for_each = aws_route53_zone.main[0].name_servers
    content { name = name_server.value }
  }
}

# A record airflow.midominio.com -> EIP estable de EC2 (sección 5.3). TTL corto para facilitar rotación.
resource "aws_route53_record" "airflow" {
  count   = var.airflow_domain == "" ? 0 : 1
  zone_id = aws_route53_zone.main[0].zone_id
  name    = var.airflow_domain
  type    = "A"
  ttl     = 300
  records = [var.public_ip]
}

# Deja que certbot (en la EC2, con el rol de instancia) resuelva el reto DNS-01 tocando SOLO esta
# zona. La política va en un .json aparte y se inyecta el zone_id con templatefile (bloque de abajo).
resource "aws_iam_role_policy" "ec2_route53_certbot" {
  count = var.airflow_domain == "" ? 0 : 1
  name  = "ec2-route53-certbot"
  role  = var.instance_role_name
  policy = templatefile("${path.module}/policies/route53-certbot.json.tftpl", {
    zone_id = aws_route53_zone.main[0].zone_id
  })
}

# Las 5 variables HTTPS del .env. Con airflow_domain vacío el mapa queda vacío y for_each no crea
# ninguno: sin dominio no hay override HTTPS que alimentar.
locals {
  airflow_https_env = var.airflow_domain == "" ? {} : {
    airflow_domain            = var.airflow_domain
    airflow_base_url          = "https://${var.airflow_domain}"
    airflow_execution_api_url = "https://${var.airflow_domain}:8080/execution/"
    airflow_ssl_cert          = "/opt/airflow/certs/live/${var.airflow_domain}/fullchain.pem"
    airflow_ssl_key           = "/opt/airflow/certs/live/${var.airflow_domain}/privkey.pem"
  }
}

resource "aws_ssm_parameter" "airflow_https" {
  for_each = local.airflow_https_env
  name     = "/${var.name_prefix}/config/${each.key}"
  type     = "String"
  value    = each.value
}