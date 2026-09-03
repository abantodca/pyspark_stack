variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "pyspark-stack"
}
# AZ fija y explícita. Antes la subnet salía de data.aws_subnets.default.ids[0], y la API de AWS
# NO garantiza el orden de esa lista: si un apply futuro devolvía otra subnet en otra AZ, la EC2
# se recreaba, el volumen /data quedaba forzado a reemplazo (un EBS no se mueve de AZ) y el
# prevent_destroy abortaba el plan entero, sin salida salvo editar el lifecycle a mano.
# Tiene que pertenecer a var.aws_region.
variable "availability_zone" {
  type    = string
  default = "us-east-1a"

  validation {
    condition = (
      startswith(var.availability_zone, var.aws_region) &&
      length(var.availability_zone) == length(var.aws_region) + 1 &&
      can(regex("[a-z]$", var.availability_zone))
    )
    error_message = "availability_zone debe ser una AZ estándar de aws_region, por ejemplo us-east-1a."
  }
}
variable "instance_type" {
  type = string
  # t3.large (2 vCPU/8 GB) corre SOLO el orquestador: Airflow + Postgres + monitoreo, casi idle
  # entre corridas. Spark se ejecuta en EMR Serverless (sección 6.4), por lo que ya no requiere la
  # CPU dedicada de m6i: un burstable (t3) es lo correcto y bastante más barato. (Antes se
  # desaconsejaba t3 porque las JVMs de Spark degradan en burstable; ese motivo se mudó a EMR
  # Serverless, que tiene su propio cómputo dedicado por-job.)
  default = "t3.large"
}
variable "ami_id" {
  description = "AMI AL2023 x86_64 resuelta y fijada en terraform.tfvars para esta release."
  type        = string
  validation {
    condition     = can(regex("^ami-[0-9a-f]+$", var.ami_id))
    error_message = "ami_id debe ser un ID de AMI explícito; no use most_recent."
  }
}
variable "root_volume_gb" {
  type    = number
  default = 40
}
variable "data_volume_gb" {
  type = number
  # gp3 crece online (aws ec2 modify-volume + xfs_growfs, sin downtime) pero NO se achica:
  # comience con poco espacio y amplíelo cuando HostDiskAlmostFull (sección 12.4) lo indique. Sin HDFS, /data
  # tiene Postgres + 15d de Prometheus + 7d de Loki → 30 GB sobran a esta escala. gp3 da 3000 IOPS
  # / 125 MB/s independientes del tamaño, así que un disco más grande no rinde más, solo cuesta más.
  default = 30
}
variable "my_ip_cidr" {
  description = "IP /32 del operador: única fuente permitida para SSH y la web de Airflow."
  type        = string

  validation {
    condition = (
      can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/32$", var.my_ip_cidr)) &&
      can(cidrhost(var.my_ip_cidr, 0))
    )
    error_message = "my_ip_cidr debe ser un CIDR IPv4 /32 válido, por ejemplo 203.0.113.10/32."
  }
}
variable "ssh_public_key" {
  description = "Contenido de ~/.ssh/pyspark_stack.pub"
  type        = string

  # Sin validation, un Enter en blanco en el prompt interactivo (cuando falta esta línea en
  # tfvars) pasa "" a aws_key_pair y el error recién aparece del lado de AWS: MissingParameter:
  # PublicKeyMaterial, sin decir qué variable lo causó.
  validation {
    condition     = can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp[0-9]+) ", var.ssh_public_key))
    error_message = "ssh_public_key debe ser una clave pública SSH válida (ssh-ed25519/ssh-rsa/ecdsa-...), por ejemplo el contenido de ~/.ssh/pyspark_stack.pub. No puede estar vacío."
  }
}
# --- Web de Airflow por HTTPS (sección 5.6). Mantenga airflow_domain = "" para usar solo túnel. ---
variable "airflow_domain" {
  description = "FQDN de la web de Airflow, p.ej. airflow.midominio.com. Vacío = no exponer (solo túnel SSH)."
  type        = string
  default     = ""
}
variable "dns_zone" {
  description = "Hosted zone de Route 53 que Terraform crea para airflow_domain, p.ej. midominio.com (sin punto final)."
  type        = string
  default     = ""

  # airflow_domain tiene que ser un subdominio de dns_zone: si no, el A record se crea en una zona
  # que no delega ese nombre y `dig` no devuelve nada, sin ningún error de Terraform.
  validation {
    condition     = var.airflow_domain == "" || endswith(var.airflow_domain, ".${var.dns_zone}")
    error_message = "airflow_domain debe ser un subdominio de dns_zone, por ejemplo airflow.midominio.com dentro de midominio.com."
  }
}
# El dominio está registrado en Route 53 Domains de esta cuenta: Terraform re-delega los NS a la
# zona que crea. En otro registrador (GoDaddy, Namecheap) déjelo en false y copie a mano el output
# dns_zone_name_servers en el panel del registrador.
variable "manage_registrar_ns" {
  type    = bool
  default = false
}
variable "letsencrypt_email" {
  description = "Email para el registro de Let's Encrypt (avisos de expiración del cert)."
  type        = string
  default     = ""
}
# Usado recién en la sección 18 (Budgets, Cost Anomaly Detection, alarma de la DLQ) — con default vacío como
# airflow_domain/dns_zone/letsencrypt_email: no bloquea los `apply` de las secciones 5-17, que no lo
# usan. Defina un valor real antes de aplicar sección 18; sin él, las notificaciones no tienen destino.
variable "alert_email" {
  description = "Email para alertas de gobierno/costo (Budgets, Cost Anomaly Detection, DLQ de Lambdas). sección 18."
  type        = string
  default     = ""
}
# Usadas recién en la sección 11.4 (rol de OIDC). Con default vacío no bloquean los apply de las secciones 5 a 10.
variable "github_org" {
  type    = string
  default = ""
}
variable "github_repo" {
  type    = string
  default = ""
}
# Horarios de auto start/stop (UTC). Ajústelos a la zona operativa.
variable "start_cron" {
  type    = string
  default = "cron(0 11 ? * MON-FRI *)" # 08:00 ART
}
variable "stop_cron" {
  type    = string
  default = "cron(0 22 ? * MON-FRI *)" # 19:00 ART
}