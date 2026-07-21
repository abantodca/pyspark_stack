# infra/prod/variables.tf
# Prefijo único: todos los recursos lo interpolan como "${var.name_prefix}-...".
variable "name_prefix" {
  type    = string
  default = "pyspark-stack"
}
variable "aws_region" {
  type    = string
  default = "us-east-1"
}
# AZ fija y explícita. Antes la subnet salía de data.aws_subnets.default.ids[0], y la API de AWS
# NO garantiza el orden de esa lista: si un apply futuro devolvía otra subnet en otra AZ, la EC2
# se recreaba, el volumen /data quedaba forzado a reemplazo (un EBS no se mueve de AZ) y el
# prevent_destroy abortaba el plan entero, sin salida salvo editar el lifecycle a mano.
# Tiene que pertenecer a var.aws_region.
variable "availability_zone" {
  type    = string
  default = "us-east-1a"
}
variable "instance_type" {
  type = string
  # t3.large (2 vCPU/8 GB) corre SOLO el orquestador: Airflow + Postgres + monitoreo, casi idle
  # entre corridas. Spark salió de la caja → EMR Serverless (§6.4), así que ya NO hace falta la
  # CPU dedicada de m6i: un burstable (t3) es lo correcto y bastante más barato. (Antes se
  # desaconsejaba t3 porque las JVMs de Spark degradan en burstable; ese motivo se mudó a EMR
  # Serverless, que tiene su propio cómputo dedicado por-job.)
  default = "t3.large"
}
variable "root_volume_gb" {
  type    = number
  default = 40
}
variable "data_volume_gb" {
  type    = number
  # gp3 crece online (aws ec2 modify-volume + xfs_growfs, sin downtime) pero NO se achica:
  # arrancá chico y crecé cuando la alerta HostDiskAlmostFull (§12.4) avise. Sin HDFS, /data solo
  # tiene Postgres + 15d de Prometheus + 7d de Loki → 30 GB sobran a esta escala. gp3 da 3000 IOPS
  # / 125 MB/s independientes del tamaño, así que un disco más grande no rinde más, solo cuesta más.
  default = 30
}
variable "my_ip_cidr" {
  description = "Tu IP /32 (única fuente de SSH y de la web de Airflow). curl -s https://checkip.amazonaws.com"
  type        = string
}
variable "ssh_public_key" {
  description = "Contenido de ~/.ssh/pyspark_stack.pub"
  type        = string
}
# --- Web de Airflow por HTTPS (§5.6). Dejá airflow_domain = "" para NO exponer nada (solo túnel). ---
variable "airflow_domain" {
  description = "FQDN de la web de Airflow, p.ej. airflow.midominio.com. Vacío = no exponer (solo túnel SSH)."
  type        = string
  default     = ""
}
variable "dns_zone" {
  description = "Hosted zone de Route 53 donde vive airflow_domain, p.ej. midominio.com (sin punto final)."
  type        = string
  default     = ""
}
variable "letsencrypt_email" {
  description = "Email para el registro de Let's Encrypt (avisos de expiración del cert)."
  type        = string
  default     = ""
}
# Horarios de auto start/stop (UTC). Ajustá a tu zona.
variable "start_cron" {
  type    = string
  default = "cron(0 11 ? * MON-FRI *)" # 08:00 ART
}
variable "stop_cron" {
  type    = string
  default = "cron(0 22 ? * MON-FRI *)" # 19:00 ART
}