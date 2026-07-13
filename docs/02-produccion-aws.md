# Guía experta — Producción en AWS (Terraform, EC2 + Serverless + automatización)

> Guía única de producción para el stack (HDFS/Spark/Jupyter/Airflow). Cubre **las dos
> arquitecturas** y cuándo usar cada una, con Terraform copy-paste, estado remoto y
> **automatización con EventBridge + Lambda** para bajar el costo al mínimo.
>
> - **Arquitectura A — EC2 con Docker** (lift-and-shift): rápido, idéntico al local. Con
>   *auto start/stop* por horario vía **EventBridge + Lambda**.
> - **Arquitectura B — Serverless** (S3 + EMR Serverless + MWAA / Step Functions): pago por uso,
>   sin servidores que administrar. La opción moderna y barata para producción real.

Índice:
1. [Panorama y elección de arquitectura](#1-panorama-y-elección-de-arquitectura)
2. [Comparativa de costos](#2-comparativa-de-costos)
3. [Prerrequisitos](#3-prerrequisitos)
4. [Fundamentos comunes: backend Terraform (S3 + DynamoDB)](#4-fundamentos-comunes-backend-terraform)
5. [Arquitectura A — EC2 con Docker](#5-arquitectura-a--ec2-con-docker)
   - 5.1 [Variables y red (SSH-only)](#51-variables-y-red)
   - 5.2 [IAM + key pair](#52-iam--key-pair)
   - 5.3 [EC2 + EBS + user_data](#53-ec2--ebs--user_data)
   - 5.4 [**Automatización: EventBridge + Lambda (auto start/stop)**](#54-automatización-eventbridge--lambda)
   - 5.5 [Desplegar, subir código y túnel SSH](#55-desplegar-subir-código-y-túnel-ssh)
6. [Arquitectura B — Serverless (EMR + MWAA)](#6-arquitectura-b--serverless)
   - 6.1 [S3 + Glue Catalog](#61-s3--glue-catalog)
   - 6.2 [IAM (EMR + MWAA)](#62-iam-emr--mwaa)
   - 6.3 [EMR Serverless](#63-emr-serverless)
   - 6.4 [Red + MWAA](#64-red--mwaa)
   - 6.5 [Adaptar jobs y DAG](#65-adaptar-jobs-y-dag)
7. [**Variante ultra-barata: EventBridge + Lambda en vez de MWAA**](#7-variante-ultra-barata-eventbridge--lambda)
8. [Operación, seguridad y ahorro](#8-operación-seguridad-y-ahorro)
9. [Notebooks: dónde viven y cómo se ejecutan](#9-notebooks-dónde-viven-y-cómo-se-ejecutan)
   - 9.1 [Habilitar papermill](#91-habilitar-papermill)
   - 9.2 [Parametrizar el notebook](#92-parametrizar-el-notebook)
   - 9.3 [DAG que ejecuta el notebook](#93-dag-que-ejecuta-el-notebook--dagsrun_notebook_dagpy)
10. [Flujo local → servidor → los DAGs corren solos](#10-flujo-local--servidor--los-dags-corren-solos)
    - 10.1 [Modo rápido (dev): deploy manual sin CI](#101-modo-rápido-dev-deploy-manual-sin-esperar-ci)
11. [CI/CD con GitHub Actions (OIDC, sin claves)](#11-cicd-con-github-actions-oidc-sin-claves)
    - 11.1 [Terraform: OIDC provider + rol](#111-terraform-oidc-provider--rol--infraprodcicdtf)
    - 11.2 [Workflow de CI](#112-workflow-de-ci--githubworkflowsciyml)
    - 11.3 [Workflow de Deploy](#113-workflow-de-deploy--githubworkflowsdeployyml)
    - 11.4 [Puesta en marcha](#114-puesta-en-marcha-una-vez)
12. [Monitoreo (Prometheus + Grafana + Alertmanager + Loki)](#12-monitoreo-prometheus--grafana--alertmanager--loki)
    - 12.1 [Qué se monitorea y análisis de completitud](#121-qué-se-monitorea-y-análisis-de-completitud)
    - 12.2 [Estructura de archivos](#122-estructura-de-archivos-monitoring)
    - 12.3 [Servicios de monitoreo (compose)](#123-servicios-de-monitoreo-compose)
    - 12.4 [Prometheus + alertas + Alertmanager](#124-prometheus--alertas--alertmanager)
    - 12.5 [Métricas de Airflow y Spark](#125-métricas-de-airflow-y-spark)
    - 12.6 [Grafana (datasources + dashboard)](#126-grafana-datasources--dashboard)
    - 12.7 [Logs: Loki + Promtail](#127-logs-loki--promtail)
    - 12.8 [Acceso, verificación y HDFS](#128-acceso-verificación-y-hdfs)
13. [Hardening de producción (secretos, restart, límites, s3a)](#13-hardening-de-producción)
14. [Archivos compose completos (copy-paste)](#14-archivos-compose-completos)
    - 14.1 [docker-compose.prod.yml (producción, completo)](#141-docker-composeprodyml-producción-completo)
    - 14.2 [docker-compose.dev.yml (dev-lite 8 GB)](#142-docker-composedevyml-dev-lite-8-gb)

---

## 1. Panorama y elección de arquitectura

| Necesidad | Arquitectura recomendada |
|---|---|
| Prototipo / entorno idéntico al local / una persona | **A — EC2** (con auto start/stop) |
| Producción real, cargas intermitentes, mínimo mantenimiento | **B — Serverless** |
| Máximo ahorro y podés soltar Airflow | **B + Step Functions/Lambda** (§7) |

**Regla mental:** almacenar es barato y constante; **computar es lo que cuesta, y solo cuando
corrés**. Toda la guía empuja el gasto hacia *pago por uso* y elimina lo *always-on*.

```
Arquitectura A (EC2)                    Arquitectura B (Serverless)
┌──────────────────────────┐           ┌──────────────────────────────────┐
│ EC2 + Docker Compose      │           │ S3 (data lake) + Glue Catalog     │
│  todo el stack en 1 host  │           │ EMR Serverless (Spark, pago/seg)  │
│  EventBridge+Lambda:      │           │ MWAA  ó  EventBridge+Lambda        │
│   prende 8am / apaga 18h  │           │  (orquestación)                   │
└──────────────────────────┘           └──────────────────────────────────┘
```

---

## 2. Comparativa de costos

> Precios **aproximados** us-east-1 (on-demand), sujetos a cambio — validá en
> [calculator.aws](https://calculator.aws). Escenario: ~1 h de Spark/día, ~50 GB.

| Opción | Qué corre | US$/mes | Naturaleza |
|---|---|---|---|
| **A — EC2 `m6i.2xlarge` 24/7** | stack completo | **~300** | Fijo (corras o no) |
| **A — EC2 apagable** (auto start/stop, 8h×22d) | stack completo o dev-lite | **~70 / ~17** | Fijo reducido |
| **B — Serverless + MWAA** | EMR + MWAA + NAT | **~175** | $167 fijo + ~$8 uso |
| **B — Serverless + EventBridge/Lambda** (§7) | EMR + Lambda | **~8–15** | Casi todo por uso |

El auto start/stop de §5.4 convierte los ~$300 fijos de A en ~$70 (o ~$17 con el compose dev-lite
de `docker-compose.dev.yml`). La variante serverless de §7 es la más barata para producción.

---

## 3. Prerrequisitos

```bash
aws configure && aws sts get-caller-identity   # credenciales con permisos EC2/S3/IAM/Lambda...
terraform -version                             # >= 1.6
ssh-keygen -t ed25519 -f ~/.ssh/pyspark_stack -C "pyspark_stack"   # si no tenés par de claves
```

Estructura de carpetas Terraform:

```
infra/
├── bootstrap/           # crea backend S3+DynamoDB (una vez, state local)
├── a-ec2/               # Arquitectura A
│   ├── lambda/startstop.py
│   └── *.tf
└── b-serverless/        # Arquitectura B
    └── *.tf
```

---

## 4. Fundamentos comunes: backend Terraform

Problema huevo-y-gallina: el backend S3 debe existir antes de que Terraform guarde el state ahí.
Se crea con un mini-Terraform de **state local**, una sola vez.

**`infra/bootstrap/main.tf`**:

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }
  # sin backend => state LOCAL (intencional: este stack crea el backend).
}
provider "aws" { region = "us-east-1" }

locals {
  state_bucket = "pyspark-stack-tfstate-abanto-2026"   # ← único global, cambiá el sufijo
  lock_table   = "pyspark-stack-tf-lock"
}

resource "aws_s3_bucket" "tfstate" {
  bucket = local.state_bucket
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule { apply_server_side_encryption_by_default { sse_algorithm = "AES256" } }
}
resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_dynamodb_table" "tf_lock" {
  name         = local.lock_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"
  attribute {
    name = "LockID"
    type = "S"
  }
}
```

```bash
cd infra/bootstrap && terraform init && terraform apply && cd ../..
```

**Backend compartido** (cada arquitectura usa el mismo bucket, distinto `key`):

```hcl
# infra/a-ec2/backend.tf   (para B, usar key = "pyspark-stack-serverless/terraform.tfstate")
terraform {
  backend "s3" {
    bucket         = "pyspark-stack-tfstate-abanto-2026"
    key            = "pyspark-stack-ec2/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "pyspark-stack-tf-lock"
    encrypt        = true
  }
}
```

**Provider común** (mismo archivo en ambas carpetas):

```hcl
# providers.tf
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    random  = { source = "hashicorp/random", version = "~> 3.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.0" }  # para zippear la Lambda
  }
}
provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { Project = "pyspark-stack", ManagedBy = "terraform", Env = "prod" }
  }
}
```

---

## 5. Arquitectura A — EC2 con Docker

Lift-and-shift: una EC2 corre el `docker-compose` completo (o `docker-compose.dev.yml` si tu
máquina es chica). Acceso **solo por túnel SSH**; ninguna UI expuesta a internet.

### 5.1 Variables y red

```hcl
# infra/a-ec2/variables.tf
variable "aws_region" {
  type    = string
  default = "us-east-1"
}
variable "instance_type" {
  type    = string
  default = "m6i.2xlarge" # dev-lite: "t3.large"
}
variable "root_volume_gb" {
  type    = number
  default = 40 # dev-lite: 30
}
variable "data_volume_gb" {
  type    = number
  default = 200
}
variable "my_ip_cidr" {
  description = "Tu IP /32 (única fuente de SSH). curl -s https://checkip.amazonaws.com"
  type        = string
}
variable "ssh_public_key" {
  description = "Contenido de ~/.ssh/pyspark_stack.pub"
  type        = string
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
```

```hcl
# infra/a-ec2/network.tf
data "aws_vpc" "default" {
  default = true
}
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "pyspark" {
  name        = "pyspark-stack-sg"
  description = "SSH-only desde mi IP. UIs por tunel."
  vpc_id      = data.aws_vpc.default.id
  ingress {
    description = "SSH desde mi IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

### 5.2 IAM + key pair

```hcl
# infra/a-ec2/iam.tf
resource "aws_key_pair" "pyspark" {
  key_name   = "pyspark-stack-key"
  public_key = var.ssh_public_key
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "ec2" {
  name               = "pyspark-stack-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"   # entrar sin abrir puertos
}
resource "aws_iam_instance_profile" "ec2" {
  name = "pyspark-stack-ec2-profile"
  role = aws_iam_role.ec2.name
}
```

### 5.3 EC2 + EBS + user_data

```hcl
# infra/a-ec2/ec2.tf
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "pyspark" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.pyspark.key_name
  vpc_security_group_ids = [aws_security_group.pyspark.id]
  subnet_id              = data.aws_subnets.default.ids[0]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }
  user_data                   = templatefile("${path.module}/user_data.sh.tftpl", {})
  user_data_replace_on_change = true
  tags = { Name = "pyspark-stack", AutoStartStop = "true" }   # ← la Lambda filtra por este tag
}

resource "aws_ebs_volume" "data" {
  availability_zone = aws_instance.pyspark.availability_zone
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "pyspark-stack-data" }
}
resource "aws_volume_attachment" "data" {
  device_name = "/dev/xvdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.pyspark.id
}
```

**`infra/a-ec2/user_data.sh.tftpl`** (instala Docker + prepara disco de datos):

```bash
#!/bin/bash
set -euxo pipefail
dnf update -y && dnf install -y docker git && systemctl enable --now docker

DOCKER_CONFIG=/usr/local/lib/docker
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
usermod -aG docker ec2-user

# Disco de datos (Nitro => /dev/nvme1n1). Solo formatea si está vacío (no borra en recreación).
DATA_DEV=$(lsblk -dpno NAME | grep -E 'nvme1n1|xvdf' | head -n1 || true)
if [ -n "$DATA_DEV" ]; then
  blkid "$DATA_DEV" || mkfs -t xfs "$DATA_DEV"
  mkdir -p /data && mount "$DATA_DEV" /data
  echo "$DATA_DEV /data xfs defaults,nofail 0 2" >> /etc/fstab
  chown -R ec2-user:ec2-user /data
fi
echo 'vm.max_map_count=262144' > /etc/sysctl.d/99-pyspark.conf && sysctl --system
```

### 5.4 Automatización: EventBridge + Lambda

**El porqué:** en vez de apagar la EC2 a mano, una **Lambda** la prende/apaga y **EventBridge
Scheduler** la dispara por cron. Con Lambda (en lugar de que Scheduler llame a EC2 directo)
podés **personalizar la lógica**: no apagar si hay un DAG corriendo, avisar por SNS/Slack, o
prender solo si hay trabajo en cola. Convierte los ~$300/mes fijos en ~$70 (u ~$17 con dev-lite).

**Código de la Lambda — `infra/a-ec2/lambda/startstop.py`:**

```python
import os
import boto3

ec2 = boto3.client("ec2")

def handler(event, context):
    """Prende o apaga las EC2 marcadas con el tag AutoStartStop=true.
    event = {"action": "start"} | {"action": "stop"}
    Personalizá aquí: chequear jobs en curso, notificar, etc."""
    action   = event.get("action", "stop")
    tag_key  = os.environ.get("TAG_KEY", "AutoStartStop")
    tag_val  = os.environ.get("TAG_VALUE", "true")

    resp = ec2.describe_instances(Filters=[
        {"Name": f"tag:{tag_key}", "Values": [tag_val]},
        {"Name": "instance-state-name", "Values": ["running", "stopped", "stopping"]},
    ])
    ids = [i["InstanceId"] for r in resp["Reservations"] for i in r["Instances"]]
    if not ids:
        return {"msg": "no instances tagged", "action": action}

    if action == "start":
        ec2.start_instances(InstanceIds=ids)
    else:
        # --- PERSONALIZACIÓN: no apagar si hay algo crítico corriendo ---
        # Ej.: consultar una métrica CloudWatch, un DAG activo, un flag en SSM Parameter Store.
        # if _job_en_curso(): return {"msg": "job activo, no apago", "instances": ids}
        ec2.stop_instances(InstanceIds=ids)

    return {"action": action, "instances": ids}
```

**Terraform de la automatización — `infra/a-ec2/automation.tf`:**

```hcl
# ---- Empaquetar el código de la Lambda en un zip ----
data "archive_file" "startstop" {
  type        = "zip"
  source_file = "${path.module}/lambda/startstop.py"
  output_path = "${path.module}/lambda/startstop.zip"
}

# ---- Rol de ejecución de la Lambda ----
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "lambda" {
  name               = "pyspark-stack-startstop-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Permisos: describir/prender/apagar EC2 + logs a CloudWatch.
data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]                       # Describe no admite ARN específico
  }
  statement {
    actions   = ["ec2:StartInstances", "ec2:StopInstances"]
    resources = ["*"]
    # Endurecido: solo instancias con el tag correcto.
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/AutoStartStop"
      values   = ["true"]
    }
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}
resource "aws_iam_role_policy" "lambda" {
  name   = "startstop-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

# ---- La función Lambda ----
resource "aws_lambda_function" "startstop" {
  function_name    = "pyspark-stack-startstop"
  filename         = data.archive_file.startstop.output_path
  source_code_hash = data.archive_file.startstop.output_base64sha256
  handler          = "startstop.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.lambda.arn
  timeout          = 30
  environment {
    variables = { TAG_KEY = "AutoStartStop", TAG_VALUE = "true" }
  }
}

# ---- Rol que EventBridge Scheduler usa para invocar la Lambda ----
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "scheduler" {
  name               = "pyspark-stack-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}
resource "aws_iam_role_policy" "scheduler" {
  name = "invoke-lambda"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow", Action = "lambda:InvokeFunction",
      Resource = aws_lambda_function.startstop.arn
    }]
  })
}

# ---- Schedules: prender y apagar por cron ----
resource "aws_scheduler_schedule" "start" {
  name                         = "pyspark-stack-start"
  schedule_expression          = var.start_cron
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.startstop.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ action = "start" })
  }
}
resource "aws_scheduler_schedule" "stop" {
  name                         = "pyspark-stack-stop"
  schedule_expression          = var.stop_cron
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.startstop.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ action = "stop" })
  }
}
```

> **EventBridge Scheduler vs Rules:** usamos **Scheduler** (más nuevo) porque soporta cron con
> timezone nativo y un solo target limpio. Podría llamar a EC2 directo (universal target) sin
> Lambda, pero metemos la Lambda a propósito para poder **personalizar** (no apagar con jobs
> activos, notificar, etc.).

### 5.5 Desplegar, subir código y túnel SSH

```hcl
# infra/a-ec2/outputs.tf
output "public_ip"   { value = aws_instance.pyspark.public_ip }
output "instance_id" { value = aws_instance.pyspark.id }
output "tunnel_command" {
  value = "ssh -i ~/.ssh/pyspark_stack -L 8082:localhost:8082 -L 8888:localhost:8888 -L 8081:localhost:8081 -L 9870:localhost:9870 ec2-user@${aws_instance.pyspark.public_ip}"
}
```

```bash
cd infra/a-ec2
terraform init && terraform apply    # ~12 recursos + Lambda + schedules

# Subir el proyecto (excluí infra/ y basura)
IP=$(terraform output -raw public_ip)
cd ../..
rsync -avz --exclude '.git' --exclude 'infra' --exclude '__pycache__' \
  -e "ssh -i ~/.ssh/pyspark_stack" ./ ec2-user@$IP:/home/ec2-user/pyspark_stack/

# Levantar (completo o dev-lite según la instancia)
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'cd pyspark_stack && docker compose up -d --build'          # o -f docker-compose.dev.yml

# Túnel a las UIs (ver output tunnel_command)
ssh -i ~/.ssh/pyspark_stack -L 8082:localhost:8082 -L 8888:localhost:8888 ec2-user@$IP
```

UIs (con el túnel abierto): Airflow `localhost:8082`, Jupyter `localhost:8888`,
Spark `localhost:8081`, HDFS `localhost:9870`.

---

## 6. Arquitectura B — Serverless

Rediseño moderno: **S3** reemplaza HDFS, **Glue Catalog** el metastore, **EMR Serverless** el
cluster Spark (pago por segundo), y **MWAA** orquesta. Cómputo cero en reposo.

Mapa de migración:

| Local (compose) | Serverless AWS |
|---|---|
| HDFS namenode/datanode | S3 + Glue Data Catalog |
| spark-master/worker + `spark-submit` | EMR Serverless + `EmrServerlessStartJobOperator` |
| 5 contenedores Airflow + Postgres | MWAA (o EventBridge+Lambda, §7) |
| Jupyter | EMR Studio (opcional) |

Carpeta `infra/b-serverless/` (backend con `key = "pyspark-stack-serverless/terraform.tfstate"`).

### 6.1 S3 + Glue Catalog

```hcl
# infra/b-serverless/s3.tf
resource "random_id" "suffix" { byte_length = 4 }
locals {
  datalake  = "pyspark-stack-datalake-${random_id.suffix.hex}"
  artifacts = "pyspark-stack-artifacts-${random_id.suffix.hex}"
  dags      = "pyspark-stack-dags-${random_id.suffix.hex}"
}

resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake
}
resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifacts
}
resource "aws_s3_bucket" "dags" {
  bucket = local.dags
}

resource "aws_s3_bucket_versioning" "all" {
  for_each = toset([aws_s3_bucket.datalake.id, aws_s3_bucket.artifacts.id, aws_s3_bucket.dags.id])
  bucket   = each.value
  versioning_configuration {
    status = "Enabled" # MWAA exige versioning en DAGs
  }
}
resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = toset([aws_s3_bucket.datalake.id, aws_s3_bucket.artifacts.id, aws_s3_bucket.dags.id])
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    id     = "tiering"
    status = "Enabled"
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

# infra/b-serverless/glue.tf
resource "aws_glue_catalog_database" "main" {
  name         = "pyspark_stack_db"
  location_uri = "s3://${aws_s3_bucket.datalake.id}/curated/"
}
```

### 6.2 IAM (EMR + MWAA)

```hcl
# infra/b-serverless/iam.tf
# ---- Rol de los jobs EMR Serverless ----
data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "emr_exec" {
  name               = "pyspark-stack-emr-exec"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}
data "aws_iam_policy_document" "emr_exec" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.datalake.arn, "${aws_s3_bucket.datalake.arn}/*",
                 aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    actions   = ["glue:GetDatabase", "glue:GetTable*", "glue:CreateTable", "glue:UpdateTable",
                 "glue:GetPartition*", "glue:BatchCreatePartition", "glue:CreatePartition"]
    resources = ["*"]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup"]
    resources = ["*"]
  }
}
resource "aws_iam_role_policy" "emr_exec" {
  name   = "emr-exec"
  role   = aws_iam_role.emr_exec.id
  policy = data.aws_iam_policy_document.emr_exec.json
}

# ---- Rol de MWAA ----
data "aws_iam_policy_document" "mwaa_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "mwaa_exec" {
  name               = "pyspark-stack-mwaa-exec"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume.json
}
data "aws_iam_policy_document" "mwaa_exec" {
  statement {  # DAGs
    actions   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
    resources = [aws_s3_bucket.dags.arn, "${aws_s3_bucket.dags.arn}/*"]
  }
  statement {  # Logs
    actions   = ["logs:CreateLogStream", "logs:CreateLogGroup", "logs:PutLogEvents",
                 "logs:GetLogEvents", "logs:GetLogRecord", "logs:GetLogGroupFields",
                 "logs:GetQueryResults", "logs:DescribeLogGroups"]
    resources = ["arn:aws:logs:*:*:log-group:airflow-pyspark-stack-*"]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
  statement {
    actions   = ["sqs:*"]
    resources = ["arn:aws:sqs:*:*:airflow-celery-*"]
  }
  statement { # lanzar/monitorear EMR Serverless
    actions   = ["emr-serverless:StartApplication", "emr-serverless:StartJobRun",
                 "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun", "emr-serverless:GetApplication"]
    resources = ["*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_exec.arn]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*", "kms:Encrypt", "kms:DescribeKey"]
    resources = ["*"]
  }
}
resource "aws_iam_role_policy" "mwaa_exec" {
  name   = "mwaa-exec"
  role   = aws_iam_role.mwaa_exec.id
  policy = data.aws_iam_policy_document.mwaa_exec.json
}
```

### 6.3 EMR Serverless

```hcl
# infra/b-serverless/emr.tf
resource "aws_emrserverless_application" "spark" {
  name          = "pyspark-stack-spark"
  release_label = "emr-7.5.0"    # Spark 3.5.x. Verificá el último release.
  type          = "spark"
  architecture  = "ARM64"        # Graviton = ~20% más barato (si tu código lo soporta)

  auto_start_configuration {
    enabled = true
  }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15 # ← clave del ahorro
  }
  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }
  # sin initial_capacity => cero costo en reposo (arranque en frío ~1-2 min)
}
output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}
```

### 6.4 Red + MWAA

```hcl
# infra/b-serverless/network.tf   (MWAA exige VPC con 2 subnets privadas en 2 AZ)
data "aws_availability_zones" "available" {
  state = "available"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"
  name    = "pyspark-stack-vpc"
  cidr    = "10.20.0.0/16"

  azs             = slice(data.aws_availability_zones.available.names, 0, 2)
  private_subnets = ["10.20.1.0/24", "10.20.2.0/24"]
  public_subnets  = ["10.20.101.0/24", "10.20.102.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true # 1 solo NAT = lo más barato que funciona (~$32/mes)
  enable_dns_hostnames = true
}
resource "aws_vpc_endpoint" "s3" {   # gateway endpoint S3 = GRATIS, evita tráfico por NAT
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.us-east-1.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids
}
resource "aws_security_group" "mwaa" {
  name   = "pyspark-stack-mwaa-sg"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

```hcl
# infra/b-serverless/mwaa.tf
resource "aws_s3_object" "requirements" {
  bucket  = aws_s3_bucket.dags.id
  key     = "requirements.txt"
  content = "apache-airflow-providers-amazon>=8.0.0\n"   # trae EmrServerlessStartJobOperator
  etag    = "manual-v1"
}
resource "aws_mwaa_environment" "airflow" {
  name                 = "pyspark-stack"
  airflow_version      = "2.10.3"      # ⚠️ MWAA va por detrás de Airflow 3; verificá tu región
  environment_class    = "mw1.micro"   # la clase más barata
  source_bucket_arn    = aws_s3_bucket.dags.arn
  dag_s3_path          = "dags/"
  requirements_s3_path = aws_s3_object.requirements.key
  execution_role_arn   = aws_iam_role.mwaa_exec.arn
  min_workers           = 1
  max_workers           = 2
  webserver_access_mode = "PUBLIC_ONLY" # UI con login IAM; PRIVATE_ONLY = solo VPC
  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = module.vpc.private_subnets
  }
  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }
}
output "mwaa_webserver_url" {
  value = aws_mwaa_environment.airflow.webserver_url
}
```

> ⏱️ Crear MWAA tarda ~20-30 min. Es normal.

### 6.5 Adaptar jobs y DAG

**Jobs PySpark:** cambiá rutas HDFS → S3 (`s3://…`) y subí el script a S3:

```python
df = spark.read.csv("s3://pyspark-stack-datalake-xxxx/raw/customers.csv", header=True)
df.write.mode("overwrite").parquet("s3://pyspark-stack-datalake-xxxx/curated/customers")
```
```bash
aws s3 cp spark-apps/customer_etl.py s3://pyspark-stack-artifacts-xxxx/scripts/customer_etl.py
```

**DAG con EMR Serverless — `dags/customer_etl_emr.py`:**

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

with DAG("customer_etl_emr", schedule="@daily", start_date=datetime(2026,1,1),
         catchup=False, tags=["emr-serverless"]) as dag:
    EmrServerlessStartJobOperator(
        task_id="run_customer_etl",
        application_id="00fxxxxxxxxxxxxx",                               # emr_application_id
        execution_role_arn="arn:aws:iam::<ACCOUNT>:role/pyspark-stack-emr-exec",
        job_driver={"sparkSubmit": {
            "entryPoint": "s3://pyspark-stack-artifacts-xxxx/scripts/customer_etl.py",
            "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=8g",
        }},
        configuration_overrides={"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": "s3://pyspark-stack-artifacts-xxxx/logs/"}}},
        wait_for_completion=True,
    )
```

El DAG ya **no ejecuta Spark**: solo dispara el job en EMR Serverless. MWAA queda liviano.

---

## 7. Variante ultra-barata: EventBridge + Lambda en vez de MWAA

**El mayor costo fijo de la Arquitectura B es MWAA (~$135/mes) + NAT (~$32).** Si tu orquestación
es simple (disparar jobs por horario o por evento), reemplazalos por **EventBridge + una Lambda
que lanza el job EMR Serverless**. Costo total: **~$8–15/mes**. Y elimina la VPC/NAT porque EMR
Serverless no necesita VPC.

**Lambda que dispara el job — `infra/b-serverless/lambda/trigger_emr.py`:**

```python
import os
import boto3

emr = boto3.client("emr-serverless")

def handler(event, context):
    """Lanza un job Spark en EMR Serverless.
    Disparado por cron (EventBridge Scheduler) o por evento (llegada de archivo a S3)."""
    # 'script' puede venir en el event (personalización) o de env var por defecto.
    script = event.get("script", os.environ["DEFAULT_SCRIPT"])

    resp = emr.start_job_run(
        applicationId=os.environ["EMR_APP_ID"],
        executionRoleArn=os.environ["EMR_ROLE_ARN"],
        jobDriver={"sparkSubmit": {
            "entryPoint": script,
            "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=8g",
        }},
        configurationOverrides={"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": os.environ["LOG_URI"]}}},
    )
    return {"jobRunId": resp["jobRunId"], "script": script}
```

**Terraform — `infra/b-serverless/orchestration_lite.tf`:**

```hcl
data "archive_file" "trigger_emr" {
  type        = "zip"
  source_file = "${path.module}/lambda/trigger_emr.py"
  output_path = "${path.module}/lambda/trigger_emr.zip"
}

data "aws_iam_policy_document" "trigger_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "trigger" {
  name               = "pyspark-stack-trigger-emr"
  assume_role_policy = data.aws_iam_policy_document.trigger_assume.json
}
resource "aws_iam_role_policy" "trigger" {
  name = "trigger-emr"
  role = aws_iam_role.trigger.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun",
                                     "emr-serverless:StartApplication"], Resource = "*" },
      { Effect = "Allow", Action = "iam:PassRole", Resource = aws_iam_role.emr_exec.arn },
      { Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogStream",
                                     "logs:PutLogEvents"], Resource = "arn:aws:logs:*:*:*" },
    ]
  })
}
resource "aws_lambda_function" "trigger" {
  function_name    = "pyspark-stack-trigger-emr"
  filename         = data.archive_file.trigger_emr.output_path
  source_code_hash = data.archive_file.trigger_emr.output_base64sha256
  handler          = "trigger_emr.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.trigger.arn
  timeout          = 60
  environment {
    variables = {
      EMR_APP_ID     = aws_emrserverless_application.spark.id
      EMR_ROLE_ARN   = aws_iam_role.emr_exec.arn
      DEFAULT_SCRIPT = "s3://${aws_s3_bucket.artifacts.id}/scripts/customer_etl.py"
      LOG_URI        = "s3://${aws_s3_bucket.artifacts.id}/logs/"
    }
  }
}

# --- Opción 1: por CRON (EventBridge Scheduler) ---
data "aws_iam_policy_document" "sched_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "sched" {
  name               = "pyspark-stack-emr-scheduler"
  assume_role_policy = data.aws_iam_policy_document.sched_assume.json
}
resource "aws_iam_role_policy" "sched" {
  name = "invoke"
  role = aws_iam_role.sched.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "lambda:InvokeFunction", Resource = aws_lambda_function.trigger.arn }] })
}
resource "aws_scheduler_schedule" "daily_etl" {
  name                         = "pyspark-stack-daily-etl"
  schedule_expression          = "cron(0 6 * * ? *)"   # 06:00 UTC diario
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.trigger.arn
    role_arn = aws_iam_role.sched.arn
    input    = jsonencode({ script = "s3://REEMPLAZA/scripts/customer_etl.py" })
  }
}

# --- Opción 2: por EVENTO (llega un archivo nuevo al data lake) ---
resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.datalake.arn
}
resource "aws_s3_bucket_notification" "on_upload" {
  bucket = aws_s3_bucket.datalake.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"      # dispara el ETL cuando entra algo a raw/
  }
  depends_on = [aws_lambda_permission.s3_invoke]
}
```

> **Esto es orquestación event-driven pura:** o corre por cron, o **automáticamente cuando llega
> un archivo** a `raw/`. Sin MWAA, sin VPC, sin NAT. Para pipelines con dependencias complejas
> entre tasks, subí a **Step Functions** (state machine) manteniendo el mismo esquema de costo.

---

## 8. Operación, seguridad y ahorro

**Operación (cheat-sheet):**

```bash
# Arquitectura A — la Lambda prende/apaga sola. Manual:
aws ec2 stop-instances  --instance-ids $(cd infra/a-ec2 && terraform output -raw instance_id)
aws lambda invoke --function-name pyspark-stack-startstop \
  --payload '{"action":"start"}' /dev/stdout

# Arquitectura B — EMR se auto-apaga (idle 15m). Apagar MWAA en dev para no pagar el piso:
cd infra/b-serverless && terraform destroy -target=aws_mwaa_environment.airflow
# recrear: terraform apply -target=aws_mwaa_environment.airflow

# Disparar un job serverless a mano (variante §7):
aws lambda invoke --function-name pyspark-stack-trigger-emr /dev/stdout

# Teardown total
terraform destroy
```

**Seguridad (checklist):**
- [ ] Buckets con `public_access_block` y cifrado (ya en el TF).
- [ ] SG de EC2: solo puerto 22 desde tu IP; UIs por túnel.
- [ ] IAM least-privilege: la Lambda de start/stop **solo** actúa sobre instancias con el tag
      (condición ya puesta); endurecé los `*` de Glue/EMR a ARNs concretos.
- [ ] MWAA `PRIVATE_ONLY` + SSM/bastión en entornos sensibles.
- [ ] Secrets de conexiones en **AWS Secrets Manager** (MWAA lo integra), no en texto plano.
- [ ] `.env`, `terraform.tfvars` y `*.zip` de Lambdas en `.gitignore`.

**Palancas de ahorro (orden de impacto):**
1. **Arquitectura B + §7 (EventBridge/Lambda)** en vez de MWAA → de ~$175 a ~$8–15/mes.
2. **Auto start/stop de la EC2** (§5.4) → de ~$300 a ~$70 (u ~$17 con dev-lite).
3. **EMR Serverless sin `initial_capacity` + auto_stop** (ya aplicado): cero costo en reposo.
4. **Graviton (ARM64)** en EMR (ya aplicado): ~20% más barato.
5. **1 NAT + S3 gateway endpoint gratis** (ya aplicado).
6. **S3 lifecycle a IA/Glacier + Parquet particionado** (ya aplicado): menos datos escaneados.

### Resumen: qué reemplaza a qué

| Local (compose) | Arquitectura A (EC2) | Arquitectura B (Serverless) |
|---|---|---|
| HDFS | (dentro del contenedor) | S3 + Glue Catalog |
| Spark master/worker | contenedores en EC2 | EMR Serverless (pago/seg) |
| Airflow (5 svcs) + Postgres | contenedores en EC2 | MWAA **o** EventBridge+Lambda |
| Jupyter | contenedor en EC2 | EMR Studio (opcional) |
| Encendido | EC2 24/7 (o apagable) | **cero en reposo** |
| Costo típico | ~$70–300/mes | ~$8–175/mes |

---

## 9. Notebooks: dónde viven y cómo se ejecutan

Dos usos distintos del mismo `.ipynb`:

| Uso | Cómo | Dónde |
|---|---|---|
| **Explorar / desarrollar** | Interactivo en JupyterLab | `./notebooks` (montado en `/opt/notebooks`) |
| **Ejecutar programado** (parte de un pipeline) | **papermill** disparado por un DAG | el mismo `./notebooks`, output a `./spark-apps/notebook-output` |

**Regla:** los notebooks se guardan en **`./notebooks`** (versionados en git). Para ejecutarlos
de forma automática (no a mano en Jupyter), se usa **papermill**, que inyecta parámetros y corre
el notebook de punta a punta desde un DAG de Airflow.

### 9.1 Habilitar papermill

Agregá el provider a `requirements.txt` (se instala en la imagen de Airflow):

```text
apache-airflow-providers-papermill==3.9.1
```

Y montá `./notebooks` en el contenedor donde corren las tasks (con LocalExecutor es el
`scheduler`). En `docker-compose.prod.yml`:

```yaml
  airflow-scheduler:
    volumes:
      - ./notebooks:/opt/notebooks   # papermill lee los .ipynb desde aquí
```

### 9.2 Parametrizar el notebook

En Jupyter, marcá **una celda con el tag `parameters`** (View → Cell Toolbar → Tags) con las
variables que querés inyectar:

```python
# celda tagueada 'parameters'
run_date = "2026-01-01"
```

### 9.3 DAG que ejecuta el notebook — `dags/run_notebook_dag.py`

```python
from datetime import datetime

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

with DAG(
    dag_id="run_notebook_example",
    schedule=None,  # manual o disparado por otro DAG/Lambda
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["notebook", "papermill"],
) as dag:
    PapermillOperator(
        task_id="run_analysis_notebook",
        input_nb="/opt/notebooks/analysis.ipynb",
        output_nb="/opt/spark-apps/notebook-output/analysis_{{ ds }}.ipynb",
        parameters={"run_date": "{{ ds }}"},  # inyecta la fecha de ejecución
    )
```

papermill guarda una **copia ejecutada con outputs** en `notebook-output/` (queda como
evidencia/auditoría de cada corrida).

---

## 10. Flujo local → servidor → los DAGs corren solos

El objetivo: **editás local, hacés `git push`, y los DAGs aparecen y corren en la EC2** sin pasos
manuales.

```
laptop (edita dags/, spark-apps/, notebooks/)
   │ git push a main
   ▼
GitHub Actions (CI valida → Deploy)
   │ aws s3 sync  →  s3://artifacts/deploy/*
   ▼
SSM SendCommand  →  EC2: aws s3 sync  →  ./dags ./spark-apps ./notebooks
   ▼
Airflow dag-processor detecta los DAGs nuevos (escaneo ~30s)
   │  (no quedan en pausa)  →  corren por su schedule
   ▼
Spark ejecuta · escribe a s3a://datalake/curated
```

**Por qué “corren solos”:** el `dag-processor` de Airflow 3 re-escanea `./dags` cada ~30s, así
que un archivo nuevo aparece sin reiniciar nada. Y con esta variable los DAGs **no quedan en
pausa** al aparecer, por lo que arrancan según su `schedule`:

```yaml
# docker-compose.prod.yml  (env de los servicios airflow)
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "False"
```

> Para forzar una corrida inmediata (además del schedule), el paso de deploy puede terminar con
> un `airflow dags trigger <dag>` vía SSM, o usar la Lambda `trigger-airflow` de la
> [guía de arquitectura](03-arquitectura.md).

### 10.1 Modo rápido (dev): deploy manual sin esperar CI

Para iterar rápido mientras desarrollás, un script que sube directo por `rsync`:

```bash
#!/usr/bin/env bash
# scripts/deploy.sh — deploy rápido a la EC2 (dev). Uso: ./scripts/deploy.sh
set -euo pipefail
IP=$(cd infra/prod && terraform output -raw public_ip)
KEY=~/.ssh/pyspark_stack

rsync -avz --delete \
  --exclude '__pycache__' \
  -e "ssh -i $KEY" \
  dags/ spark-apps/ notebooks/ \
  ec2-user@"$IP":/home/ec2-user/pyspark_stack/

echo "✔ deploy hecho — Airflow detectará los DAGs en ~30s"
```

---

## 11. CI/CD con GitHub Actions (OIDC, sin claves)

Dos workflows: **CI** (valida en cada PR) y **Deploy** (al mergear a `main`). GitHub Actions
asume un rol IAM vía **OIDC** — sin access keys guardadas en el repo.

### 11.1 Terraform: OIDC provider + rol — `infra/prod/cicd.tf`

```hcl
# GitHub Actions asume este rol vía OIDC. Puede: subir a S3, disparar el pull en la EC2 (SSM)
# y correr `terraform plan` (ReadOnly + state). El `apply` queda manual/local.

variable "github_repo" {
  description = "Repo autorizado, formato 'org/repo'"
  type        = string
  default     = "tu-usuario/pyspark_stack"
}

data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com/.well-known/openid-configuration"
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.github.certificates[0].sha1_fingerprint]
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"] # solo tu repo puede asumirlo
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "${var.name_prefix}-github-actions"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

data "aws_iam_policy_document" "github_deploy" {
  statement {
    sid       = "S3DeployArtifacts"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    sid     = "SsmDeploy"
    actions = ["ssm:SendCommand"]
    resources = [
      "arn:aws:ec2:${local.region}:${local.account_id}:instance/${aws_instance.pyspark.id}",
      "arn:aws:ssm:${local.region}::document/AWS-RunShellScript",
    ]
  }
  statement {
    sid       = "SsmResult"
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations"]
    resources = ["*"]
  }
  statement {
    sid       = "TfState"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = ["arn:aws:s3:::*tfstate*", "arn:aws:s3:::*tfstate*/*"]
  }
  statement {
    sid       = "TfLock"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
    resources = ["arn:aws:dynamodb:${local.region}:${local.account_id}:table/*tf-lock*"]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  name   = "deploy-policy"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.github_deploy.json
}

# ReadOnly para que `terraform plan` lea el estado real en CI.
resource "aws_iam_role_policy_attachment" "github_readonly" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions.arn
}
```

> Necesitás el provider `tls` en `providers.tf`:
> ```hcl
> tls = { source = "hashicorp/tls", version = "~> 4.0" }
> ```

Tras `terraform apply`, copiá el output `github_actions_role_arn` y guardalo como **secret**
`AWS_ROLE_ARN` en GitHub (Settings → Secrets and variables → Actions).

### 11.2 Workflow de CI — `.github/workflows/ci.yml`

```yaml
name: CI
on:
  pull_request:
  push:
    branches: [main]

jobs:
  lint-dags:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install ruff
      - name: Ruff (estilo/errores)
        run: ruff check dags/ spark-apps/ || true
      - name: Compilar DAGs (sintaxis)
        run: python -m compileall -q dags/

  terraform:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9.0"
      - run: terraform -chdir=infra/prod fmt -check -recursive
      - name: Init (sin backend) + validate
        run: |
          terraform -chdir=infra/prod init -backend=false -input=false
          terraform -chdir=infra/prod validate
```

### 11.3 Workflow de Deploy — `.github/workflows/deploy.yml`

```yaml
name: Deploy
on:
  push:
    branches: [main]
    paths:
      - "dags/**"
      - "spark-apps/**"
      - "notebooks/**"
      - "monitoring/**"
      - "docker-compose*.yml"

permissions:
  id-token: write   # requerido para OIDC
  contents: read

env:
  AWS_REGION: us-east-1
  INSTANCE_TAG_NAME: pyspark-stack-node
  PROJECT_DIR: /home/ec2-user/pyspark_stack

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Autenticar en AWS (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Resolver bucket e instancia
        id: res
        run: |
          ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
          echo "bucket=pyspark-stack-artifacts-$ACCOUNT" >> "$GITHUB_OUTPUT"
          INSTANCE=$(aws ec2 describe-instances \
            --filters "Name=tag:Name,Values=${INSTANCE_TAG_NAME}" \
                      "Name=instance-state-name,Values=running" \
            --query 'Reservations[0].Instances[0].InstanceId' --output text)
          echo "instance=$INSTANCE" >> "$GITHUB_OUTPUT"

      - name: Sync a S3 (fuente de verdad del deploy)
        run: |
          B=${{ steps.res.outputs.bucket }}
          aws s3 sync dags/       s3://$B/deploy/dags/       --delete --exclude '__pycache__/*'
          aws s3 sync spark-apps/ s3://$B/deploy/spark-apps/ --delete
          aws s3 sync notebooks/  s3://$B/deploy/notebooks/  --delete

      - name: Pull en la EC2 (si está encendida) vía SSM
        run: |
          B=${{ steps.res.outputs.bucket }}
          I=${{ steps.res.outputs.instance }}
          if [ "$I" = "None" ] || [ -z "$I" ]; then
            echo "EC2 apagada: el deploy quedó en S3; se aplicará al próximo arranque."
            exit 0
          fi
          CMD=$(aws ssm send-command --instance-ids "$I" \
            --document-name AWS-RunShellScript --comment "deploy from GitHub Actions" \
            --parameters commands="[\
              \"cd ${PROJECT_DIR}\",\
              \"aws s3 sync s3://$B/deploy/dags/ dags/ --delete\",\
              \"aws s3 sync s3://$B/deploy/spark-apps/ spark-apps/ --delete\",\
              \"aws s3 sync s3://$B/deploy/notebooks/ notebooks/ --delete\"\
            ]" --query 'Command.CommandId' --output text)
          aws ssm wait command-executed --command-id "$CMD" --instance-id "$I" || true
          aws ssm get-command-invocation --command-id "$CMD" --instance-id "$I" \
            --query 'StandardOutputContent' --output text
```

### 11.4 Puesta en marcha (una vez)

1. `terraform apply` con `cicd.tf` → copiá `github_actions_role_arn`.
2. GitHub → Settings → Secrets → Actions → crear **`AWS_ROLE_ARN`** con ese ARN.
3. Ajustá `github_repo` en `cicd.tf` a tu `org/repo`.
4. `git push` a `main` → el workflow **Deploy** sube el código y la EC2 lo baja; Airflow corre
   los DAGs. Los PRs disparan **CI** (lint + terraform validate).

> **Seguridad:** el rol solo lo puede asumir tu repo (condición `sub = repo:org/repo:*`), no hay
> claves de larga vida, y el deploy usa **SSM** (no expone SSH en CI). El `terraform apply` queda
> fuera de CI (manual/local) para no dar permisos amplios a Actions.

---

## 12. Monitoreo (Prometheus + Grafana + Alertmanager + Loki)

Observabilidad completa corriendo **dentro de la EC2** junto al `docker-compose`: métricas +
alertas + **logs centralizados**. Cada bloque indica su ruta en la 1ª línea; creá esos archivos.

### 12.1 Qué se monitorea y análisis de completitud

| Señal | Exporter / fuente | Puerto interno |
|---|---|---|
| Host (CPU, RAM, disco, red) | `node-exporter` | 9100 |
| Contenedores (uso por servicio) | `cAdvisor` | 8080 |
| Airflow (DAGs, tasks, duraciones) | Airflow StatsD → `statsd-exporter` | 9102 |
| Spark master/worker/driver | PrometheusServlet nativo de Spark 4 | 8080 / 8081 / 4040 |
| Logs de todos los contenedores | `Promtail` → `Loki` | 3100 |
| Alertas | `Alertmanager` → email | 9093 |
| Dashboards | `Grafana` | 3000 |

**Auditoría de completitud** (qué está y qué se corrigió):

| Área | Estado | Nota |
|---|---|---|
| Host / contenedores | ✅ | node-exporter + cAdvisor |
| Airflow / Spark | ✅ | StatsD + PrometheusServlet |
| Logs centralizados | ✅ agregado | **Loki + Promtail** (era el gran faltante) |
| Alertas → notificación | ✅ | Alertmanager (antes las reglas no notificaban) |
| HDFS métricas ricas | ⚠️ parcial | `/jmx` devuelve JSON no parseable → scrape quitado; recursos vía cAdvisor; métricas ricas con jmx_exporter (§12.8) |
| Panel "Spark workers" | ✅ corregido | usa `up{job="spark-master"}` (robusto) en vez de una métrica de nombre incierto |

### 12.2 Estructura de archivos (`monitoring/`)

```
monitoring/
├── prometheus/{prometheus.yml, alerts.yml}
├── alertmanager/alertmanager.yml
├── statsd/statsd_mapping.yml
├── spark/metrics.properties
├── loki/loki-config.yml
├── promtail/promtail-config.yml
└── grafana/
    ├── provisioning/{datasources/datasources.yml, dashboards/dashboards.yml}
    └── dashboards/overview.json
```

### 12.3 Servicios de monitoreo (compose)

Se agregan al `docker-compose.prod.yml`. Incluyen `restart`, límites de memoria y rotación de logs.

```yaml
# docker-compose.prod.yml  (bloque MONITOREO)
x-mon-logging: &mon-logging
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }

services:
  prometheus:
    image: prom/prometheus:v2.54.1
    container_name: prometheus
    restart: unless-stopped
    <<: *mon-logging
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=15d
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./monitoring/prometheus/alerts.yml:/etc/prometheus/alerts.yml:ro
      - /data/prometheus:/prometheus
    ports: ["9090:9090"]
    deploy: { resources: { limits: { memory: 1g } } }
    networks: [hadoopnet]

  alertmanager:
    image: prom/alertmanager:v0.27.0
    container_name: alertmanager
    restart: unless-stopped
    <<: *mon-logging
    command: [--config.file=/etc/alertmanager/alertmanager.yml]
    volumes:
      - ./monitoring/alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
    ports: ["9093:9093"]
    networks: [hadoopnet]

  grafana:
    image: grafana/grafana:11.2.0
    container_name: grafana
    restart: unless-stopped
    <<: *mon-logging
    depends_on: [prometheus]
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:?definí GRAFANA_ADMIN_PASSWORD en .env}
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - /data/grafana:/var/lib/grafana
    ports: ["3000:3000"]
    networks: [hadoopnet]

  node-exporter:
    image: prom/node-exporter:v1.8.2
    container_name: node-exporter
    restart: unless-stopped
    <<: *mon-logging
    command:
      - --path.rootfs=/host
      - --collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)
    pid: host
    volumes: ["/:/host:ro,rslave"]
    networks: [hadoopnet]

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.1
    container_name: cadvisor
    restart: unless-stopped
    <<: *mon-logging
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /dev/disk/:/dev/disk:ro
    networks: [hadoopnet]

  statsd-exporter:
    image: prom/statsd-exporter:v0.27.1
    container_name: statsd-exporter
    restart: unless-stopped
    <<: *mon-logging
    command:
      - --statsd.mapping-config=/etc/statsd/statsd_mapping.yml
      - --statsd.listen-udp=:9125
      - --web.listen-address=:9102
    volumes:
      - ./monitoring/statsd/statsd_mapping.yml:/etc/statsd/statsd_mapping.yml:ro
    networks: [hadoopnet]

  loki:
    image: grafana/loki:3.1.1
    container_name: loki
    restart: unless-stopped
    <<: *mon-logging
    command: [-config.file=/etc/loki/loki-config.yml]
    volumes:
      - ./monitoring/loki/loki-config.yml:/etc/loki/loki-config.yml:ro
      - /data/loki:/loki
    ports: ["3100:3100"]
    networks: [hadoopnet]

  promtail:
    image: grafana/promtail:3.1.1
    container_name: promtail
    restart: unless-stopped
    <<: *mon-logging
    command: [-config.file=/etc/promtail/promtail-config.yml]
    volumes:
      - ./monitoring/promtail/promtail-config.yml:/etc/promtail/promtail-config.yml:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks: [hadoopnet]
```

Para que Airflow **emita** métricas StatsD, en los servicios `airflow-*` del override:

```yaml
# docker-compose.prod.yml
x-airflow-metrics: &airflow-metrics
  AIRFLOW__METRICS__STATSD_ON: "True"
  AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
  AIRFLOW__METRICS__STATSD_PORT: "9125"
  AIRFLOW__METRICS__STATSD_PREFIX: airflow

services:
  airflow-apiserver:     { environment: { <<: *airflow-metrics } }
  airflow-scheduler:     { environment: { <<: *airflow-metrics } }
  airflow-dag-processor: { environment: { <<: *airflow-metrics } }
  airflow-triggerer:     { environment: { <<: *airflow-metrics } }
```

### 12.4 Prometheus + alertas + Alertmanager

```yaml
# monitoring/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
rule_files:
  - /etc/prometheus/alerts.yml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]
scrape_configs:
  - job_name: prometheus
    static_configs: [{ targets: ["localhost:9090"] }]
  - job_name: node
    static_configs: [{ targets: ["node-exporter:9100"] }]
  - job_name: cadvisor
    static_configs: [{ targets: ["cadvisor:8080"] }]
  - job_name: airflow
    static_configs: [{ targets: ["statsd-exporter:9102"] }]
  - job_name: spark-master
    metrics_path: /metrics/master/prometheus
    static_configs: [{ targets: ["spark-master:8080"] }]
  - job_name: spark-worker
    metrics_path: /metrics/prometheus
    static_configs: [{ targets: ["spark-worker:8081"] }]
  # HDFS NO se scrapea: /jmx devuelve JSON no parseable. Ver §12.8 (jmx_exporter).
```

```yaml
# monitoring/prometheus/alerts.yml
groups:
  - name: pyspark-stack
    rules:
      - alert: TargetDown
        expr: up == 0
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "Target {{ $labels.job }} caído"
          description: "{{ $labels.instance }} ({{ $labels.job }}) no responde hace 2m."
      - alert: HostLowMemory
        expr: (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100 < 8
        for: 5m
        labels: { severity: warning }
        annotations: { summary: "Memoria baja en el host", description: "Disponible < 8% hace 5m." }
      - alert: HostDiskAlmostFull
        expr: (node_filesystem_avail_bytes{mountpoint="/data"} / node_filesystem_size_bytes{mountpoint="/data"}) * 100 < 10
        for: 5m
        labels: { severity: critical }
        annotations: { summary: "Disco /data casi lleno", description: "Menos del 10% libre en /data." }
      - alert: AirflowTasksFailing
        expr: increase(airflow_ti_failures[10m]) > 0
        for: 1m
        labels: { severity: warning }
        annotations: { summary: "Tasks de Airflow fallando", description: "Fallos en los últimos 10m." }
```

> ⚠️ Alertmanager **no** expande env vars: el password va literal → este archivo **no** debe ir a git.

```yaml
# monitoring/alertmanager/alertmanager.yml
global:
  resolve_timeout: 5m
  smtp_smarthost: "smtp.gmail.com:587"
  smtp_from: "abantodca@gmail.com"
  smtp_auth_username: "abantodca@gmail.com"
  smtp_auth_password: "REEMPLAZA_CON_APP_PASSWORD"   # https://myaccount.google.com/apppasswords
  smtp_require_tls: true
route:
  receiver: email
  group_by: ["alertname"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 3h
  routes:
    - matchers: ['severity="critical"']
      receiver: email
      repeat_interval: 1h
receivers:
  - name: email
    email_configs:
      - to: "abantodca@gmail.com"
        send_resolved: true
inhibit_rules:
  - source_matchers: ['severity="critical"']
    target_matchers: ['severity="warning"']
    equal: ["instance"]
```

### 12.5 Métricas de Airflow y Spark

```yaml
# monitoring/statsd/statsd_mapping.yml
mappings:
  - match: "airflow.dag.*.*.duration"
    name: "airflow_task_duration"
    labels: { dag_id: "$1", task_id: "$2" }
  - match: "airflow.dagrun.duration.success.*"
    name: "airflow_dagrun_duration_success"
    labels: { dag_id: "$1" }
  - match: "airflow.dagrun.duration.failed.*"
    name: "airflow_dagrun_duration_failed"
    labels: { dag_id: "$1" }
  - match: "airflow.*"
    name: "airflow_$1"
```

```properties
# monitoring/spark/metrics.properties  (montar en /opt/spark/conf/ de master y worker)
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
*.source.jvm.class=org.apache.spark.metrics.source.JvmSource
```

En el override, montá `metrics.properties` en los servicios Spark:

```yaml
# docker-compose.prod.yml
services:
  spark-master:
    volumes:
      - ./monitoring/spark/metrics.properties:/opt/spark/conf/metrics.properties:ro
  spark-worker:
    volumes:
      - ./monitoring/spark/metrics.properties:/opt/spark/conf/metrics.properties:ro
```

### 12.6 Grafana (datasources + dashboard)

```yaml
# monitoring/grafana/provisioning/datasources/datasources.yml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
  - name: Loki
    type: loki
    access: proxy
    url: http://loki:3100
```

```yaml
# monitoring/grafana/provisioning/dashboards/dashboards.yml
apiVersion: 1
providers:
  - name: pyspark-stack
    orgId: 1
    folder: ""
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

```json
// monitoring/grafana/dashboards/overview.json
{
  "title": "pyspark-stack — Overview",
  "uid": "pyspark-overview",
  "schemaVersion": 39,
  "tags": ["pyspark-stack"],
  "time": { "from": "now-6h", "to": "now" },
  "panels": [
    { "type": "timeseries", "title": "CPU host (%)", "gridPos": {"h":8,"w":12,"x":0,"y":0},
      "targets": [{ "expr": "100 - (avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)" }] },
    { "type": "timeseries", "title": "Memoria usada host (%)", "gridPos": {"h":8,"w":12,"x":12,"y":0},
      "targets": [{ "expr": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100" }] },
    { "type": "gauge", "title": "Disco /data usado (%)", "gridPos": {"h":8,"w":6,"x":0,"y":8},
      "fieldConfig": { "defaults": { "max": 100, "min": 0, "unit": "percent" } },
      "targets": [{ "expr": "(1 - (node_filesystem_avail_bytes{mountpoint=\"/data\"} / node_filesystem_size_bytes{mountpoint=\"/data\"})) * 100" }] },
    { "type": "timeseries", "title": "Memoria por contenedor", "gridPos": {"h":8,"w":18,"x":6,"y":8},
      "targets": [{ "expr": "sum by (name) (container_memory_usage_bytes{name!=\"\"})", "legendFormat": "{{name}}" }] },
    { "type": "timeseries", "title": "Airflow — fallos de tasks (10m)", "gridPos": {"h":8,"w":12,"x":0,"y":16},
      "targets": [{ "expr": "increase(airflow_ti_failures[10m])" }] },
    { "type": "stat", "title": "Spark master arriba", "gridPos": {"h":8,"w":12,"x":12,"y":16},
      "targets": [{ "expr": "up{job=\"spark-master\"}" }] }
  ],
  "templating": { "list": [] }
}
```

> Para paneles ricos, importá desde la UI los dashboards de la comunidad: **1860** (Node Exporter
> Full), y uno de **Airflow** / **cAdvisor**.

### 12.7 Logs: Loki + Promtail

Promtail recolecta los logs de **todos los contenedores** y los manda a Loki. En Grafana →
Explore → datasource **Loki**: `{container="airflow-scheduler"}`.

```yaml
# monitoring/loki/loki-config.yml
auth_enabled: false
server:
  http_listen_port: 3100
common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore: { store: inmemory }
schema_config:
  configs:
    - from: 2020-10-24
      store: tsdb
      object_store: filesystem
      schema: v13
      index: { prefix: index_, period: 24h }
limits_config:
  retention_period: 168h
  reject_old_samples: true
  reject_old_samples_max_age: 168h
```

```yaml
# monitoring/promtail/promtail-config.yml
server:
  http_listen_port: 9080
positions:
  filename: /tmp/positions.yaml
clients:
  - url: http://loki:3100/loki/api/v1/push
scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 15s
    relabel_configs:
      - source_labels: ["__meta_docker_container_name"]
        regex: "/(.*)"
        target_label: container
      - source_labels: ["__meta_docker_container_log_stream"]
        target_label: stream
```

### 12.8 Acceso, verificación y HDFS

Acceso por **túnel SSH** (nada expuesto):

```bash
ssh -i ~/.ssh/pyspark_stack -L 3000:localhost:3000 -L 9090:localhost:9090 -L 9093:localhost:9093 ec2-user@$IP
# Grafana localhost:3000 · Prometheus localhost:9090 · Alertmanager localhost:9093
```

Verificar: en `http://localhost:9090/targets` todos deben estar **UP** (node, cadvisor, airflow,
spark-master, spark-worker). Levantar: `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`.

**HDFS con jmx_exporter (enhancement):** el `/jmx` del namenode es JSON no parseable. Para
métricas ricas, corré el JMX Exporter como *javaagent* en una imagen HDFS propia:

```bash
HDFS_NAMENODE_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=7071:/opt/jmx/hdfs.yml"
```
```yaml
# y agregar a prometheus.yml:
  - job_name: hdfs-namenode
    static_configs: [{ targets: ["hdfs-namenode:7071"] }]
```

---

## 13. Hardening de producción

Ajustes que un experto DevOps exige antes de considerar el stack production-ready (surgidos de
la auditoría). Todo copy-paste.

### 13.1 Secretos y parámetros con AWS (Parameter Store + Secrets Manager)

El compose base trae secretos hardcodeados (`POSTGRES_PASSWORD=airflow`, JWT `supersecretjwtkey`,
admin/admin, Jupyter sin token). En vez de un `.env` en texto plano, se **generan y guardan en AWS
SSM Parameter Store** (SecureString, cifrado con KMS); la EC2 los lee con su rol IAM y los
materializa en un `.env` efímero (chmod 600) antes de `docker compose up`. Cero secretos en git.

> **SSM Parameter Store vs Secrets Manager:** Parameter Store SecureString es **gratis** (tier
> estándar) y alcanza para esto. Secrets Manager (~$0.40/secreto/mes) suma rotación automática;
> si la necesitás, cambiá `aws_ssm_parameter` por `aws_secretsmanager_secret`.

**Terraform — `infra/prod/secrets.tf` (genera y guarda los parámetros):**

```hcl
resource "random_password" "postgres" { length = 24, special = false }
resource "random_password" "jwt"      { length = 48, special = false }
resource "random_password" "admin"    { length = 20 }
resource "random_password" "jupyter"  { length = 32, special = false }
resource "random_password" "grafana"  { length = 20 }

locals {
  secrets = {
    postgres_password      = random_password.postgres.result
    airflow_jwt_secret     = random_password.jwt.result
    airflow_admin_password = random_password.admin.result
    jupyter_token          = random_password.jupyter.result
    grafana_admin_password = random_password.grafana.result
    # el SMTP de Alertmanager lo cargás a mano una vez (no lo genera Terraform):
    # aws ssm put-parameter --name /pyspark-stack/smtp_password --type SecureString --value 'APP_PASSWORD'
  }
}

resource "aws_ssm_parameter" "secret" {
  for_each = local.secrets
  name     = "/${var.name_prefix}/${each.key}"
  type     = "SecureString"
  value    = each.value
  # Nota: los valores quedan en el state (por eso el backend S3 va cifrado).
}
```

**IAM — permitir a la EC2 leer los parámetros (agregar a `infra/prod/iam.tf`):**

```hcl
# dentro de data.aws_iam_policy_document "ec2"
statement {
  sid       = "SsmReadParams"
  actions   = ["ssm:GetParameter", "ssm:GetParametersByPath"]
  resources = ["arn:aws:ssm:${local.region}:${local.account_id}:parameter/${var.name_prefix}/*"]
}
statement {
  sid       = "KmsDecrypt"
  actions   = ["kms:Decrypt"]
  resources = ["*"] # o la ARN de la KMS key de SSM
}
```

**Script que materializa el `.env` desde SSM — `scripts/load-secrets.sh` (corre en la EC2):**

```bash
#!/usr/bin/env bash
# Genera un .env efímero desde SSM antes de levantar el stack.
set -euo pipefail
PREFIX="/pyspark-stack"
REGION="${AWS_REGION:-us-east-1}"
get() { aws ssm get-parameter --name "$PREFIX/$1" --with-decryption \
          --query Parameter.Value --output text --region "$REGION"; }

cat > .env <<EOF
POSTGRES_USER=airflow
POSTGRES_DB=airflow
POSTGRES_PASSWORD=$(get postgres_password)
AIRFLOW_JWT_SECRET=$(get airflow_jwt_secret)
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$(get airflow_admin_password)
JUPYTER_TOKEN=$(get jupyter_token)
GRAFANA_ADMIN_PASSWORD=$(get grafana_admin_password)
EOF
chmod 600 .env
echo "✔ .env generado desde SSM"
```

Uso en la EC2: `./scripts/load-secrets.sh && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`.

**Parametrizá el compose base para que lea esas variables:**

```yaml
# docker-compose.yml (fragmentos)
  airflow-db:
    environment:
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - POSTGRES_DB=${POSTGRES_DB}

x-airflow-common: &airflow-common
  environment: &airflow-common-env
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: >-
      postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@airflow-db:5432/${POSTGRES_DB}
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
```

**Valores delicados / claves con rotación → AWS Secrets Manager.** Para lo más sensible (el
**JWT secret key** de Airflow, credenciales de bases externas, API keys de terceros) usá
**Secrets Manager**, que agrega **rotación automática** y auditoría. Parameter Store queda para el
resto (gratis).

```hcl
# infra/prod/secrets.tf — secretos delicados en Secrets Manager
resource "aws_secretsmanager_secret" "airflow_jwt" {
  name                    = "${var.name_prefix}/airflow_jwt_secret"
  recovery_window_in_days = 7
}
resource "aws_secretsmanager_secret_version" "airflow_jwt" {
  secret_id     = aws_secretsmanager_secret.airflow_jwt.id
  secret_string = random_password.jwt.result
}
# (Opcional) rotación automática cada 30 días con una Lambda de rotación:
# resource "aws_secretsmanager_secret_rotation" "airflow_jwt" {
#   secret_id           = aws_secretsmanager_secret.airflow_jwt.id
#   rotation_lambda_arn = aws_lambda_function.rotator.arn
#   rotation_rules { automatically_after_days = 30 }
# }
```

IAM (agregar al rol de la EC2, junto a los permisos de SSM):

```hcl
statement {
  sid       = "SecretsRead"
  actions   = ["secretsmanager:GetSecretValue"]
  resources = ["arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:${var.name_prefix}/*"]
}
```

Leerlo en `load-secrets.sh` (en vez de SSM para ese valor):

```bash
AIRFLOW_JWT_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id pyspark-stack/airflow_jwt_secret --query SecretString --output text --region "$REGION")
```

**Cuándo usar cada uno:**

| Servicio | Usar para | Costo |
|---|---|---|
| **Parameter Store** (SecureString) | passwords operacionales, tokens, config | gratis (tier estándar) |
| **Secrets Manager** | claves delicadas con **rotación**, credenciales de terceros | ~$0.40/secreto/mes |

**Alertmanager (SMTP password):** guardalo en Secrets Manager (o SSM) y renderizá el
`alertmanager.yml` en el arranque con `envsubst` tomando el valor con
`aws secretsmanager get-secret-value ...`, así el password nunca vive en un archivo versionado.

> **Airflow Connections/Variables desde SSM:** además de los secretos del stack, Airflow puede
> leer sus *Connections* y *Variables* directo de Parameter Store con el
> `SystemsManagerParameterStoreBackend` (provider `apache-airflow-providers-amazon`), sin
> guardarlas en la metadata DB. Config: `AIRFLOW__SECRETS__BACKEND` +
> `AIRFLOW__SECRETS__BACKEND_KWARGS` apuntando al prefijo `/pyspark-stack/`.

### 13.2 restart + límites + logging en los servicios del stack

Los servicios core (`hdfs-*`, `spark-*`, `jupyter`, `airflow-db`) no traen `restart` ni límites.
Agregalos en el override de prod:

```yaml
# docker-compose.prod.yml
x-hard: &hard
  restart: unless-stopped
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }

services:
  hdfs-namenode: { <<: *hard, deploy: { resources: { limits: { memory: 2g } } } }
  hdfs-datanode: { <<: *hard, deploy: { resources: { limits: { memory: 2g } } } }
  spark-master:  { <<: *hard, deploy: { resources: { limits: { memory: 2g } } } }
  spark-worker:  { <<: *hard, deploy: { resources: { limits: { memory: 6g } } } }
  jupyter:       { <<: *hard, deploy: { resources: { limits: { memory: 2g } } } }
  airflow-db:    { <<: *hard }
```

### 13.3 Spark ↔ S3 con el rol IAM (s3a)

Los jars `hadoop-aws` ya están en las imágenes (Dockerfiles). Agregá el credential provider para
que `s3a://` use el rol de la EC2 sin keys. Creá `spark-defaults.conf` y montalo:

```properties
# spark-conf/spark-defaults.conf
spark.hadoop.fs.s3a.aws.credentials.provider  software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider
spark.hadoop.fs.s3a.endpoint.region           us-east-1
```

```yaml
# docker-compose.prod.yml (montar en los 3 que corren Spark)
  spark-master: { volumes: ["./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro"] }
  spark-worker: { volumes: ["./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro"] }
  jupyter:      { volumes: ["./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro"] }
```

### 13.4 `docker.sock` en Airflow

El compose base monta `/var/run/docker.sock` en los 5 Airflow (root del host). Si **no** usás
`DockerOperator`, quitá esa línea del `x-airflow-common`. Si lo necesitás, poné un socket-proxy
(`tecnativa/docker-socket-proxy`) read-only en vez del socket crudo.

### 13.5 Higiene del repo

```gitignore
# .gitignore (raíz)
.env
**/__pycache__/
spark-events/
notebooks/**/output/
infra/**/.terraform/
infra/**/*.tfstate*
infra/**/*.tfvars
monitoring/alertmanager/alertmanager.yml   # tiene el smtp password
```

Creá también un `.env.example` (sin valores reales) y un `README.md` raíz que enlace estas guías
y liste los tres modos de arranque (dev-lite, prod local, prod AWS).

### 13.6 Checklist final (production-ready)

- [ ] `.env` con secretos generados (`openssl rand`), fuera de git.
- [ ] Jupyter con `JUPYTER_TOKEN`; Grafana sin password default.
- [ ] `restart: unless-stopped` + límites de memoria en todos los servicios.
- [ ] Rotación de logs (`max-size`) en todos.
- [ ] `spark-defaults.conf` con el credential provider de s3a.
- [ ] `docker.sock` quitado o detrás de proxy.
- [ ] Monitoreo: targets UP, Alertmanager con SMTP real, Loki recibiendo logs.
- [ ] Backups: snapshot del EBS `/data` (DLM) + versioning de S3.
- [ ] Imágenes pineadas por tag; `terraform apply` solo manual/local.

---

## 14. Archivos compose completos

Los dos archivos listos para copiar. El de producción ya incorpora todo lo explicado por tema en
§12 (monitoreo) y §13 (hardening): persistencia en `/data`, `restart`, límites, logging, s3a,
métricas y logs. Las variables `${...}` las provee el `.env` que genera `load-secrets.sh` desde
SSM (§13.1).

### 14.1 docker-compose.prod.yml (producción, completo)

```yaml
# docker-compose.prod.yml — override de producción (se fusiona con docker-compose.yml).
#   ./scripts/load-secrets.sh   # genera .env desde SSM (§13.1)
#   docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
x-hard: &hard
  restart: unless-stopped
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }

x-airflow-metrics: &airflow-metrics
  AIRFLOW__METRICS__STATSD_ON: "True"
  AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
  AIRFLOW__METRICS__STATSD_PORT: "9125"
  AIRFLOW__METRICS__STATSD_PREFIX: airflow

services:
  # ---- Persistencia en /data (EBS) + restart/límites/logging ----
  airflow-db:
    <<: *hard
    volumes:
      - /data/postgres:/var/lib/postgresql/data

  hdfs-namenode:
    <<: *hard
    deploy: { resources: { limits: { memory: 2g } } }
    volumes:
      - /data/hdfs-nn:/hadoop/dfs/name
  hdfs-datanode:
    <<: *hard
    deploy: { resources: { limits: { memory: 2g } } }
    volumes:
      - /data/hdfs-dn:/hadoop/dfs/data

  # ---- Spark: métricas Prometheus + s3a con rol IAM ----
  spark-master:
    <<: *hard
    deploy: { resources: { limits: { memory: 2g } } }
    volumes:
      - ./monitoring/spark/metrics.properties:/opt/spark/conf/metrics.properties:ro
      - ./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
  spark-worker:
    <<: *hard
    deploy: { resources: { limits: { memory: 6g } } }
    volumes:
      - ./monitoring/spark/metrics.properties:/opt/spark/conf/metrics.properties:ro
      - ./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro

  jupyter:
    <<: *hard
    deploy: { resources: { limits: { memory: 2g } } }
    environment:
      - JUPYTER_TOKEN=${JUPYTER_TOKEN}
    volumes:
      - ./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro

  # ---- Airflow: emitir métricas StatsD ----
  airflow-apiserver:     { environment: { <<: *airflow-metrics } }
  airflow-scheduler:     { environment: { <<: *airflow-metrics } }
  airflow-dag-processor: { environment: { <<: *airflow-metrics } }
  airflow-triggerer:     { environment: { <<: *airflow-metrics } }

  # ==================== MONITOREO (detalle en §12) ====================
  prometheus:
    image: prom/prometheus:v2.54.1
    container_name: prometheus
    <<: *hard
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=15d
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./monitoring/prometheus/alerts.yml:/etc/prometheus/alerts.yml:ro
      - /data/prometheus:/prometheus
    ports: ["9090:9090"]
    deploy: { resources: { limits: { memory: 1g } } }
    networks: [hadoopnet]

  alertmanager:
    image: prom/alertmanager:v0.27.0
    container_name: alertmanager
    <<: *hard
    command: [--config.file=/etc/alertmanager/alertmanager.yml]
    volumes:
      - ./monitoring/alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
    ports: ["9093:9093"]
    networks: [hadoopnet]

  grafana:
    image: grafana/grafana:11.2.0
    container_name: grafana
    <<: *hard
    depends_on: [prometheus]
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:?definí GRAFANA_ADMIN_PASSWORD}
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - /data/grafana:/var/lib/grafana
    ports: ["3000:3000"]
    networks: [hadoopnet]

  node-exporter:
    image: prom/node-exporter:v1.8.2
    container_name: node-exporter
    <<: *hard
    command:
      - --path.rootfs=/host
      - --collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)
    pid: host
    volumes: ["/:/host:ro,rslave"]
    networks: [hadoopnet]

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.1
    container_name: cadvisor
    <<: *hard
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /dev/disk/:/dev/disk:ro
    networks: [hadoopnet]

  statsd-exporter:
    image: prom/statsd-exporter:v0.27.1
    container_name: statsd-exporter
    <<: *hard
    command:
      - --statsd.mapping-config=/etc/statsd/statsd_mapping.yml
      - --statsd.listen-udp=:9125
      - --web.listen-address=:9102
    volumes:
      - ./monitoring/statsd/statsd_mapping.yml:/etc/statsd/statsd_mapping.yml:ro
    networks: [hadoopnet]

  loki:
    image: grafana/loki:3.1.1
    container_name: loki
    <<: *hard
    command: [-config.file=/etc/loki/loki-config.yml]
    volumes:
      - ./monitoring/loki/loki-config.yml:/etc/loki/loki-config.yml:ro
      - /data/loki:/loki
    ports: ["3100:3100"]
    networks: [hadoopnet]

  promtail:
    image: grafana/promtail:3.1.1
    container_name: promtail
    <<: *hard
    command: [-config.file=/etc/promtail/promtail-config.yml]
    volumes:
      - ./monitoring/promtail/promtail-config.yml:/etc/promtail/promtail-config.yml:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks: [hadoopnet]
```

### 14.2 docker-compose.dev.yml (dev-lite 8 GB)

Solo Spark + Jupyter (sin HDFS/Airflow/Postgres) para desarrollar PySpark en una máquina de ~8 GB.
Datos en `./data` local. Uso: `docker compose -f docker-compose.dev.yml up -d --build`.

```yaml
# docker-compose.dev.yml
services:
  spark-master:
    build: { context: ., dockerfile: Dockerfile.spark }
    image: pyspark_stack-spark:4.0.3
    container_name: spark-master-dev
    entrypoint: ["/opt/spark/bin/spark-class"]
    command: ["org.apache.spark.deploy.master.Master", "--host", "spark-master", "--port", "7077", "--webui-port", "8080"]
    ports: ["7077:7077", "8081:8080"]
    volumes:
      - ./spark-apps:/opt/spark-apps
      - ./data:/data
    networks: [devnet]
    deploy: { resources: { limits: { memory: 768m } } }

  spark-worker:
    build: { context: ., dockerfile: Dockerfile.spark }
    image: pyspark_stack-spark:4.0.3
    container_name: spark-worker-dev
    depends_on: [spark-master]
    entrypoint: ["/opt/spark/bin/spark-class"]
    # --memory/--cores acotan lo que el worker OFRECE (clave en 8 GB)
    command: ["org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077", "--memory", "3G", "--cores", "2"]
    volumes:
      - ./spark-apps:/opt/spark-apps
      - ./data:/data
    networks: [devnet]
    deploy: { resources: { limits: { memory: 4g } } }

  jupyter:
    build: { context: ., dockerfile: Dockerfile.jupyter }
    image: pyspark_stack-jupyter:4.0.3
    container_name: jupyter-dev
    ports: ["8888:8888", "4040:4040"]
    depends_on: [spark-master]
    volumes:
      - ./notebooks:/opt/notebooks
      - ./spark-apps:/opt/spark-apps
      - ./data:/data
    networks: [devnet]
    environment:
      - SPARK_MASTER=spark://spark-master:7077
      - PYSPARK_PYTHON=python3.12
      - PYSPARK_DRIVER_PYTHON=python3.12
    deploy: { resources: { limits: { memory: 2500m } } }

networks:
  devnet:

# Presupuesto RAM (~8 GB): master 0.75 + worker 4.0 + jupyter 2.5 = ~7.25 GB (deja ~0.75 GB al host).
# Si vas justo: worker "--memory 2G" y limit 3g.
```
