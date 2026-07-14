# Guía experta — Producción en AWS (Terraform, EC2 self-managed + automatización)

> Guía única de producción para el stack (HDFS/Spark/Jupyter/Airflow). Implementa **una sola
> arquitectura** —la de [`docs/03-arquitectura.md`](03-arquitectura.md)— con Terraform copy-paste,
> estado remoto y **automatización con EventBridge + Lambda** para bajar el costo al mínimo.
>
> **La arquitectura (un solo camino):** el stack completo corre *self-managed* en **una EC2** con
> Docker. Alrededor, servicios AWS lo complementan: **S3** como data lake (`s3a://` con rol IAM),
> **Lambda + EventBridge** para disparar los DAGs (por cron o por evento) y para el *auto
> start/stop* de la EC2, **Prometheus + Grafana + Alertmanager + Loki** para observabilidad, y
> **CI/CD con GitHub Actions (OIDC)**. **No usa MWAA, EMR ni Glue** (ver el porqué en la guía de arquitectura).

Índice:
1. [Panorama de la arquitectura](#1-panorama-de-la-arquitectura)
2. [Costo](#2-costo)
3. [Prerrequisitos](#3-prerrequisitos)
4. [Fundamentos: backend Terraform (S3 + DynamoDB)](#4-fundamentos-backend-terraform)
5. [Núcleo: EC2 con Docker](#5-núcleo-ec2-con-docker)
   - 5.1 [Variables y red (SSH-only)](#51-variables-y-red)
   - 5.2 [IAM + key pair](#52-iam--key-pair)
   - 5.3 [EC2 + EBS + user_data](#53-ec2--ebs--user_data)
   - 5.4 [**Automatización: EventBridge + Lambda (auto start/stop)**](#54-automatización-eventbridge--lambda)
   - 5.5 [Desplegar, subir código y túnel SSH](#55-desplegar-subir-código-y-túnel-ssh)
6. [Data lake en S3 (s3a con rol IAM) + backups](#6-data-lake-en-s3)
   - 6.1 [Buckets S3 (data lake + artifacts)](#61-buckets-s3)
   - 6.2 [IAM: permitir s3a a la EC2 (sin keys)](#62-iam-permitir-s3a-a-la-ec2-sin-keys)
   - 6.3 [Backups: snapshots EBS automáticos (DLM)](#63-backups-snapshots-ebs-automáticos-dlm)
7. [Orquestación: Lambda trigger-airflow (SSM) + EventBridge + event-driven](#7-orquestación-lambda-trigger-airflow-ssm--eventbridge--event-driven)
   - 7.1 [Lambda que dispara los DAGs vía SSM](#71-lambda-que-dispara-los-dags-vía-ssm)
   - 7.2 [Disparo por cron (EventBridge Scheduler)](#72-disparo-por-cron-eventbridge-scheduler)
   - 7.3 [Disparo por evento (archivo nuevo en S3)](#73-disparo-por-evento-archivo-nuevo-en-s3)
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

## 1. Panorama de la arquitectura

Un solo host EC2 corre todo el stack en Docker; AWS *serverless* lo rodea para storage durable
(S3), disparo de DAGs (Lambda + EventBridge) y ahorro (auto start/stop). El detalle conceptual y
los diagramas están en [`docs/03-arquitectura.md`](03-arquitectura.md); esta guía es el **cómo**
(Terraform copy-paste).

**Regla mental:** almacenar es barato y constante; **computar es lo que cuesta, y solo cuando
corrés**. Por eso la EC2 se apaga fuera de horario (auto start/stop) y el data lake vive en S3.

```
                    ┌───────────── EC2 m6i.xlarge (Elastic IP) ──────────────┐
 EventBridge  ──►   │  docker compose:                                        │
  · cron ETL        │   Airflow (5) + Postgres · Spark master/worker ·        │
  · start/stop  ──► │   HDFS namenode/datanode · Jupyter                      │
      │             │   MONITOREO: Prometheus · Grafana · Alertmanager · Loki │
      ▼             └───────┬──────────────────────────────┬─────────────────┘
  Lambda trigger-airflow    │ s3a:// (rol IAM, sin keys)    │ /data (EBS gp3)
  Lambda startstop          ▼                               ▼
                    ┌────────────────┐            (snapshots EBS · DLM)
                    │ S3 data lake   │  ◄── ObjectCreated raw/ ──► Lambda trigger-airflow
                    │ raw/curated/…  │
                    └────────────────┘
```

---

## 2. Costo

> Precios **aproximados** us-east-1 (on-demand), sujetos a cambio — validá en
> [calculator.aws](https://calculator.aws). Escenario: ~1 h de Spark/día, ~50 GB.

| Item | US$/mes |
|---|---|
| EC2 `m6i.xlarge` (4 vCPU/16 GB) con **auto start/stop** (8h×22d) | ~34 |
| EBS gp3 (root 40 + data 200) + snapshots DLM | ~22 |
| S3 data lake (~50 GB) + requests | ~1.5 |
| Lambda + EventBridge + SSM | ~0 (free tier) |
| **Total** | **~58/mes** |

`m6i.xlarge` alcanza porque el cuello no es el dato (~50 MB es trivial) sino la RAM de las JVMs +
Airflow + monitoreo. Sin auto start/stop (EC2 24/7) serían ~$140/mes: el `start/stop` automático es la
palanca de ahorro principal. El monitoreo corre dentro de la misma EC2 (costo $0 adicional). Para
una máquina de desarrollo chica (~8 GB) existe el `docker-compose.dev.yml` del final, solo Spark+Jupyter.

### Self-managed vs managed: ¿cuándo cada uno?

Este diseño es self-managed **a propósito**, pero no porque lo managed sea siempre caro: depende del
uso. Comparación aproximada (us-east-1, datos chicos, ~20 tareas/día):

| Opción | Cómo cobra | ~US$/mes a esta escala | Ops | Cuándo gana |
|---|---|---|---|---|
| **Self-managed EC2** (este stack) | tiempo encendido (flat) | ~34 compute (~58 total) | Vos | consolidar varias cargas en la máquina ya paga; control y portabilidad (cero lock-in) |
| **EMR Serverless** | vCPU-seg + GB-seg, escala a cero | ~9 (+ S3) | AWS | Spark chico/esporádico con mínima ops |
| **Glue Spark** | DPU-hora (mín 2 DPU + 1 min por corrida) | ~44 | AWS | pocos jobs/día |
| **EMR on EC2** (clásico) | fleet EC2 + ~25% recargo | ~120–160 | Vos (cluster) | TB sostenidos, multi-nodo |
| **MWAA** (solo orquestación) | entorno siempre encendido | ~350+ | AWS | evitar a esta escala |

**Cómo leerlo:**
- El **costo por-corrida** duele solo en **Glue Spark** (pisos de 2 DPU + 1 min) y en clusters EMR
  clásicos (arranque). **Step Functions, Lambda, Athena, EventBridge** son centavos aunque los
  dispares miles de veces.
- El EC2 flat cuesta **lo mismo con 1 o 20 tareas** → cada tarea extra es gratis. Contra **Glue/MWAA**
  conviene a partir de ~10-20 tareas/día; contra **EMR Serverless** (sin pisos) el self-managed **no
  gana por costo**, gana por **control, portabilidad, aprendizaje y consolidación** (HDFS real +
  Airflow/Grafana siempre disponibles en una sola caja ya paga).
- **Regla:** uso bajo/esporádico + mínima ops → **serverless** (EMR Serverless / Lambda / Glue Python
  Shell + Step Functions + Athena); uso real y sostenido + querer controlar/aprender → **self-managed**
  (este stack).

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
└── prod/                # toda la infra del stack (un solo estado)
    ├── lambda/startstop.py
    ├── lambda/trigger_airflow.py
    └── *.tf              # network, iam, ec2, s3, orchestration, autostop, cicd, secrets
```

---

## 4. Fundamentos: backend Terraform

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
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
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

**Backend** (un solo estado remoto para toda la infra de producción):

```hcl
# infra/prod/backend.tf
terraform {
  backend "s3" {
    bucket         = "pyspark-stack-tfstate-abanto-2026"
    key            = "pyspark-stack-prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "pyspark-stack-tf-lock"
    encrypt        = true
  }
}
```

**Provider** (`infra/prod/providers.tf`):

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

## 5. Núcleo: EC2 con Docker

Una EC2 corre el `docker-compose` completo (idéntico al local). Acceso **solo por túnel SSH**;
ninguna UI expuesta a internet. Es el corazón del stack; las dos secciones siguientes le agregan el
data lake S3 y el disparo automático de los DAGs.

### 5.1 Variables y red

```hcl
# infra/prod/variables.tf
# Prefijo único de nombres. Cambiá SOLO esto para renombrar todo el proyecto (buckets, roles,
# lambdas, tags…). Todos los recursos lo interpolan como "${var.name_prefix}-...".
variable "name_prefix" {
  type    = string
  default = "pyspark-stack"
}
variable "aws_region" {
  type    = string
  default = "us-east-1"
}
variable "instance_type" {
  type = string
  # m6i.xlarge = 4 vCPU / 16 GB: corre el stack completo + monitoreo para datos chicos (~50 MB).
  # IMPORTANTE: familia m6i (NO t3). m6i tiene CPU dedicada y constante; t3 es burstable y al
  # apagar/prender agota "CPU credits" → caída de rendimiento visible. m6i garantiza el mismo
  # rendimiento en cada arranque. dev-lite (solo Spark+Jupyter): "t3.large".
  default = "m6i.xlarge"
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

Identidad de la cuenta (foundational: la reutilizan s3/iam/cicd/secrets, por eso va acá con las
variables y no más abajo):

```hcl
# infra/prod/locals.tf
# `var.name_prefix` se define arriba en variables.tf y lo usan TODOS los recursos.
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}
```

```hcl
# infra/prod/network.tf
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
  name        = "${var.name_prefix}-sg"
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
# infra/prod/iam.tf
resource "aws_key_pair" "pyspark" {
  key_name   = "${var.name_prefix}-key"
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
  name               = "${var.name_prefix}-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"   # entrar sin abrir puertos
}
resource "aws_iam_instance_profile" "ec2" {
  name = "${var.name_prefix}-ec2-profile"
  role = aws_iam_role.ec2.name
}
```

### 5.3 EC2 + EBS + user_data

```hcl
# infra/prod/ec2.tf
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

  # IMDSv2 obligatorio: sin esto, un SSRF en Airflow/Jupyter/Grafana podría leer
  # 169.254.169.254 y robar las credenciales del instance profile (S3 + secretos SSM).
  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }

  # Name = "<prefix>-node" (el workflow de CI busca la instancia por este tag);
  # AutoStartStop = "true" (la Lambda startstop filtra por él).
  tags = {
    Name          = "${var.name_prefix}-node"
    AutoStartStop = "true"
  }
}

resource "aws_ebs_volume" "data" {
  availability_zone = aws_instance.pyspark.availability_zone
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "${var.name_prefix}-data" } # ← el DLM (backups) respalda por este tag
  lifecycle {
    prevent_destroy = true # el disco de trabajo (HDFS/Postgres/Prometheus) NO se borra por accidente
  }
}
resource "aws_volume_attachment" "data" {
  device_name = "/dev/xvdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.pyspark.id
}
```

**`infra/prod/user_data.sh.tftpl`** (instala Docker + prepara disco de datos):

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
prender solo si hay trabajo en cola. Convierte los ~$140/mes fijos en ~$34 (u ~$17 con dev-lite).

**Código de la Lambda — `infra/prod/lambda/startstop.py`:**

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

**Terraform de la automatización — `infra/prod/automation.tf`:**

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
  name               = "${var.name_prefix}-startstop-lambda"
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
  function_name    = "${var.name_prefix}-startstop"
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
  name               = "${var.name_prefix}-startstop-scheduler"
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
  name                         = "${var.name_prefix}-start"
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
  name                         = "${var.name_prefix}-stop"
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

**Apagar/prender NO degrada el rendimiento** (cuatro garantías de diseño):

1. **Familia `m6i` (no `t3`)** — CPU dedicada y constante. Los `t3` son *burstable*: acumulan/gastan
   "CPU credits" y tras un arranque podés arrancar con pocos créditos → throttling visible. `m6i`
   entrega el 100% de sus vCPU desde el primer segundo, en cada boot.
2. **EBS `gp3` (no `gp2`)** — IOPS y throughput **provisionados y constantes** (3000 IOPS / 125 MB/s
   base). `gp2` usa un "burst balance" que se agota; `gp3` no tiene ese pozo que vaciar, así que el
   disco rinde igual antes y después de cada ciclo.
3. **Los datos persisten** — al *stop* la instancia conserva sus volúmenes EBS (root + `/data`).
   HDFS, Postgres y las métricas siguen ahí; no se recalcula ni se recarga nada al prender.
4. **El stack vuelve solo** — Docker arranca en boot y `restart: unless-stopped` (ver hardening) vuelve
   a levantar los contenedores. No hay pasos manuales.

> Lo único más lento es la **primera** corrida tras el arranque (~1-2 min): las JVMs de Spark/HDFS
> hacen *warmup* (JIT, block report del datanode). No es una caída de rendimiento sostenida, es el
> costo único de encender; para datos de ~50 MB es de segundos.

### 5.5 Desplegar, subir código y túnel SSH

```hcl
# infra/prod/outputs.tf
output "public_ip"   { value = aws_instance.pyspark.public_ip }
output "instance_id" { value = aws_instance.pyspark.id }
output "tunnel_command" {
  value = "ssh -i ~/.ssh/pyspark_stack -L 8082:localhost:8082 -L 8888:localhost:8888 -L 8081:localhost:8081 -L 9870:localhost:9870 ec2-user@${aws_instance.pyspark.public_ip}"
}
```

Antes del `apply`, definí las dos variables sin default (`my_ip_cidr`, `ssh_public_key`) en un
`terraform.tfvars` — así el `apply` no las pide interactivamente y queda repetible. El archivo no se
commitea (`infra/**/*.tfvars` está en el `.gitignore`):

```hcl
# infra/prod/terraform.tfvars
my_ip_cidr     = "203.0.113.7/32"                     # curl -s https://checkip.amazonaws.com  (agregale /32)
ssh_public_key = "ssh-ed25519 AAAA...tu_clave... pyspark_stack" # cat ~/.ssh/pyspark_stack.pub
```

```bash
cd infra/prod
terraform init && terraform apply    # red + IAM + EC2 + EBS + auto start/stop (lo definido hasta acá)

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

> **Esto es el núcleo, no el final — la infra se arma incrementalmente.** El `apply` de acá crea solo
> lo definido hasta la §5. Las secciones **6-7** (data lake S3, orquestación), **11** (CI/CD) y **13**
> (secretos) **agregan más `.tf` a `infra/prod/`**: cada vez que sumás archivos, volvés a correr
> `terraform apply` (Terraform calcula el diff y crea lo nuevo). Del mismo modo, el `docker compose up`
> de arriba es el **arranque base** (útil para validar que el host levanta); la **puesta en producción
> real** —con monitoreo, hardening y secretos desde SSM— usa el override de prod y se arma en las §12-14:
> `./scripts/load-secrets.sh && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`.

---

## 6. Data lake en S3

HDFS (en la EC2) es el storage *de trabajo* de Spark; **S3 es el data lake durable**: sobrevive al
apagado de la EC2, es barato y es la fuente/destino de los ETL (`raw/ → curated/ → analytics/`).
Spark lo lee y escribe con `s3a://` usando el **rol IAM de la EC2** — sin access keys en disco.

### 6.1 Buckets S3

> `locals.tf` (con `local.account_id` y `local.region`) ya quedó definido en §5.1 — lo reutilizan los
> nombres de bucket de abajo.

```hcl
# infra/prod/s3.tf
locals {
  datalake  = "${var.name_prefix}-datalake-${local.account_id}"
  artifacts = "${var.name_prefix}-artifacts-${local.account_id}"   # scripts + logs + deploy/
}

resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake
  lifecycle { prevent_destroy = true } # el data lake NO se borra con terraform destroy
}
resource "aws_s3_bucket" "artifacts" { bucket = local.artifacts }

# Privados + cifrados + solo-TLS + versionado, para ambos buckets.
resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = toset([aws_s3_bucket.datalake.id, aws_s3_bucket.artifacts.id])
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = toset([aws_s3_bucket.datalake.id, aws_s3_bucket.artifacts.id])
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_versioning" "all" {
  for_each = toset([aws_s3_bucket.datalake.id, aws_s3_bucket.artifacts.id])
  bucket   = each.value
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_policy" "tls_only" {
  for_each = toset([aws_s3_bucket.datalake.id, aws_s3_bucket.artifacts.id])
  bucket   = each.value
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid = "DenyInsecureTransport", Effect = "Deny", Principal = "*", Action = "s3:*",
      Resource  = ["arn:aws:s3:::${each.value}", "arn:aws:s3:::${each.value}/*"],
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# Data lake: transición a clases baratas para bajar costo de almacenamiento.
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    id     = "tiering"
    status = "Enabled"
    filter {} # aplica a todo el bucket; el provider exige filter o prefix
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

output "datalake_bucket"  { value = aws_s3_bucket.datalake.id }
output "artifacts_bucket" { value = aws_s3_bucket.artifacts.id }
```

### 6.2 IAM: permitir s3a a la EC2 (sin keys)

Se agrega una política al **rol de la EC2** (`aws_iam_role.ec2`, definido antes) para que `s3a://`
funcione con el *instance profile*. La config de Spark (`spark-defaults.conf` con
`InstanceProfileCredentialsProvider`) se muestra en el hardening.

```hcl
# infra/prod/iam.tf   (junto al rol de la EC2)
data "aws_iam_policy_document" "ec2_s3" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/*", "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.datalake.arn, aws_s3_bucket.artifacts.arn]
  }
}
resource "aws_iam_role_policy" "ec2_s3" {
  name   = "ec2-s3a"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_s3.json
}
```

En los jobs PySpark, apuntá las rutas a `s3a://` (el rol resuelve las credenciales solo):

```python
df = spark.read.csv(f"s3a://{DATALAKE}/raw/customers.csv", header=True)
df.write.mode("overwrite").parquet(f"s3a://{DATALAKE}/curated/customers")
```

### 6.3 Backups: snapshots EBS automáticos (DLM)

`/data` (EBS gp3) guarda HDFS + Postgres + datos de monitoreo: es el estado que **no** vive en S3.
**Data Lifecycle Manager (DLM)** toma snapshots automáticos y retiene los últimos N — cero código.

```hcl
# infra/prod/backups.tf
data "aws_iam_policy_document" "dlm_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["dlm.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "dlm" {
  name               = "${var.name_prefix}-dlm"
  assume_role_policy = data.aws_iam_policy_document.dlm_assume.json
}
resource "aws_iam_role_policy_attachment" "dlm" {
  role       = aws_iam_role.dlm.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSDataLifecycleManagerServiceRole"
}
resource "aws_dlm_lifecycle_policy" "data" {
  description        = "Snapshots diarios del volumen de datos" # sin '/': DLM solo admite [0-9A-Za-z _-]
  execution_role_arn = aws_iam_role.dlm.arn
  state              = "ENABLED"
  policy_details {
    resource_types = ["VOLUME"]
    target_tags    = { Name = "${var.name_prefix}-data" }   # el tag del aws_ebs_volume.data
    schedule {
      name = "diario-7d"
      create_rule {
        interval      = 24
        interval_unit = "HOURS"
        times         = ["05:00"]
      }
      retain_rule { count = 7 }
      tags_to_add = { SnapshotCreator = "dlm" }
      copy_tags   = true
    }
  }
}
```

> Restore: creás un volumen desde el snapshot y lo montás en `/data` (o recreás la instancia con
> `user_data` que ya hace `mount ... /data`). S3 ya está versionado, así que el data lake
> tiene su propia protección.

---

## 7. Orquestación: Lambda trigger-airflow (SSM) + EventBridge + event-driven

Airflow corre dentro de la EC2, **sin su API expuesta a internet**. Para dispararlo desde AWS (por
cron o cuando llega un archivo a S3) se usa una **Lambda que ejecuta `airflow dags trigger` vía SSM
`SendCommand`** — sin abrir puertos ni exponer la UI. Es el mismo patrón para los dos disparadores.

### 7.1 Lambda que dispara los DAGs vía SSM

```python
# infra/prod/lambda/trigger_airflow.py
import os
import json
import urllib.parse
import boto3

ssm = boto3.client("ssm")

def handler(event, context):
    """Dispara un DAG de Airflow dentro de la EC2 vía SSM SendCommand.
    - Por cron (EventBridge): event = {"dag": "customer_etl"}.
    - Por evento S3: event = {"Records": [{s3: {bucket, object{key}}}]} → pasa bucket/key como --conf.
    """
    instance_id = os.environ["INSTANCE_ID"]
    default_dag = os.environ.get("DEFAULT_DAG", "customer_etl")

    conf = {}
    dag = event.get("dag", default_dag)
    if "Records" in event:  # vino de S3 ObjectCreated
        rec = event["Records"][0]["s3"]
        # S3 codifica la key en el evento (espacios → '+', chars especiales → %XX): decodificar
        key = urllib.parse.unquote_plus(rec["object"]["key"])
        conf = {"bucket": rec["bucket"]["name"], "key": key}

    trigger = f"airflow dags trigger {dag}"
    if conf:
        trigger += f" --conf '{json.dumps(conf)}'"
    cmd = f"docker exec airflow-scheduler {trigger}"

    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment=f"trigger airflow dag {dag}",
        Parameters={"commands": [cmd]},
    )
    return {"dag": dag, "conf": conf, "commandId": resp["Command"]["CommandId"]}
```

> **Ojo con el auto start/stop:** si la EC2 está apagada cuando llega el evento/cron, el `SendCommand`
> no ejecuta (no hay agente SSM online) y el DAG no se dispara. Para ETLs event-driven fuera de horario,
> encendé la EC2 antes (encadenando la Lambda `startstop`) o programá el cron dentro de la ventana de
> encendido. `send_command` no falla de forma obvia en ese caso — el `DailyEtlMissing` de §12.4 lo cubre.

```hcl
# infra/prod/orchestration.tf
data "archive_file" "trigger_airflow" {
  type        = "zip"
  source_file = "${path.module}/lambda/trigger_airflow.py"
  output_path = "${path.module}/lambda/trigger_airflow.zip"
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
resource "aws_iam_role" "trigger_airflow" {
  name               = "${var.name_prefix}-trigger-airflow"
  assume_role_policy = data.aws_iam_policy_document.trigger_assume.json
}
data "aws_iam_policy_document" "trigger_airflow" {
  statement {   # solo puede mandar el comando a NUESTRA instancia
    actions   = ["ssm:SendCommand"]
    resources = [
      "arn:aws:ec2:${local.region}:${local.account_id}:instance/${aws_instance.pyspark.id}",
      "arn:aws:ssm:${local.region}::document/AWS-RunShellScript",
    ]
  }
  statement {
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations"]
    resources = ["*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}
resource "aws_iam_role_policy" "trigger_airflow" {
  name   = "trigger-airflow"
  role   = aws_iam_role.trigger_airflow.id
  policy = data.aws_iam_policy_document.trigger_airflow.json
}
resource "aws_lambda_function" "trigger_airflow" {
  function_name    = "${var.name_prefix}-trigger-airflow"
  filename         = data.archive_file.trigger_airflow.output_path
  source_code_hash = data.archive_file.trigger_airflow.output_base64sha256
  handler          = "trigger_airflow.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.trigger_airflow.arn
  timeout          = 60
  environment {
    variables = {
      INSTANCE_ID = aws_instance.pyspark.id
      DEFAULT_DAG = "customer_etl"
    }
  }
}
```

### 7.2 Disparo por cron (EventBridge Scheduler)

```hcl
# infra/prod/orchestration.tf  (continuación)
data "aws_iam_policy_document" "sched_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "sched_etl" {
  name               = "${var.name_prefix}-etl-scheduler"
  assume_role_policy = data.aws_iam_policy_document.sched_assume.json
}
resource "aws_iam_role_policy" "sched_etl" {
  name = "invoke-trigger"
  role = aws_iam_role.sched_etl.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "lambda:InvokeFunction",
    Resource = aws_lambda_function.trigger_airflow.arn }] })
}
resource "aws_scheduler_schedule" "daily_etl" {
  name                         = "${var.name_prefix}-daily-etl"
  schedule_expression          = "cron(0 6 * * ? *)"   # 06:00 UTC diario
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.trigger_airflow.arn
    role_arn = aws_iam_role.sched_etl.arn
    input    = jsonencode({ dag = "customer_etl" })
  }
}
```

### 7.3 Disparo por evento (archivo nuevo en S3)

Cuando llega un archivo a `raw/`, S3 invoca la Lambda, que dispara el DAG pasando `{bucket,key}`
como `--conf`. ETL 100% event-driven, sin polling.

```hcl
# infra/prod/orchestration.tf  (continuación)
resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_airflow.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.datalake.arn
}
resource "aws_s3_bucket_notification" "on_upload" {
  bucket = aws_s3_bucket.datalake.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_airflow.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }
  depends_on = [aws_lambda_permission.s3_invoke]
}
```

> El DAG recibe `bucket`/`key` en `dag_run.conf` y Spark lee justo ese objeto de `s3a://`. Para
> pipelines con dependencias complejas entre tasks, el DAG de Airflow ya las modela (no hace falta
> nada extra) — es la ventaja de mantener Airflow self-managed en vez de disparar jobs sueltos.

---

## 8. Operación, seguridad y ahorro

**Validación (smoke tests) — de abajo hacia arriba.** Un experto no da por hecho que el `apply`
"funcionó": valida capa por capa y **para en la primera que falle** (no tiene sentido probar un DAG
si el agente SSM no está `Online`). El orden es infra → host/red → stack → negocio → monitoreo.

```bash
# ── 0. PRE-APPLY (local, antes de tocar AWS) ─────────────────────────────
aws sts get-caller-identity                              # ¿la cuenta correcta?
terraform -chdir=infra/prod fmt -check -recursive        # formato canónico
terraform -chdir=infra/prod validate                     # config válida
terraform -chdir=infra/prod plan                          # LEÉ el diff antes de aplicar
# (extra experto) escaneo de seguridad de la IaC, si los tenés instalados:
tfsec infra/prod  ||  checkov -d infra/prod

# ── 1. INFRA AWS (después del apply) ─────────────────────────────────────
cd infra/prod
ID=$(terraform output -raw instance_id); IP=$(terraform output -raw public_ip)
ACCT=$(aws sts get-caller-identity --query Account --output text)
aws ec2 describe-instances --instance-ids "$ID" \
  --query 'Reservations[].Instances[].{estado:State.Name,imdsv2:MetadataOptions.HttpTokens}'  # running + required
aws ssm describe-instance-information \
  --query "InstanceInformationList[?InstanceId=='$ID'].PingStatus"   # "Online" (si no, el trigger NO anda)
aws s3 ls | grep pyspark-stack                            # buckets datalake + artifacts
aws scheduler list-schedules --query 'Schedules[].Name'   # start / stop / daily-etl
aws dlm get-lifecycle-policies --query 'Policies[].State' # ENABLED (backups)

# ── 2. RED / HOST (superficie de ataque) ─────────────────────────────────
nc -zv "$IP" 22 && echo "SSH ok"
curl --max-time 5 "http://$IP:8082" && echo "MAL: Airflow expuesto a internet" || echo "OK: UI cerrada"
ssh -i ~/.ssh/pyspark_stack ec2-user@"$IP" 'cd pyspark_stack && docker compose ps'  # todos Up/healthy

# ── 3. STACK FUNCIONAL (por túnel SSH — abrí el output tunnel_command aparte) ─
S="ssh -i ~/.ssh/pyspark_stack ec2-user@$IP"
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags list-import-errors'  # vacío = OK
$S 'cd pyspark_stack && docker compose exec -T hdfs-namenode hdfs dfsadmin -report | grep "Live datanodes"'
curl -s localhost:8081 | grep -o ALIVE | head -1          # Spark: worker vivo
aws s3 cp README.md "s3://pyspark-stack-datalake-$ACCT/raw/smoke.txt"  # s3a/rol IAM escribe (sin keys)

# ── 4. NEGOCIO end-to-end (orquestación) ─────────────────────────────────
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --payload '{"dag":"customer_etl"}' /dev/stdout          # disparo manual (mismo camino que EventBridge/S3)
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags list-runs -d customer_etl'
# event-driven: un archivo nuevo en raw/ debe disparar el DAG solo
aws s3 cp datos.csv "s3://pyspark-stack-datalake-$ACCT/raw/" && sleep 20   # y repetí el list-runs

# ── 5. MONITOREO (por túnel -L 9090 -L 3000) ─────────────────────────────
curl -sf localhost:9090/-/healthy && echo "Prometheus OK"
curl -s  localhost:9090/api/v1/targets | grep -o '"health":"[a-z]*"' | sort | uniq -c  # todos "up"
curl -sf localhost:3000/api/health   && echo "Grafana OK"
curl -sf localhost:9093/-/healthy    && echo "Alertmanager OK"
curl -sf localhost:3100/ready        && echo "Loki OK"
```

> Estos smoke tests son la base de un **check de CI post-deploy**: el workflow de Deploy (§11) puede
> correr los de la capa 1-2 tras cada push para confirmar que la EC2 quedó sana.

**Operación (cheat-sheet):**

```bash
# La Lambda startstop prende/apaga la EC2 sola por cron. Manual:
aws ec2 stop-instances --instance-ids $(cd infra/prod && terraform output -raw instance_id)
aws lambda invoke --function-name pyspark-stack-startstop --payload '{"action":"start"}' /dev/stdout

# Disparar un DAG a mano (mismo camino que usa EventBridge/S3):
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --payload '{"dag":"customer_etl"}' /dev/stdout

# Teardown total
cd infra/prod && terraform destroy
```

**Seguridad (checklist):**
- [ ] Buckets con `public_access_block`, cifrado y política solo-TLS (ya en el TF del data lake).
- [ ] SG de EC2: solo puerto 22 desde tu IP; UIs por túnel; API de Airflow nunca expuesta.
- [ ] IAM least-privilege: `startstop` **solo** actúa sobre instancias con el tag; `trigger-airflow`
      **solo** puede `ssm:SendCommand` sobre esta instancia (condiciones ya puestas).
- [ ] Spark usa `s3a://` con el rol IAM (instance profile), sin access keys en disco.
- [ ] Secretos en **SSM Parameter Store / Secrets Manager**, no en texto plano.
- [ ] `.env`, `terraform.tfvars`, `*.zip` de Lambdas y `alertmanager.yml` en `.gitignore`.

**Palancas de ahorro (orden de impacto):**
1. **Auto start/stop de la EC2** → de ~$140 a ~$34/mes. Es la palanca principal.
2. **S3 lifecycle a IA/Glacier** (ya aplicado): baja el costo de almacenamiento del lake.
3. **Snapshots DLM con retención acotada** (7 días): backups sin acumular costo.
4. **`docker-compose.dev.yml`** en una instancia chica (~$17/mes) para desarrollo.

### Resumen: qué corre y dónde

| Subsistema | Dónde corre | Storage durable |
|---|---|---|
| HDFS namenode/datanode | contenedores en EC2 | — (trabajo); respaldo en snapshots EBS |
| Spark master/worker + `spark-submit` | contenedores en EC2 | lee/escribe `s3a://` |
| Airflow (5 svcs) + Postgres | contenedores en EC2 | Postgres en `/data` (EBS) + snapshots |
| Jupyter | contenedor en EC2 | `./notebooks` (git) + S3 |
| Disparo de DAGs | Lambda trigger-airflow (SSM) + EventBridge | — |
| Encendido | EC2 con auto start/stop | — |

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

### 9.0 Patrones de tarea para ETL batch (¿PySpark o Python puro?)

Airflow es **solo el orquestador**: cada task elige su motor. La regla que sigue un ingeniero de datos
para no sobre-usar Spark:

| Tarea | Motor | Operador | Cuándo |
|---|---|---|---|
| Datos chicos (<~1 GB), transform simple, llamar una API, mover/validar archivos, gatillar dbt | **Python puro** (pandas/duckdb) | `@task` / `PythonOperator` | La mayoría de los pasos. No arranques una JVM Spark para 50 MB. |
| Datos medianos/grandes, joins/`groupBy` pesados, muchos archivos, paralelismo | **PySpark** | `SparkSubmitOperator` (o `BashOperator`→`spark-submit`) | Cuando el dato no entra cómodo en una máquina o el *shuffle* es grande. |
| Análisis/reporte reproducible con evidencia | Notebook | `PapermillOperator` | Ver 9.1-9.3. |

**Python puro — el caso "no necesito Spark"** (`requirements.txt`: `pandas`, `s3fs`, `pyarrow`):

```python
# dags/small_etl_dag.py — sin Spark: pandas lee de S3 y escribe curated
from datetime import datetime
import pandas as pd
from airflow.sdk import DAG, task        # Airflow 3: DAG y TaskFlow @task en airflow.sdk

with DAG("small_etl", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False) as dag:
    @task
    def transform(run_date="{{ ds }}"):
        base = "s3://pyspark-stack-datalake-<acct>"
        df = pd.read_csv(f"{base}/raw/ventas.csv")            # s3fs + rol IAM (sin keys)
        out = df[df["monto"] > 0].groupby("pais")["monto"].sum().reset_index()
        out.to_parquet(f"{base}/curated/ventas_por_pais/{run_date}.parquet")
    transform()
```

**PySpark — el caso "sí necesito Spark"** (operador idiomático; `requirements.txt`:
`apache-airflow-providers-apache-spark`, y una conexión Airflow `spark_default` →
`spark://spark-master:7077`):

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
SparkSubmitOperator(
    task_id="sales_etl",
    application="/opt/spark-apps/sales_etl/scripts/sales_etl_job.py",
    conn_id="spark_default",
    application_args=["{{ ds }}"],
)
```
Es más limpio que envolver `spark-submit` en un `BashOperator` (maneja conexión, args y código de
salida). El `BashOperator`→`spark-submit` de los DAGs actuales funciona; esto es su evolución.

**Leer archivos, y cómo se unen los tres almacenamientos (dónde guardar y cumplir):**
- **Fuente y destino durable = S3.** Capas: `raw/` (crudo, como llegó) → `curated/` (limpio, Parquet)
  → `analytics/` (agregados listos para consumo/BI). Sobrevive al apagado de la EC2. Spark usa
  `s3a://`, pandas usa `s3://` — ambos con el rol IAM, sin keys.
- **Disco local (EBS) = scratch AUTOMÁTICO de Spark.** El *shuffle* y el *spill* (cuando el dato no
  entra en RAM) van a `spark.local.dir` en el disco local, **no a HDFS**. Esto pasa solo, sin que
  toques nada, y es lo que de verdad usa Spark para trabajar.
- **HDFS = staging EXPLÍCITO opcional, no data lake.** Solo se usa si vos escribís `hdfs://` a
  propósito — típicamente para materializar un dataset intermedio que **comparten varios jobs Spark**
  en un pipeline multi-paso, evitando el ida-y-vuelta a S3. Si tu ETL es leer `s3a://raw` → procesar →
  escribir `s3a://curated`, **HDFS no se toca nunca**.
- **Patrón:** leé de `s3a://…/raw/` → Spark procesa (spillea a disco local solo) → escribí a
  `s3a://…/curated/`. Los DAGs de ejemplo que leen `hdfs://` son para pruebas locales; en producción
  las rutas apuntan a `s3a://`.

> **Cómo encaja HDFS en este diseño:** HDFS gana su lugar cuando un pipeline tiene **varios jobs Spark
> encadenados** y el intermedio se reusa: materializarlo en `hdfs://` (local al cluster) evita el
> ida-y-vuelta a S3 entre pasos. El dato durable siempre termina en S3; HDFS es el *scratch compartido*
> entre jobs. (Si tu pipeline fuera un solo job `s3a://raw → s3a://curated`, HDFS no haría falta — pero
> con multi-paso PySpark sí cumple.)

**Pipeline unificado — Python puro + PySpark + HDFS + S3, todo junto:**

```python
# dags/ventas_diario_dag.py
from datetime import datetime
import pandas as pd
from airflow.sdk import DAG, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

S3   = "s3://pyspark-stack-datalake-<acct>"          # pandas
S3A  = "s3a://pyspark-stack-datalake-<acct>"         # Spark
HDFS = "hdfs://hdfs-namenode:9000/staging/ventas"    # intermedio, reusado entre jobs

with DAG("ventas_diario", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False) as dag:

    @task  # 1) PYTHON PURO: ingesta/validación liviana → aterriza en S3 raw
    def ingesta(ds=None):
        df = pd.read_csv("/opt/spark-apps/landing/ventas.csv")
        assert not df.empty, "archivo vacío"
        df.to_csv(f"{S3}/raw/ventas/{ds}.csv", index=False)

    # 2) PYSPARK (tu cluster): join/limpieza pesada  S3 raw → HDFS (intermedio)
    enriquecer = SparkSubmitOperator(
        task_id="enriquecer", conn_id="spark_default",           # spark://spark-master:7077
        application="/opt/spark-apps/ventas/enriquecer.py",
        application_args=[S3A + "/raw/ventas/{{ ds }}.csv", HDFS + "/{{ ds }}"],
    )

    # 3) PYSPARK (tu cluster): agrega desde HDFS → S3 curated (durable)
    agregar = SparkSubmitOperator(
        task_id="agregar", conn_id="spark_default",
        application="/opt/spark-apps/ventas/agregar.py",
        application_args=[HDFS + "/{{ ds }}", S3A + "/curated/ventas_por_pais/{{ ds }}"],
    )

    @task  # 4) PYTHON PURO: post-proceso liviano (resumen / publicación)
    def publicar(ds=None):
        df = pd.read_parquet(f"{S3}/curated/ventas_por_pais/{ds}")
        print("filas curadas:", len(df))

    ingesta() >> enriquecer >> agregar >> publicar()
```

Qué usa cada pieza, y por qué corre en producción:

| Paso | Motor | Storage | Rol |
|---|---|---|---|
| 1 ingesta | **Python puro** (`@task`) | escribe **S3 `raw/`** | I/O liviano, no amerita Spark |
| 2 enriquecer | **PySpark** (cluster) | lee S3 `raw/` → escribe **HDFS** | transform pesado; deja intermedio local |
| 3 agregar | **PySpark** (cluster) | lee **HDFS** → escribe **S3 `curated/`** | reusa el intermedio sin round-trip a S3 |
| 4 publicar | **Python puro** (`@task`) | lee **S3 `curated/`** | resumen/notificación liviana |

Así **Airflow** orquesta, **Spark** (tu máquina) hace lo pesado, **HDFS** es el scratch compartido entre
los jobs pesados, **S3** guarda lo durable, y **Python puro** cubre lo liviano — cada componente que
levantás tiene un trabajo real en el pipeline.

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
> un `airflow dags trigger <dag>` vía SSM, o usar la Lambda `trigger-airflow` de la sección de
> orquestación.

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
      values   = ["repo:${var.github_repo}:ref:refs/heads/main"] # solo la branch main de tu repo
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
| HDFS métricas ricas | ⚠️ parcial | `/jmx` devuelve JSON no parseable → scrape quitado; recursos vía cAdvisor; métricas ricas con jmx_exporter (ver "Acceso, verificación y HDFS") |
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
  # HDFS NO se scrapea: /jmx devuelve JSON no parseable. Ver la nota de jmx_exporter más abajo.
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
      # ── Alertas de NEGOCIO (no solo infra): ¿el pipeline cumple? ──
      - alert: DagRunFailed
        expr: increase(airflow_dagrun_duration_failed_count[15m]) > 0
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "Falló un DAG run ({{ $labels.dag_id }})"
          description: "El pipeline {{ $labels.dag_id }} terminó en error en los últimos 15m."
      - alert: DailyEtlMissing   # dead-man switch: el ETL diario dejó de correr en silencio
        expr: increase(airflow_dagrun_duration_success_count{dag_id="customer_etl"}[26h]) == 0
        for: 10m
        labels: { severity: critical }
        annotations:
          summary: "El ETL diario no completó con éxito (dead-man switch)"
          description: "customer_etl no registró corrida exitosa en 26h (¿EC2 apagada? ¿trigger falló?). Ajustá dag_id/ventana al DAG real."
```

> Las métricas `airflow_dagrun_duration_{success,failed}_count` salen del `statsd_mapping.yml` (§12.5);
> verificá los nombres exactos en `localhost:9090/api/v1/targets` la primera vez. `DailyEtlMissing` es la
> más valiosa para un ingeniero de datos: avisa cuando el pipeline **deja de correr**, no solo cuando falla.

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
resource "random_password" "postgres" {
  length  = 24
  special = false
}
resource "random_password" "jwt" {
  length  = 48
  special = false
}
resource "random_password" "admin" {
  length = 20
}
resource "random_password" "jupyter" {
  length  = 32
  special = false
}
resource "random_password" "grafana" {
  length = 20
}

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

**IAM — permitir a la EC2 leer los parámetros (agregar a `infra/prod/iam.tf`, junto al rol de la EC2):**

```hcl
# infra/prod/iam.tf   (política propia del rol de la EC2 para leer secretos)
data "aws_iam_policy_document" "ec2_secrets" {
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
}
resource "aws_iam_role_policy" "ec2_secrets" {
  name   = "ec2-secrets"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_secrets.json
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

IAM — agregá este `statement` **dentro de** `data.aws_iam_policy_document.ec2_secrets` (el bloque de
arriba), junto a los de SSM/KMS:

```hcl
# infra/prod/iam.tf   (statement adicional en data.aws_iam_policy_document "ec2_secrets")
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

Los servicios core (`hdfs-*`, `spark-*`, `airflow-db`) no traen `restart` ni límites.
Agregalos en el override de prod.

> **Jupyter no arranca en prod por defecto.** En el base está bajo el perfil `dev`
> (`profiles: ["dev"]`), así que `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`
> **no** lo levanta salvo que el `.env` traiga `COMPOSE_PROFILES=dev` (el `.env` generado por
> `load-secrets.sh` no lo incluye). En prod el ETL corre por Airflow y los `.ipynb` por papermill
> (headless), que no necesitan el server de Jupyter. El bloque `jupyter:` de abajo solo aplica si
> activás el perfil `dev` a mano (p. ej. para analizar algo puntual en la EC2). **`restart: unless-stopped` es lo que hace que, al **prender** la
EC2 (auto start/stop), el stack vuelva solo** sin intervención (Docker arranca en boot por
`systemctl enable docker` del `user_data`, y reinicia los contenedores que estaban corriendo).

Los límites están calibrados para **`m6i.xlarge` (16 GB)**: suman con holgura y dejan margen al SO.

```yaml
# docker-compose.prod.yml
x-hard: &hard
  restart: unless-stopped
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }

services:
  hdfs-namenode: { <<: *hard, deploy: { resources: { limits: { memory: 1536m } } } }
  hdfs-datanode: { <<: *hard, deploy: { resources: { limits: { memory: 1536m } } } }
  spark-master:  { <<: *hard, deploy: { resources: { limits: { memory: 1g } } } }
  spark-worker:
    <<: *hard
    # El worker OFRECE 3G/2 cores a los executors (sobra para ~50 MB; evita que agarre toda la RAM).
    command: ["org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077", "--memory", "3G", "--cores", "2"]
    deploy: { resources: { limits: { memory: 4g } } }
  jupyter:       { <<: *hard, deploy: { resources: { limits: { memory: 1536m } } } }
  airflow-db:    { <<: *hard, deploy: { resources: { limits: { memory: 512m } } } }
```

> **¿Por qué el worker no pierde rendimiento con menos memoria?** Para 50 MB, un executor con 3 GB
> y 2 cores procesa el dato entero en memoria sin *spill* a disco — que es lo que sí frenaría. Los
> 6 GB anteriores eran margen ocioso, no velocidad. El límite del contenedor (4 GB) deja aire al
> heap del worker por encima de lo que ofrece.

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

### 13.6 Spark History Server (UI de jobs terminados)

Spark master/worker solo muestran jobs **en vivo**; al terminar, la UI del driver desaparece. El
**History Server** reconstruye la UI de cada job desde los *event logs* — clave para depurar un ETL
que ya corrió. La imagen ya existe (`Dockerfile.history`) y los logs se escriben en `./spark-events`.

Habilitá el *event log* en `spark-defaults.conf` (el mismo que usa s3a) y descomentá el servicio:

```properties
# spark-conf/spark-defaults.conf  (agregar a lo de s3a)
spark.eventLog.enabled  true
spark.eventLog.dir      file:/tmp/spark-events
```

```yaml
# docker-compose.prod.yml  (servicio History Server)
  spark-history-server:
    build: { context: ., dockerfile: Dockerfile.history }
    image: pyspark_stack-spark-history:4.0.3
    container_name: spark-history
    <<: *hard
    entrypoint: ["/opt/spark/bin/spark-class"]
    command: ["org.apache.spark.deploy.history.HistoryServer"]
    environment:
      - SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=file:/tmp/spark-events
    ports: ["18080:18080"]
    volumes:
      - ./spark-events:/tmp/spark-events
    networks: [hadoopnet]
```

UI por túnel: `ssh -L 18080:localhost:18080 …` → `http://localhost:18080`.

### 13.7 Checklist final (production-ready)

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
las secciones de monitoreo y hardening: persistencia en `/data`, `restart`, límites, logging, s3a,
métricas y logs. Las variables `${...}` las provee el `.env` que genera `load-secrets.sh` desde SSM.

### 14.1 docker-compose.prod.yml (producción, completo)

```yaml
# docker-compose.prod.yml — override de producción (se fusiona con docker-compose.yml).
#   ./scripts/load-secrets.sh   # genera .env desde SSM
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
  # ---- Persistencia en /data (EBS) + restart/límites/logging (calibrado a m6i.xlarge 16 GB) ----
  airflow-db:
    <<: *hard
    deploy: { resources: { limits: { memory: 512m } } }
    volumes:
      - /data/postgres:/var/lib/postgresql/data

  hdfs-namenode:
    <<: *hard
    deploy: { resources: { limits: { memory: 1536m } } }
    volumes:
      - /data/hdfs-nn:/hadoop/dfs/name
  hdfs-datanode:
    <<: *hard
    deploy: { resources: { limits: { memory: 1536m } } }
    volumes:
      - /data/hdfs-dn:/hadoop/dfs/data

  # ---- Spark: métricas Prometheus + s3a con rol IAM ----
  spark-master:
    <<: *hard
    deploy: { resources: { limits: { memory: 1g } } }
    volumes:
      - ./monitoring/spark/metrics.properties:/opt/spark/conf/metrics.properties:ro
      - ./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
  spark-worker:
    <<: *hard
    # OFRECE 3G/2 cores a los executors: sobra para ~50 MB y evita que tome toda la RAM del host.
    command: ["org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077", "--memory", "3G", "--cores", "2"]
    deploy: { resources: { limits: { memory: 4g } } }
    volumes:
      - ./monitoring/spark/metrics.properties:/opt/spark/conf/metrics.properties:ro
      - ./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro

  jupyter:
    <<: *hard
    deploy: { resources: { limits: { memory: 1536m } } }
    environment:
      - JUPYTER_TOKEN=${JUPYTER_TOKEN}
    volumes:
      - ./spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro

  # ---- Airflow: emitir métricas StatsD ----
  airflow-apiserver:     { environment: { <<: *airflow-metrics } }
  airflow-scheduler:     { environment: { <<: *airflow-metrics } }
  airflow-dag-processor: { environment: { <<: *airflow-metrics } }
  airflow-triggerer:     { environment: { <<: *airflow-metrics } }

  # ==================== MONITOREO (detalle en la sección de monitoreo) ====================
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
