# Guía experta — Producción en AWS para DataOps

> Arquitectura híbrida de bajo costo: Airflow y Postgres se ejecutan en una EC2; Spark se ejecuta
> en EMR Serverless; S3 funciona como data lake; EventBridge, SQS, Lambda y SSM automatizan los
> disparos; Terraform administra la infraestructura; GitHub Actions entrega el código mediante
> OIDC, sin claves permanentes.
>
> El diseño es *single-node* para el plano de control. Prioriza simplicidad y costo frente a alta
> disponibilidad. EMR Serverless escala el cómputo, pero la EC2 sigue siendo un punto único de
> fallo para Airflow, Postgres y el monitoreo local.

## Cómo usar esta guía

- Las secciones 1–7 construyen la plataforma.
- Desde la sección 8, la guía funciona como manual DevOps/DataOps: operar, desplegar, observar,
  proteger, controlar costos y recuperar el servicio.
- Cada bloque indica **dónde corre**, **qué resuelve** y **qué resultado esperar**.
- Los comentarios extensos se mantienen fuera del código. Dentro del código solo quedan
  advertencias que evitan una ejecución peligrosa o incorrecta.
- Sustituye `pyspark-stack` únicamente si también cambiaste `var.name_prefix`.

## Índice

1. [Panorama de la arquitectura](#1-panorama-de-la-arquitectura)
2. [Costo](#2-costo)
3. [Prerrequisitos](#3-prerrequisitos)
4. [Fundamentos: backend Terraform](#4-fundamentos-backend-terraform)
5. [Núcleo: EC2 con Docker](#5-núcleo-ec2-con-docker)
6. [Data lake en S3](#6-data-lake-en-s3)
7. [Orquestación por cron y eventos](#7-orquestación-lambda-trigger-airflow-ssm--eventbridge--event-driven)
8. [Operación diaria y diagnóstico](#8-operación-diaria-y-diagnóstico)
9. [Patrones de tareas DataOps](#9-patrones-de-tareas-dataops)
10. [Flujo de desarrollo y despliegue](#10-flujo-de-desarrollo-y-despliegue)
11. [CI/CD con GitHub Actions y OIDC](#11-cicd-con-github-actions-y-oidc)
12. [Observabilidad e incidentes](#12-observabilidad-e-incidentes)
13. [Hardening y secretos](#13-hardening-y-secretos)
14. [Compose canónico de producción](#14-compose-canónico-de-producción)
15. [Runbook de puesta en producción](#15-runbook-de-puesta-en-producción)
16. [Athena e Iceberg](#16-athena-e-iceberg)
17. [Qué motor usar para cada tarea](#17-qué-motor-usar-para-cada-tarea)
18. [Gobierno, resiliencia y costos](#18-gobierno-resiliencia-y-costos)
19. [Transformaciones con dbt](#19-transformaciones-con-dbt)
20. [Calidad de datos](#20-calidad-de-datos)
21. [Control de cambios y límites](#21-control-de-cambios-y-límites)
22. [Lineage con OpenLineage](#22-lineage-con-openlineage)

---
## 1. Panorama de la arquitectura

Una EC2 **chica** corre solo el orquestador en Docker; AWS *serverless* lo rodea para el cómputo
Spark (EMR Serverless), storage durable (S3), disparo de DAGs (Lambda + EventBridge) y ahorro
(auto start/stop). El detalle conceptual y los diagramas están en
[`docs/03-arquitectura.md`](03-arquitectura.md); esta guía es el cómo (Terraform copy-paste).

Regla mental: almacenar es barato y constante; computar es lo que cuesta, y solo cuando corrés.
Por eso Spark vive en EMR Serverless (escala a cero, paga solo mientras corre el job), la EC2 se
apaga fuera de horario (auto start/stop) y el data lake vive en S3.

```text
                    ┌──────────── EC2 t3.large (Elastic IP) ─────────────────┐
 EventBridge  ──►   │  docker compose (solo ORQUESTADOR, casi idle):          │
  · cron ETL        │   Airflow (5) + Postgres                                │
  · start/stop  ──► │   MONITOREO: Prometheus · Grafana · Alertmanager · Loki │
      │             └───────┬───────────────┬──────────────────┬─────────────┘
      ▼                     │ StartJobRun    │ s3a:// (rol IAM)  │ /data (EBS gp3)
  Lambda trigger-airflow    ▼                ▼                   ▼
  Lambda startstop   ┌──────────────┐  ┌────────────────┐  (snapshots EBS · DLM)
                     │ EMR          │  │ S3 data lake   │  ◄── ObjectCreated raw/ ──►
                     │ Serverless   │─►│ raw/curated/…  │        Lambda trigger-airflow
                     │ (Spark)      │  └────────────────┘
                     └──────────────┘
```

Airflow (en la EC2) dispara cada job Spark con `EmrServerlessStartJobOperator` y lo pollea con
`EmrServerlessJobSensor`; EMR Serverless lee/escribe `s3a://` con **su propio** rol de ejecución.
La EC2 nunca corre Spark: solo orquesta.

---

## 2. Costo

> Precios aproximados us-east-1 (on-demand), estimados en julio 2026 y sujetos a cambio — validá en
> [calculator.aws](https://calculator.aws). Escenario **real**: ~2 GB/día, 3 corridas/semana
> (≈13/mes) de Spark en EMR Serverless, con ~50 GB acumulados en el data lake.

| Ítem | US$/mes (auto start/stop 8h×22d) |
|---|---|
| EC2 `t3.large` (Airflow + Postgres + monitoreo) | ~12 |
| EMR Serverless (pago por uso, ~13 corridas/mes) | ~9 |
| EBS gp3 (root 40 + data 30) + snapshots DLM | ~9 |
| S3 data lake (~50 GB) + requests | ~1.5 |
| IPv4 pública (EIP; AWS la cobra desde feb-2024, asociada o no) | ~3.6 |
| Lambda + EventBridge + SSM | ~0 (free tier) |
| **Total** | **~35/mes** |

La EC2 `t3.large` (2 vCPU/8 GB) alcanza porque ya no corre Spark: es solo el orquestador (Airflow +
Postgres + monitoreo), casi idle entre corridas → un burstable barato es lo correcto (ver §5.4). El
cómputo pesado lo hace **EMR Serverless**, que paga solo mientras corre el job y **escala a cero**.
Variante **24/7** (EC2 encendida siempre): ~**$83/mes** (EC2 `t3.large` 24/7 ~$60, el resto igual).
Con tu volumen exacto, EMR Serverless ronda ~$5 → real ~**$31** (start/stop) / ~**$79** (24/7). Nota: el
auto start/stop ahora **impacta menos** que antes, porque ya no existe la EC2 de Spark
siempre encendida; lo que queda apagable es una instancia de por sí chica. El monitoreo corre dentro de la misma
EC2 (costo $0 adicional). Para el entorno de desarrollo local, ver `docs/01-docker-compose-explicado.md`.

### Self-managed vs managed: ¿cuándo cada uno?

Este diseño **ya es el híbrido**: EMR Serverless para el cómputo Spark chico/infrecuente, EC2 chica
solo para orquestar. No siempre lo managed gana ni pierde: depende del uso. Comparación aproximada
(us-east-1, datos chicos, ~20 tareas/día):

| Opción | Cómo cobra | ~US$/mes a esta escala | Ops | Cuándo gana |
|---|---|---|---|---|
| **EMR Serverless** (este stack, cómputo) | vCPU-seg + GB-seg, escala a cero | ~9 (+ S3) | AWS | **Spark chico/esporádico con mínima ops → lo elegido acá** |
| **Airflow en EC2 chica** (este stack, orquestación) | tiempo encendido (flat) | ~12 (~35 total) | Vos | orquestador liviano y portable, sin lock-in |
| **Spark self-managed en EC2** (una instancia grande) | tiempo encendido (flat) | ~34 compute | Vos | consolidar varias cargas Spark sostenidas en una máquina ya paga; HDFS real |
| **Glue Spark** | DPU-hora (mín 2 DPU + 1 min por corrida) | ~44 | AWS | pocos jobs/día, ecosistema Glue |
| **EMR on EC2** (clásico) | fleet EC2 + ~25% recargo | ~120–160 | Vos (cluster) | TB sostenidos, multi-nodo |
| **MWAA** (solo orquestación) | entorno siempre encendido | ~350+ | AWS | evitar a esta escala |

Cómo leerlo:
- Para este uso (Spark chico e infrecuente, ~13 corridas/mes) **EMR Serverless es lo que elegimos**:
  sin pisos por-corrida, sin caja siempre encendida, cero mantenimiento del cluster. Glue Spark
  queda penalizado por sus mínimos facturables (2 DPU + 1 min); EMR on EC2 y MWAA están sobredimensionados a esta escala.
- **Spark self-managed puro** (todo en una EC2 gorda, la arquitectura anterior de esta guía) queda
  como la alternativa para **consolidar cargas Spark sostenidas**: si corrieras Spark muchas horas
  al día, la EC2 flat ganaría por costo y te daría HDFS real más control total (cero lock-in). No es el caso
  de este volumen.
- Regla: uso bajo/esporádico + mínima ops → serverless (EMR Serverless / Lambda / Athena), con
  Airflow en una EC2 chica orquestando; uso Spark real y sostenido + querer controlar/aprender →
  Spark self-managed en una instancia dedicada ya paga.

---

## 3. Prerrequisitos

Se asume una cuenta de AWS con permisos para crear EC2, S3, IAM, Lambda, EventBridge y EMR
Serverless, el AWS CLI v2 ya configurado, y Terraform instalado. Estos tres comandos verifican lo
anterior: si los tres pasan sin error, tenés todo lo necesario para llegar hasta el final de la guía.

```bash
aws configure && aws sts get-caller-identity   # credenciales con permisos EC2/S3/IAM/Lambda...
terraform -version                             # >= 1.10 (el backend usa use_lockfile, §4)
ssh-keygen -t ed25519 -f ~/.ssh/pyspark_stack -C "pyspark_stack"   # si no tenés par de claves
```

Estructura de infraestructura usada por esta guía:

```text
infra/
├── bootstrap/                       # crea una sola vez el bucket del backend; state local
│   └── main.tf
└── prod/                            # raíz Terraform de producción; un único backend y state
    ├── backend.tf
    ├── providers.tf
    ├── variables.tf
    ├── locals.tf
    ├── network.tf
    ├── iam.tf
    ├── ec2.tf
    ├── user_data.sh.tftpl
    ├── automation.tf
    ├── orchestration.tf
    ├── s3.tf
    ├── emr.tf
    ├── backups.tf
    ├── dns.tf                       # solo si se habilita HTTPS para Airflow
    ├── secrets.tf
    ├── cicd.tf
    ├── governance.tf
    ├── athena.tf
    ├── outputs.tf
    ├── policies/
    │   └── route53-certbot.json.tftpl
    └── lambda/
        ├── startstop.py
        └── trigger_airflow.py
```

> **Cómo se construye:** no necesitás crear toda esta estructura al inicio. La guía agrega cada
> archivo exactamente cuando aparece por primera vez y luego indica cuándo ampliar uno existente.
> El flujo continúa siendo secuencial y copy-paste: `bootstrap` se ejecuta una vez y todas las
> secciones posteriores agregan recursos dentro de `infra/prod`.
>
> **Separación lógica, no física:** nombres como red, plataforma, datos o automatización describen
> responsabilidades de los archivos, pero **no son directorios Terraform independientes**. Esta
> guía no crea `foundation/`, `platform/`, `data/` ni `automation/`, no introduce módulos raíz
> adicionales, no divide el state y no requiere `terraform state mv`. Todas las herramientas y la
> arquitectura permanecen iguales: Airflow sigue siendo el orquestador; EMR Serverless ejecuta
> Spark; S3 almacena los datos; Lambda, EventBridge y SQS automatizan eventos; Terraform administra
> la infraestructura desde `infra/prod`.
>
> Los nombres anteriores representan el resultado final esperado. Durante la implementación puede
> haber menos archivos porque algunos son opcionales o se crean en capítulos posteriores. La fuente
> de verdad continúa siendo el código Terraform, no la estructura del índice ni los pasos de consola.

### Dos formas de crear la infra: Terraform y consola

Cada recurso de esta guía trae su Terraform (copy-paste) y, debajo, un desplegable
«🖱️ A mano en la consola AWS» con los pasos equivalentes. La consola sirve para entender qué
crea cada bloque, o para un despliegue puntual sin IaC; **Terraform es la fuente de verdad**:
reproducible, versionado y con `terraform destroy` limpio. No mezcles los dos caminos para el
mismo recurso — si algo ya lo creaste a mano y querés pasarlo a Terraform, primero
`terraform import`; si no, el `apply` duplica o falla por nombre ocupado.

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
  state_bucket = "pyspark-stack-tfstate-tu-sufijo-2026"   # ← único global, cambiá "tu-sufijo"
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
# El lock del state lo hace S3 nativamente (use_lockfile en el backend de abajo), con un objeto
# <key>.tflock por conditional write. Por eso no hay tabla DynamoDB: ya no hace falta.
```

```bash
terraform -chdir=infra/bootstrap init
terraform -chdir=infra/bootstrap apply
```

<details>
<summary>🖱️ A mano en la consola AWS — backend del state (S3)</summary>

1. **S3 → Create bucket**: nombre `pyspark-stack-tfstate-<sufijo-único>`, región `us-east-1`.
   *Bucket Versioning*: **Enable** · *Default encryption*: SSE-S3/AES256 (viene por defecto) ·
   *Block Public Access*: las 4 casillas activadas (default).
2. Listo: el bloque `backend "s3"` de abajo apunta a este bucket por nombre. No hay que crear nada
   más — el lock lo maneja S3 con `use_lockfile`, sin tabla DynamoDB.

</details>

**Backend** (un solo estado remoto para toda la infra de producción):

> Esta decisión se conserva para mantener el flujo copy-paste y evitar cambios destructivos. Aumenta
> el radio de impacto de cada `plan/apply`; por eso el state debe tratarse como recurso crítico,
> versionarse, bloquearse y revisarse siempre mediante `terraform plan` antes de aplicar.

```hcl
# infra/prod/backend.tf
terraform {
  backend "s3" {
    bucket         = "pyspark-stack-tfstate-tu-sufijo-2026"   # el mismo del bootstrap
    key            = "pyspark-stack-prod/terraform.tfstate"
    region         = "us-east-1"
    use_lockfile   = true   # lock nativo de S3 (conditional writes); reemplaza a dynamodb_table
    encrypt        = true
  }
}
```

**Provider** (`infra/prod/providers.tf`):

```hcl
# providers.tf
terraform {
  required_version = ">= 1.10"   # use_lockfile (backend.tf) no existe antes de 1.10
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

```bash
# comprobá
aws s3api head-bucket --bucket pyspark-stack-tfstate-tu-sufijo-2026          # sin error = existe
```

---

## 5. Núcleo: EC2 con Docker

Una EC2 corre un `docker-compose.prod.yml` propio, standalone (§14.1) — no el mismo de local: solo el
orquestador (Airflow + Postgres + monitoreo), sin Spark ni HDFS. Acceso por **túnel SSH** para todo,
más una **excepción explícita**: la web de Airflow se publica por **HTTPS (443) restringida a tu IP**
(§5.6), para poder seguir los DAGs desde el navegador sin túnel. Grafana/Prometheus/Loki
siguen **solo por túnel**. Esta sección arma el núcleo del stack; las dos siguientes le agregan el
data lake S3 y el disparo automático de los DAGs.

### 5.1 Variables y red

```hcl
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
# Usado recién en §18 (Budgets, Cost Anomaly Detection, alarma de la DLQ) — con default vacío como
# airflow_domain/dns_zone/letsencrypt_email: no bloquea los `apply` de las secciones 5-17, que no lo
# usan. Poné un valor real antes de aplicar §18 (sin él, esas notificaciones no tienen destino).
variable "alert_email" {
  description = "Email para alertas de gobierno/costo (Budgets, Cost Anomaly Detection, DLQ de Lambdas). §18."
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
```

Identidad de la cuenta — la reutilizan s3/iam/cicd/secrets, por eso va acá con las variables:

```hcl
# infra/prod/locals.tf
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
  # Sin este filtro, ids[0] es "la que devolvió la API primero" y puede cambiar entre applies.
  filter {
    name   = "availability-zone"
    values = [var.availability_zone]
  }
}

resource "aws_security_group" "pyspark" {
  name        = "${var.name_prefix}-sg"
  # OJO: AWS solo acepta a-zA-Z0-9 y . _-:/()#,@[]+=&;{}!$* en las descripciones de SG.
  # Nada de comillas simples ni acentos: fallan con InvalidParameterValue al crear el grupo.
  description = "SSH desde mi IP. Web de Airflow (443) desde mi IP si airflow_domain no esta vacio. Resto por tunel."
  vpc_id      = data.aws_vpc.default.id
  ingress {
    description = "SSH desde mi IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }
  # HTTPS de Airflow SOLO si se configuró airflow_domain (§5.6), y SOLO desde tu IP.
  # Vacío el dominio => 0 reglas 443 => nada expuesto (comportamiento original).
  dynamic "ingress" {
    for_each = var.airflow_domain == "" ? [] : [1]
    content {
      description = "HTTPS web de Airflow desde mi IP"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = [var.my_ip_cidr]
    }
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

<details>
<summary>🖱️ A mano en la consola AWS — security group</summary>

1. **VPC → Security groups → Create security group**: nombre `pyspark-stack-sg`, VPC: la *default*.
2. *Inbound rules* → Type `SSH` (TCP 22), Source **My IP** (tu `/32`). Si vas a exponer la web de
   Airflow (§5.6), agregá **una segunda** regla: Type `HTTPS` (TCP 443), Source **My IP**.
3. *Outbound rules*: dejar la default (todo permitido).
4. Verificá que **no** haya inbound para 8082/9090/3000 (ni ningún otro puerto de UI): esas van
   solo por túnel SSH. La única UI publicable es Airflow por 443 (§5.6); la Spark UI vive en la
   consola de EMR Serverless, no en la EC2.

</details>

> **Si tu IP de cliente cambia** (IP dinámica): la EC2 usa Elastic IP (§5.3), así que el *servidor* es
> estable entre stop/start; lo que se desactualiza es **tu** `/32` en `var.my_ip_cidr` (el *Source* de
> las reglas 22/443). Una Elastic IP no resuelve ese desfasaje: la que cambia es tu IP de casa/oficina, no la de la EC2.
> Nunca edites el SG a mano en la consola: mientras Terraform gestione las reglas, el próximo `apply`
> revierte el cambio a `var.my_ip_cidr`.
>
> Para refrescar ese `/32` **elegí una de las dos opciones de abajo, no las dos**: son excluyentes.
>
> **Opción A — Terraform lo sigue gestionando (recomendada).** Actualizá **`terraform.tfvars`** con tu
> IP actual y aplicá. Si es la primera vez que corrés Terraform contra `infra/prod` en esta máquina
> (hasta acá solo inicializaste `infra/bootstrap`, §4), primero `init`:
>
> ```bash
> terraform -chdir=infra/prod init
> sed -i "s#^my_ip_cidr .*#my_ip_cidr     = \"$(curl -s https://checkip.amazonaws.com)/32\"#" infra/prod/terraform.tfvars
> terraform -chdir=infra/prod apply
> ```
>
> Sin setup extra ni permisos IAM nuevos. Es la que conviene si tu IP cambia cada tantos días o semanas.
>
> ⚠️ **No uses solo `-var "my_ip_cidr=..."` sin tocar `tfvars`.** El flag `-var` vale nada más para
> ESE comando puntual: `tfvars` sigue teniendo el valor viejo (o el placeholder `203.0.113.7/32` de
> este mismo ejemplo, si todavía no lo tocaste). De acá en adelante, **todas** las secciones §6-§18
> te hacen correr `terraform apply` a secas, sin `-var`, en cada bloque `comprobá` — y ese `apply`
> "a secas" reaplica lo que hay en `tfvars`, revirtiendo el SG a la IP vieja y **cortándote el SSH
> que tenías andando**. Es la causa más común de "se me cae el SSH cada vez que sigo con la guía":
> `tfvars` nunca se actualizó con tu IP real. Arreglalo una vez con el `sed`/edición de arriba y no
> vuelve a pasar en ninguna sección siguiente.
>
> **Opción B — se lo sacás a Terraform y lo maneja un script CLI.** Solo vale la pena si tu IP cambia
> tan seguido que querés automatizarlo por cron local, o si no querés depender de tener Terraform y el
> state a mano. Tiene dos costos: agregar `lifecycle { ignore_changes = [ingress] }` al
> `aws_security_group` (a partir de ahí Terraform **deja de gestionar** esas reglas —ya no podés usar la
> opción A—) y darle a tu usuario/rol local los permisos `ec2:DescribeSecurityGroups`,
> `ec2:DescribeSecurityGroupRules` y `ec2:ModifySecurityGroupRules`.

<details>
<summary>📜 Opción B — script <code>scripts/update-sg-ip.sh</code></summary>

Corré esto desde tu máquina cuando cambie tu IP (o por cron local). Actualiza el `/32` de las reglas 22
y 443 sin tocar sus IDs, y salta el 443 si no lo expusiste. Recordá el `ignore_changes` y los permisos
de arriba: sin eso, el próximo `apply` pisa el cambio.

```bash
#!/usr/bin/env bash
# scripts/update-sg-ip.sh — pone tu IP de cliente actual en las reglas 22 y 443 del SG.
set -euo pipefail
REGION="${AWS_REGION:-us-east-1}"
SG_NAME="pyspark-stack-sg"
MYIP="$(curl -s https://checkip.amazonaws.com)/32"
echo "IP actual: $MYIP"
SG_ID=$(aws ec2 describe-security-groups --region "$REGION" \
  --filters "Name=group-name,Values=$SG_NAME" \
  --query 'SecurityGroups[0].GroupId' --output text)
for PORT in 22 443; do
  RULE_ID=$(aws ec2 describe-security-group-rules --region "$REGION" \
    --filters "Name=group-id,Values=$SG_ID" \
    --query "SecurityGroupRules[?FromPort==\`$PORT\` && IsEgress==\`false\` && IpProtocol=='tcp'].SecurityGroupRuleId | [0]" \
    --output text)
  [ "$RULE_ID" = "None" ] || [ -z "$RULE_ID" ] && { echo "puerto $PORT: sin regla, salto"; continue; }
  aws ec2 modify-security-group-rules --region "$REGION" --group-id "$SG_ID" \
    --security-group-rules "SecurityGroupRuleId=$RULE_ID,SecurityGroupRule={IpProtocol=tcp,FromPort=$PORT,ToPort=$PORT,CidrIpv4=$MYIP,Description=auto-mi-ip}"
  echo "puerto $PORT: regla $RULE_ID -> $MYIP"
done
```

</details>

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

<details>
<summary>🖱️ A mano en la consola AWS — key pair + rol de la EC2</summary>

1. **EC2 → Key pairs → Actions → Import key pair**: nombre `pyspark-stack-key`, pegá el
   contenido de `~/.ssh/pyspark_stack.pub`.
2. **IAM → Roles → Create role** → *Trusted entity*: **AWS service → EC2**.
3. Adjuntá la managed policy **`AmazonSSMManagedInstanceCore`** (habilita SSM sin abrir puertos).
4. Nombre `pyspark-stack-ec2-role` → *Create role*. Al asignarle el rol a la EC2 desde la
   consola, el *instance profile* homónimo se crea solo.

</details>

### 5.3 EC2 + EBS + user_data

```hcl
# infra/prod/ec2.tf
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    # "2023*" excluye las variantes minimal/ecs, que no traen el agente SSM (lo usa toda la §7).
    values = ["al2023-ami-2023*-x86_64"]
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
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    # Identificador exacto del EBS persistente. El script lo usa para resolver el NVMe correcto
    # antes de montar o formatear; no depende del orden nvme1n1/nvme2n1.
    data_volume_id = aws_ebs_volume.data.id
  })
  user_data_replace_on_change = true

  # IMDSv2 obligatorio: un SSRF en Airflow/Grafana no puede robar las credenciales
  # del instance profile. hop_limit = 2: los contenedores llegan al IMDS cruzando el bridge
  # de Docker (+1 hop); con el default (1) el token no llega y s3a con rol IAM falla.
  metadata_options {
    http_tokens                 = "required"
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 2
  }

  # Name = "<prefix>-node" (el workflow de CI busca la instancia por este tag);
  # AutoStartStop = "true" (la Lambda startstop filtra por él).
  tags = {
    Name          = "${var.name_prefix}-node"
    AutoStartStop = "true"
  }
}

# EIP: sin ella, cada stop/start cambiaría la IP pública (túneles SSH, output public_ip).
# Costo: AWS cobra toda IPv4 pública (~$3.6/mes, ver tabla de §2), asociada o no.
resource "aws_eip" "pyspark" {
  domain = "vpc"
  tags   = { Name = "${var.name_prefix}-eip" }
}
resource "aws_eip_association" "pyspark" {
  instance_id   = aws_instance.pyspark.id
  allocation_id = aws_eip.pyspark.id
}

resource "aws_ebs_volume" "data" {
  # De la variable, NO de aws_instance.pyspark.availability_zone: así el volumen no se arrastra
  # detrás de la instancia si esta se recrea, y la AZ es un dato fijo del stack.
  availability_zone = var.availability_zone
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "${var.name_prefix}-data" } # ← el DLM (backups) respalda por este tag
  lifecycle {
    prevent_destroy = true # el disco de estado (Postgres/Prometheus/Loki) NO se borra por accidente
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
# Instala solo las dependencias requeridas. Los parches del sistema deben aplicarse mediante
# una actualización controlada de la AMI o una ventana de mantenimiento; `dnf update -y` en cada
# recreación hace que dos boots puedan producir hosts distintos.
dnf install -y docker git && systemctl enable --now docker

# Versiones PINEADAS (mismo criterio que las imágenes por @sha256): un boot de hoy y uno de dentro
# de 6 meses instalan lo mismo. Actualizalas a propósito, no dejes que "latest" decida por vos.
COMPOSE_VERSION=v5.3.1
BUILDX_VERSION=v0.35.0
DOCKER_CONFIG=/usr/local/lib/docker
mkdir -p $DOCKER_CONFIG/cli-plugins
# OJO: templatefile() trata TODO este archivo como plantilla, comentarios incluidos. Cualquier
# apertura de variable sin escapar rompe el parseo ("Invalid expression") aunque esté dentro de
# un comentario, o -si el parseo no rompe- Terraform la interpreta como variable SUYA y falla
# ("vars map does not contain key"). Para que bash expanda una variable en el boot hay que
# escribirla con el símbolo de pesos duplicado antes de la llave, como en la línea de abajo. Todo
# lo que agregues acá entre llaves y sea de bash necesita el mismo escape.
curl --fail --silent --show-error --location --retry 5 --retry-all-errors "https://github.com/docker/compose/releases/download/$${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
  -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
# El paquete `docker` de dnf en AL2023 no trae buildx (o trae uno viejo): sin esto, el paso 5 de
# infra/deploy revienta con "compose build requires buildx 0.17.0 or later" al hacer `up --build`.
curl --fail --silent --show-error --location --retry 5 --retry-all-errors "https://github.com/docker/buildx/releases/download/$${BUILDX_VERSION}/buildx-$${BUILDX_VERSION}.linux-amd64" \
  -o $DOCKER_CONFIG/cli-plugins/docker-buildx
chmod +x $DOCKER_CONFIG/cli-plugins/docker-buildx
usermod -aG docker ec2-user

# Disco de datos: resolver el NVMe por el ID exacto del volumen EBS recibido desde Terraform.
# AWS expone el serial como vol0123... (sin guion); nunca seleccionar "el primer nvme" porque la
# enumeración puede cambiar entre boots y formatear el dispositivo equivocado sería destructivo.
EXPECTED_VOLUME_ID="${data_volume_id}"
EXPECTED_SERIAL="$(printf '%s' "$EXPECTED_VOLUME_ID" | tr -d '-')"
DATA_DEV=""

for _ in $(seq 1 60); do
  while read -r dev serial; do
    if [ "$serial" = "$EXPECTED_SERIAL" ]; then
      DATA_DEV="/dev/$dev"
      break
    fi
  done < <(lsblk -ndo NAME,SERIAL)

  [ -n "$DATA_DEV" ] && break
  sleep 2
done

if [ -z "$DATA_DEV" ]; then
  echo "ERROR: no se encontró el volumen EBS esperado $EXPECTED_VOLUME_ID" >&2
  exit 1
fi

ROOT_SOURCE="$(findmnt -n -o SOURCE /)"
ROOT_PARENT="$(lsblk -n -o PKNAME "$ROOT_SOURCE" | head -n1)"
ROOT_DEV="/dev/$${ROOT_PARENT:-$(basename "$ROOT_SOURCE")}"
if [ "$DATA_DEV" = "$ROOT_DEV" ]; then
  echo "ERROR: el volumen de datos resuelto coincide con el dispositivo root" >&2
  exit 1
fi

if ! blkid "$DATA_DEV" >/dev/null 2>&1; then
  # Solo formatea el volumen exacto y únicamente si no tiene filesystem.
  mkfs -t xfs "$DATA_DEV"
fi

mkdir -p /data
mountpoint -q /data || mount "$DATA_DEV" /data
DATA_UUID="$(blkid -s UUID -o value "$DATA_DEV")"
grep -q "UUID=$DATA_UUID " /etc/fstab || echo "UUID=$DATA_UUID /data xfs defaults,nofail 0 2" >> /etc/fstab

chown -R ec2-user:ec2-user /data
# Bind mounts del compose de prod. Los UID están ligados a las imágenes pineadas; validar los
# permisos al actualizar imágenes.
mkdir -p /data/postgres /data/prometheus /data/grafana /data/loki
chown 65534:65534 /data/prometheus
chown 472:472     /data/grafana
chown 10001:10001 /data/loki
echo 'vm.max_map_count=262144' > /etc/sysctl.d/99-pyspark.conf && sysctl --system
```

<details>
<summary>🖱️ A mano en la consola AWS — EC2 + EBS + Elastic IP</summary>

1. **EC2 → Launch instance**: nombre `pyspark-stack-node` · AMI **Amazon Linux 2023 (x86_64)** ·
   tipo **t3.large** (solo orquestador; Spark corre en EMR Serverless) · key pair `pyspark-stack-key`.
2. *Network settings* → **Select existing security group** → `pyspark-stack-sg`.
3. *Configure storage*: root **40 GiB gp3, Encrypted** · *Add new volume* → **30 GiB gp3,
   Encrypted**, device `/dev/xvdf` (gp3 crece online, así que empezás chico; ver la nota de la
   variable `data_volume_gb`).
4. *Advanced details*:
   - **IAM instance profile** → `pyspark-stack-ec2-role`.
   - **Metadata version** → **V2 only (token required)** (IMDSv2 obligatorio) y **Metadata
     response hop limit** → **2** (sin esto los contenedores no alcanzan el IMDS y `s3a://`
     falla por credenciales).
   - **User data** → pegá el script de arriba tal cual.
5. *Tags*: `Name=pyspark-stack-node` y **`AutoStartStop=true`** (la Lambda de §5.4 filtra por
   este tag; el workflow de CI busca por `Name`).
6. **EC2 → Elastic IPs → Allocate Elastic IP address** → *Actions → Associate* con la instancia
   (sin EIP, la IP pública cambia en cada stop/start del ahorro automático).
7. **EC2 → Volumes**: etiquetá a mano el volumen de datos (30 GiB) con `Name=pyspark-stack-data`
   — el wizard de Launch instance no lo etiqueta, y sin ese tag el DLM de §6.3 no respalda nada.

</details>

### 5.4 Automatización: EventBridge + Lambda

En vez de apagar la EC2 a mano, una Lambda la prende/apaga y EventBridge Scheduler la dispara
por cron. Con Lambda (en lugar de que Scheduler llame a EC2 directo) podés personalizar la
lógica: no apagar si hay un DAG corriendo, avisar por SNS/Slack, o prender solo si hay trabajo
en cola. Esta automatización convierte los ~$60/mes fijos de la EC2 `t3.large` en ~$12 (8h×22d). Con Spark ya fuera
de la EC2 (EMR Serverless), esta palanca impacta **menos** que antes —lo que apagás es
una EC2 ya chica— pero sigue valiendo la pena si no necesitás Airflow encendido 24/7.

**Código de la Lambda — `infra/prod/lambda/startstop.py`:** el handler `stop` no apaga a ciegas:
antes consulta **si hay DAG runs activos en Airflow** (guardia anti-corte) y, si los hay, no apaga.
Así el apagado es *job-aware* — con varios DAGs, solo se apaga cuando el **último** terminó (§10.3).

```python
import os
import time
import boto3

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")

def _dags_activos(instance_id):
    """Cuenta los DAG runs en estado 'running' DENTRO de la EC2, vía SSM SendCommand.
    Guardia anti-corte: si hay alguno, NO apagamos (otro DAG sigue corriendo). Ante cualquier
    duda (comando fallido, salida no numérica) es conservador y devuelve >0 → no apagar."""
    # Airflow 3: contamos los DAG runs 'running' consultando la metadata DB desde el scheduler.
    # (Alternativas equivalentes: `airflow jobs check --job-type SchedulerJob` para salud del
    #  scheduler, o `airflow dags list-runs --state running` filtrando por DAG.)
    py = ("from airflow.models.dagrun import DagRun;"
          "from airflow.utils.state import DagRunState;"
          "print(len(DagRun.find(state=DagRunState.RUNNING)))")
    cmd = f'docker exec airflow-scheduler python -c "{py}"'
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment="startstop: chequeo de DAG runs activos",
        Parameters={"commands": [cmd]},
    )
    cid = resp["Command"]["CommandId"]
    inv = {"Status": "Pending"}
    for _ in range(20):                       # espera hasta ~40s a que el comando termine
        time.sleep(2)
        inv = ssm.get_command_invocation(CommandId=cid, InstanceId=instance_id)
        if inv["Status"] in ("Success", "Failed", "TimedOut", "Cancelled"):
            break
    if inv["Status"] != "Success":
        return 1                              # no pudimos verificar → conservador: no apagar
    try:
        return int(inv["StandardOutputContent"].strip().splitlines()[-1])
    except (ValueError, IndexError):
        return 1

def handler(event, context):
    """Prende o apaga las EC2 marcadas con el tag AutoStartStop=true.
    event = {"action": "start"} | {"action": "stop"} | {"action": "stop", "force": true}
    El stop es JOB-AWARE: no apaga si hay DAG runs corriendo (§10.3).
    Con force=true apaga igual; se reserva para una intervención manual de emergencia."""
    action   = event.get("action", "stop")
    tag_key  = os.environ.get("TAG_KEY", "AutoStartStop")
    tag_val  = os.environ.get("TAG_VALUE", "true")

    # Solo estados accionables: start sobre "stopping" lanza IncorrectInstanceState.
    states = ["stopped"] if action == "start" else ["running"]
    resp = ec2.describe_instances(Filters=[
        {"Name": f"tag:{tag_key}", "Values": [tag_val]},
        {"Name": "instance-state-name", "Values": states},
    ])
    ids = [i["InstanceId"] for r in resp["Reservations"] for i in r["Instances"]]
    if not ids:
        return {"msg": "no instances tagged", "action": action}

    if action == "start":
        ec2.start_instances(InstanceIds=ids)
    else:
        # --- GUARDIA ANTI-CORTE: no apagar si algún DAG sigue corriendo (§10.3) ---
        # La task trigger_stop del DAG invoca esta Lambda al terminar (trigger_rule=all_done);
        # con varios DAGs en vuelo, solo el ÚLTIMO en terminar la deja apagar.
        #
        # force=True saltea el guard y se conserva únicamente para una intervención manual de
        # emergencia. El schedule normal no lo envía: un control de costo no debe interrumpir un
        # DAG legítimo ni dejar a Airflow sin registrar correctamente el estado final.
        if event.get("force"):
            ec2.stop_instances(InstanceIds=ids)
            return {"action": action, "instances": ids, "forced": True}

        # Evaluar todas las instancias encontradas. El diseño normal tiene una sola, pero esta
        # iteración evita detener otras instancias etiquetadas si el stack se amplía o se duplica.
        blocked = {}
        safe_to_stop = []
        for instance_id in ids:
            activos = _dags_activos(instance_id)
            if activos > 0:
                blocked[instance_id] = activos
            else:
                safe_to_stop.append(instance_id)

        if safe_to_stop:
            ec2.stop_instances(InstanceIds=safe_to_stop)
        if blocked:
            return {
                "action": action,
                "stopped": safe_to_stop,
                "blocked": blocked,
                "msg": "hay DAG runs activos o el estado no pudo verificarse",
            }

    return {"action": action, "instances": ids}
```

**Terraform de la automatización — `infra/prod/automation.tf`:**

```hcl
data "archive_file" "startstop" {
  type        = "zip"
  source_file = "${path.module}/lambda/startstop.py"
  output_path = "${path.module}/lambda/startstop.zip"
}

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

data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]                       # Describe no admite ARN específico
  }
  statement {
    actions   = ["ec2:StartInstances", "ec2:StopInstances"]
    resources = ["*"]
    # least-privilege: solo instancias con el tag correcto
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/AutoStartStop"
      values   = ["true"]
    }
  }
  # Guardia anti-corte: el handler `stop` consulta los DAG runs activos vía SSM antes de apagar.
  statement {
    actions = ["ssm:SendCommand"]
    resources = [
      "arn:aws:ec2:${local.region}:${local.account_id}:instance/${aws_instance.pyspark.id}",
      "arn:aws:ssm:${local.region}::document/AWS-RunShellScript",
    ]
  }
  statement {
    actions   = ["ssm:GetCommandInvocation"]
    resources = ["*"]
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

# Sin esto, Lambda crea el log group solo en la primera invocación, con retención INFINITA por
# defecto — a este volumen no pesa en dólares, pero es basura acumulándose para
# siempre por descuido. `depends_on` en la Lambda de abajo es necesario: si Lambda llega primero,
# auto-crea el log group y este `resource` falla con ResourceAlreadyExistsException al aplicar.
resource "aws_cloudwatch_log_group" "startstop" {
  name              = "/aws/lambda/${var.name_prefix}-startstop"
  retention_in_days = 14
}

resource "aws_lambda_function" "startstop" {
  function_name    = "${var.name_prefix}-startstop"
  filename         = data.archive_file.startstop.output_path
  source_code_hash = data.archive_file.startstop.output_base64sha256
  handler          = "startstop.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.lambda.arn
  timeout          = 120   # el guard job-aware espera al SSM SendCommand (chequeo de DAG runs)
  environment {
    variables = { TAG_KEY = "AutoStartStop", TAG_VALUE = "true" }
  }
  depends_on = [aws_cloudwatch_log_group.startstop]
}

# Rol que EventBridge Scheduler asume para invocar la Lambda.
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
    # El schedule usa el mismo guard job-aware que el apagado disparado por los DAGs. Si el
    # estado no puede verificarse o hay ejecuciones activas, no apaga y deja evidencia en logs.
    # `force=true` queda disponible solo para una invocación manual de emergencia.
    input    = jsonencode({ action = "stop" })
  }
}
```

Los schedules quedan activos desde este mismo `apply`. En la ventana de stop, la Lambda intenta
apagar la EC2 únicamente cuando puede verificar que no hay DAG runs activos. Si el estado es
incierto o existe trabajo en ejecución, conserva la instancia encendida y registra el motivo.
Al volver a prender, Docker intenta recuperar los servicios mediante `restart: unless-stopped`;
la recuperación completa se confirma con los health checks y smoke tests del §8.1.

<details>
<summary>🖱️ A mano en la consola AWS — Lambda startstop + schedules</summary>

1. **Lambda → Create function**: *Author from scratch*, nombre `pyspark-stack-startstop`,
   runtime **Python 3.12** → pegá el código de `startstop.py` en el editor (`lambda_function.py`)
   y en *Runtime settings → Edit* cambiá el handler a **`lambda_function.handler`** (el default
   de la consola es `lambda_function.lambda_handler`, pero el código define `def handler`).
2. *Configuration → General configuration*: timeout **120 s** (el guard job-aware espera al SSM
   SendCommand del chequeo de DAG runs). *Environment variables*: `TAG_KEY=AutoStartStop`,
   `TAG_VALUE=true`.
3. *Configuration → Permissions* → clic en el rol de ejecución → **Add permissions → Create
   inline policy** → pestaña JSON → pegá los permisos del Terraform (`ec2:DescribeInstances` en
   `*`; `ec2:StartInstances`/`StopInstances` con condición `aws:ResourceTag/AutoStartStop=true`;
   `ssm:SendCommand` sobre el ARN de tu instancia y `AWS-RunShellScript`, más
   `ssm:GetCommandInvocation` en `*` para el chequeo de DAGs activos antes de apagar).
4. **EventBridge → Scheduler → Create schedule** ×2, ambos con *Flexible time window* **Off**
   y timezone **UTC** (la consola crea sola el rol que invoca la Lambda):
   - `pyspark-stack-start`: cron `0 11 ? * MON-FRI *` → target la Lambda, payload
     `{"action": "start"}`.
   - `pyspark-stack-stop`: cron `0 22 ? * MON-FRI *` → payload `{"action": "stop"}`.

</details>

> EventBridge Scheduler vs Rules: usamos Scheduler porque soporta cron con timezone nativo y un
> solo target limpio. Podría llamar a EC2 directo (universal target) sin Lambda, pero la Lambda
> permite personalizar: no apagar con jobs activos, notificar, etc.

El ciclo de stop/start conserva estas cuatro propiedades de diseño:

1. **`t3.large` burstable es lo correcto acá** — la EC2 ya no corre Spark: solo orquesta (Airflow +
   Postgres + monitoreo), una carga liviana y a ráfagas que pasa la mayor parte del tiempo idle,
   justo el perfil para el que los `t3` acumulan "CPU credits". El motivo por el que antes se
   exigía CPU dedicada (`m6i`) —las JVMs de Spark degradan en burstable— **se mudó a EMR
   Serverless** (§6.4), que corre Spark con su propio cómputo dedicado por-job. Sin Spark en la
   EC2, `t3.large` es más barato y suficiente.
2. **EBS `gp3` (no `gp2`)** — IOPS y throughput provisionados y constantes (3000 IOPS / 125 MB/s
   base). `gp2` usa un "burst balance" que se agota; `gp3` rinde igual antes y después de cada ciclo.
3. **Los datos persisten** — al *stop* la instancia conserva sus volúmenes EBS (root + `/data`).
   Postgres y las métricas siguen ahí; el data lake vive en S3. Nada se recalcula al prender.
4. **Docker intenta recuperar el stack** — Docker arranca en boot y `restart: unless-stopped`
   reinicia los contenedores previamente activos. Esto no sustituye los health checks: el montaje
   de `/data`, Postgres, Airflow y los servicios de monitoreo deben validarse después del arranque.

> Lo único más lento es la primera corrida de Spark tras un período idle (~1-2 min): es el *cold
> start* de EMR Serverless (aprovisionar los workers). Con `auto_stop` (idle 15 min, §6.4) la app
> se apaga sola y la próxima corrida vuelve a pagar ese arranque; es el costo único de escalar a
> cero, no una degradación sostenida. La EC2, al no correr Spark, arranca sin warmup relevante.

```bash
# comprobá (tras el apply de §5.5)
aws lambda invoke --function-name pyspark-stack-startstop \
  --cli-binary-format raw-in-base64-out --payload '{"action":"stop"}' /dev/stdout
# debe listar tu instancia, no {"msg": "no instances tagged"} (revisá el tag AutoStartStop).
# Nota: con el guard job-aware, si el chequeo SSM ve DAGs corriendo (o no puede verificar) devuelve
# {"msg": "N DAG run(s) activos, no apago"} — es lo esperado; probá el stop sin DAGs en vuelo.
```

### 5.5 Desplegar, subir código y túnel SSH

```hcl
# infra/prod/outputs.tf
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
```

Antes del `apply`, definí las dos variables sin default (`my_ip_cidr`, `ssh_public_key`) en un
`terraform.tfvars` — así el `apply` no las pide interactivamente y queda repetible. El archivo no se
commitea (`*.tfvars` está en el `.gitignore`, con la excepción `!*.tfvars.example`):

**Generalo con este comando, no lo escribas a mano.** Un placeholder tipeado a mano (tipo
`"203.0.113.7/32"`) es fácil de dejar sin reemplazar — si eso llega a un `apply`, el SG te abre
22/443 a una IP que no existe y te quedás sin SSH (la causa más común de "perdí el acceso", ver ⚠️
más abajo). El heredoc de acá resuelve tu IP real con `curl` en el momento, así el archivo nace
correcto siempre, la primera vez y cualquier vez que lo recrees desde cero (máquina nueva, clon
nuevo del repo — `terraform.tfvars` nunca se commitea):

```bash
cat > infra/prod/terraform.tfvars <<EOF
my_ip_cidr     = "$(curl -s https://checkip.amazonaws.com)/32"
ssh_public_key = "$(cat ~/.ssh/pyspark_stack.pub)"
EOF
cat infra/prod/terraform.tfvars   # confirmá que las dos líneas tienen valores reales, no vacíos
```

**Paso 0 — antes que nada, creá `docker-compose.prod.yml` en la raíz de tu repo LOCAL** (todavía no
existe). A diferencia de `docker-compose.yml` (el del dev local), **este NO es un override que se
fusiona**: es un archivo standalone, completo y autosuficiente, que arranca solo con
`-f docker-compose.prod.yml` — sin Spark, sin HDFS, sin Jupyter (acá no se usa: es solo un dashboard
de exploración de dev, y el ETL real de prod corre por Airflow + EMR Serverless, no interactivo —
si alguna vez necesitás explorar datos productivos, hacé un túnel a un Jupyter local apuntando a
S3, no lo agregues a esta caja).
Es obligatorio, no opcional: si le hicieras `docker compose up` pelado a `docker-compose.yml` (el
de dev), levantarías Spark standalone + HDFS en la EC2 orquestadora, justo lo que este stack evita
moviendo Spark a EMR Serverless (§1, §6.4).

**Paso 0b — creá también `Dockerfile.airflow.prod`** (junto a `docker-compose.prod.yml`, no
reemplaza a `Dockerfile.airflow`: ese sigue siendo el de `docker-compose.yml`/dev). `Dockerfile.airflow`
instala JDK 17 + Spark 4.0.3 + Hadoop CLI (~1.2 GB) para que el dev local pueda hacer `spark-submit`
contra el cluster standalone — en prod eso **nunca se ejecuta**: los jobs van a EMR Serverless
(§6.4) vía `EmrServerlessStartJobOperator`, que llama a la API de AWS, no a un binario `spark-submit`
local. Compartir el mismo Dockerfile pesado en prod solo suma minutos de build y una descarga lenta
e inestable contra `archive.apache.org`, sin ningún beneficio:

```dockerfile
# Dockerfile.airflow.prod — imagen de PRODUCCIÓN (EC2 orquestador). A propósito NO instala
# JDK/Spark/Hadoop como Dockerfile.airflow: acá Airflow nunca corre spark-submit local.
FROM apache/airflow:3.2.2-python3.12

ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

Los providers de `requirements.txt` (`apache-airflow-providers-apache-spark`, que usan los DAGs de
ejemplo de §5, y luego `apache-airflow-providers-amazon` de §9.1) son paquetes Python puros: se
importan bien sin que Java/Spark estén instalados, solo fallarían si de verdad intentaras correr
`spark-submit` — y en prod eso no pasa nunca.

Esta es la versión **mínima** (Airflow + Postgres); las secciones 9 (provider amazon), 12 (monitoreo) y 13
(EMR Serverless env + secretos + hardening) van a pedirte que **reemplaces todo el archivo** por una
versión más completa (la final está en §14.1) — no lo vayas parcheando a mano por partes, total cada
sección te da el archivo entero de nuevo:

```yaml
# docker-compose.prod.yml — stack de PRODUCCIÓN, standalone (un solo archivo, sin merge).
# Arranque mínimo: Airflow + Postgres. Sin Spark/HDFS (esos jobs van a EMR Serverless, §6.4) y sin
# Jupyter (no se usa en prod: exploración interactiva queda para el dev local, docs/01).
# §9/§12/§13 amplían este mismo archivo — versión final en §14.1.
#   docker compose -f docker-compose.prod.yml up -d --build
x-airflow-common: &airflow-common
  image: pyspark_stack-airflow-prod:3.2.2
  build:
    context: .
    dockerfile: Dockerfile.airflow.prod   # liviana (Paso 0b): sin JDK/Spark/Hadoop
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@airflow-db:5432/${POSTGRES_DB:-airflow}
    AIRFLOW__CORE__LOAD_EXAMPLES: 'False'
    # El scheduler/worker habla con el api-server via la Task Execution API (Airflow 3); debe
    # apuntar al hostname del contenedor, NO a localhost.
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
    AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW_JWT_SECRET:-change-me-in-prod}'
    AIRFLOW_UID: 50000
  volumes:
    - ./dags:/opt/airflow/dags
  # Sin este bloque en cada servicio, `restart`/`logging` no se hereda desde acá con `<<: *airflow-common`
  restart: unless-stopped
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }
  networks:
    - hadoopnet

services:
  airflow-db:
    image: postgres:16
    container_name: airflow-db
    restart: unless-stopped
    logging: { driver: json-file, options: { max-size: "10m", max-file: "3" } }
    deploy: { resources: { limits: { memory: 512m } } } # calibrado a t3.large 8GB, sin Spark compitiendo
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}
      - POSTGRES_DB=${POSTGRES_DB:-airflow}
    volumes:
      - /data/postgres:/var/lib/postgresql/data   # EBS persistente (§5.3), no un volumen Docker
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER:-airflow}"]
      interval: 5s
      timeout: 5s
      retries: 10
    networks:
      - hadoopnet

  # Init one-shot: migra el esquema (core + FAB) y crea el admin, luego sale.
  airflow-init:
    <<: *airflow-common
    container_name: airflow-init
    restart: "no"   # one-shot: pisa el `unless-stopped` heredado, no reintenta en loop
    depends_on:
      airflow-db: { condition: service_healthy }
    command: >
      bash -c "
        airflow db migrate &&
        airflow fab-db migrate &&
        airflow users create --username ${AIRFLOW_ADMIN_USER:-admin} --firstname Admin --lastname User --role Admin --email admin@example.com --password ${AIRFLOW_ADMIN_PASSWORD:-admin} || true"

  airflow-apiserver:
    <<: *airflow-common
    container_name: airflow-apiserver
    command: api-server
    ports:
      - "8082:8080"
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }

  airflow-dag-processor:
    <<: *airflow-common
    container_name: airflow-dag-processor
    command: dag-processor
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }

  airflow-triggerer:
    <<: *airflow-common
    container_name: airflow-triggerer
    command: triggerer
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }

networks:
  hadoopnet:
```

Con eso creado, siguen los 6 pasos de infra/deploy. **Corrélos desde la raíz del repo** (no hace
falta entrar a `infra/prod`: el `-chdir` de Terraform se encarga). El paso 2 define `$IP`, que usan
del 3 al 6 — si abrís una terminal nueva a mitad de camino, volvé a definirla.

```bash
# ─── 1. Crear la infra ──────────────────────────────────────── (~3-4 min) ───
# Red + IAM + EC2 + EBS + auto start/stop: todo lo definido hasta esta sección.
terraform -chdir=infra/prod init
terraform -chdir=infra/prod apply

# ─── 2. Esperar el primer boot ──────────────────────────────── (~2-5 min) ───
# La instancia aparece como "running" mucho antes de terminar el user_data. Este wait
# corta recién cuando pasa los status checks; sin él, el rsync del paso 3 falla por
# "connection refused" (sshd todavía no levantó).
IP=$(terraform -chdir=infra/prod output -raw public_ip)
aws ec2 wait instance-status-ok \
  --instance-ids "$(terraform -chdir=infra/prod output -raw instance_id)"

# La EIP (public_ip) es estable entre stop/start, PERO si el `apply` del paso 1 reemplazó la
# instancia (-/+ en el plan: pasa con cambios al SG, al user_data, a la AMI...) la EC2 nueva
# generó una host key SSH nueva. Tu ~/.ssh/known_hosts todavía tiene la vieja asociada a esa
# misma IP → rsync y ssh fallan con "Host key verification failed" o el warning de "REMOTE HOST
# IDENTIFICATION HAS CHANGED". Limpiala siempre acá, sin preguntar (si no hubo reemplazo, esto
# no hace nada):
ssh-keygen -f ~/.ssh/known_hosts -R "$IP" >/dev/null 2>&1 || true

# ─── 3. Subir el código ──────────────────────────────────────────────────────
# --exclude '.env': el .env local (dev) no debe pisar el de prod, que lo genera
# load-secrets.sh en la EC2 desde SSM (§13.1). 'infra' tampoco viaja: vive en tu máquina.
# docker-compose.prod.yml (Paso 0) SÍ viaja: no tiene --exclude.
rsync -avz --exclude '.git' --exclude 'infra' --exclude '.env' --exclude '__pycache__' \
  -e "ssh -i ~/.ssh/pyspark_stack" ./ ec2-user@$IP:/home/ec2-user/pyspark_stack/

# ─── 4. Confirmar que el user_data terminó ───────────────────────────────────
# Espera a cloud-init y verifica las dos cosas que instala: Compose y /data montado.
# Si /data no aparece, el boot falló: mirá /var/log/cloud-init-output.log en la EC2.
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'cloud-init status --wait && docker compose version && df -h /data | tail -1'

# ─── 5. Levantar el stack (sin Spark/HDFS: no hacen falta en la EC2) ─────────
# (la 1ª vez tarda un par de minutos: instala los providers de requirements.txt; con
# Dockerfile.airflow.prod ya NO baja JDK/Spark/Hadoop, así que no depende de archive.apache.org) ───
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'cd pyspark_stack && docker compose -f docker-compose.prod.yml up -d --build'

# ─── 6. Abrir el túnel a las UIs ───────── (deja la terminal ocupada: es así) ───
# Es exactamente el output tunnel_command. Abrí las UIs en otra terminal/navegador.
ssh -i ~/.ssh/pyspark_stack -L 8082:localhost:8082 ec2-user@$IP
```

UIs (con el túnel abierto): Airflow `localhost:8082` — o, si exponés la web por HTTPS (§5.6),
directo en `https://airflow.midominio.com` sin túnel. Spark ya no corre en la EC2 (los jobs van a
EMR Serverless — su UI de Spark y sus logs se ven desde la consola de EMR / CloudWatch / S3, §12.8).
No hay Jupyter en prod (§5.5): la exploración interactiva queda para el stack local (`docs/01`).

> **Esto es orientación sobre lo que viene, no un paso para ejecutar ahora** — seguí con §5.6 abajo.
>
> Lo aplicado hasta acá (§1-5) es el núcleo, no la infra final: se arma incrementalmente a medida que
> avanzás por la guía. El `apply` que ya corriste crea solo lo definido hasta la §5. Las secciones 6-7
> (data lake S3, orquestación), 11 (CI/CD) y 13 (secretos) van a agregar más `.tf` a `infra/prod/`;
> recién ahí, en cada una de esas secciones, corresponde volver a correr `terraform apply` — no hace
> falta hacerlo ahora. Del mismo modo, el `docker-compose.prod.yml` del Paso 0 arranca a propósito
> **sin** Spark/HDFS (nunca estuvieron en el archivo): recién en §12-14 ese archivo se reemplaza por una
> versión más completa (monitoreo, env de EMR Serverless, hardening, secretos desde SSM), y **ahí** —no
> acá— el comando para levantar el stack pasa a ser (§14.1 final):
> `./scripts/load-secrets.sh && docker compose -f docker-compose.prod.yml up -d`.

---

### 5.6 Exponer la web de Airflow (HTTPS nativo, solo tu IP)

Hasta acá **nada** estaba expuesto: veías Airflow tuneleando `-L 8082`. Práctico para operar, incómodo
para *seguir los DAGs* desde el navegador. Esta sección publica **solo la web de Airflow** por
**HTTPS (443) restringida a tu IP** — el resto (Grafana/Prometheus/Loki) sigue por túnel.

Cuatro piezas, todas parametrizadas (nada hardcodeado — sale de `terraform output`):

1. **DNS** — un `A record` `airflow.midominio.com → EIP` de la EC2, gestionado por Terraform.
2. **Cert** — Let's Encrypt por **DNS-01** con `certbot/dns-route53`: usa el **rol de la EC2** para
   crear el TXT del reto en Route 53. **No abre el puerto 80** (encaja con el SG cerrado a tu IP).
3. **TLS nativo** — el `api-server` de Airflow sirve HTTPS él mismo (`AIRFLOW__API__SSL_CERT/KEY`).
   Cero contenedores extra. (En Airflow 3 la config del webserver se mudó a la sección **`[api]`**;
   los nombres `AIRFLOW__API__SSL_CERT` / `SSL_KEY` / `BASE_URL` son los de 3.2, verificados contra la
   [config reference oficial](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)
   — ya **no** son los `AIRFLOW__WEBSERVER__*` de Airflow 2.)
4. **SG** — 443 abierto **solo a `var.my_ip_cidr`** (ya lo agregó el `dynamic "ingress"` de §5.1).

> **El problema que hay que resolver (documentado oficialmente).** En Airflow 3 el
> `api-server` sirve en el **mismo puerto 8080** la UI, la API REST **y** la *Task Execution API*
> (`/execution/`) que el scheduler usa internamente. Al activar TLS, *todo* 8080 pasa a HTTPS, incluido
> ese tráfico interno. El cert es para `airflow.midominio.com`, pero los contenedores se hablan por el
> hostname `airflow-apiserver` → la verificación TLS **fallaría** y las tasks dejarían de correr (es
> exactamente el [howto oficial de self-signed cert](https://airflow.apache.org/docs/apache-airflow/stable/howto/run-with-self-signed-certificate.html)
> y los issues [#55147](https://github.com/apache/airflow/issues/55147) / [#53493](https://github.com/apache/airflow/issues/53493)).
>
> El howto oficial lo resuelve **metiendo `localhost` y `airflow-apiserver` como SANs del certificado**
> — pero eso **solo sirve con un cert self-signed que vos generás**. Con un cert **Let's Encrypt** no
> podés: LE solo firma dominios públicos que controlás, no hostnames internos. **La solución correcta
> para un cert público** es al revés: darle al contenedor un **alias de red = el FQDN del cert** y
> apuntar `EXECUTION_API_SERVER_URL` a ese FQDN. Así el hostname interno pasa a ser
> `airflow.midominio.com` (que *sí* está en el cert), la verificación TLS pasa contra las CAs públicas
> (sin `ssl_ca`) y el tráfico sigue por el bridge de Docker (no sale a internet). Es la razón por la que
> un reverse-proxy (Caddy, al final) evita todo esto — pero con el SG cerrado a tu IP, el TLS nativo es
> el camino más directo (Caddy necesitaría el puerto 80 abierto al mundo, ver abajo).

**Terraform — `infra/prod/dns.tf`** (todo condicionado a `var.airflow_domain`: vacío ⇒ no crea nada):

```hcl
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
```

**Política en archivo aparte — creá `infra/prod/policies/route53-certbot.json.tftpl`** con:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "Route53ChangeRecordsInZone", "Effect": "Allow",
      "Action": ["route53:ChangeResourceRecordSets"],
      "Resource": ["arn:aws:route53:::hostedzone/${zone_id}"] },
    { "Sid": "Route53ReadForDns01", "Effect": "Allow",
      "Action": ["route53:GetChange", "route53:ListHostedZones", "route53:ListResourceRecordSets"],
      "Resource": ["*"] }
  ]
}
```

> **Convención del repositorio:** las políticas IAM viven en `infra/prod/policies/*.json` (o `.json.tftpl`
> si necesitan interpolar, como el `zone_id` acá) y el `.tf` las referencia con
> `file()`/`templatefile()`. Podés migrar las políticas inline de §6.2/§16.3 a este mismo esquema.

**Terraform — outputs (agregá a `infra/prod/outputs.tf`)** — de acá saca todo el resto:

```hcl
# infra/prod/outputs.tf — agregá estos 3 al final del archivo que ya tenías (§5.3/§5.5). No
# los pegues en terraform.tfvars: outputs van en un output "..." { value = ... }, tfvars son
# asignaciones sueltas (bloque de abajo) — mezclarlos rompe el parseo de Terraform.
output "airflow_domain" { value = var.airflow_domain }
output "airflow_url" {
  value = var.airflow_domain == "" ? "(no expuesto: solo túnel SSH)" : "https://${var.airflow_domain}"
}
# Lo consume el comando de emisión del cert (abajo), para no repetir el email a mano.
output "letsencrypt_email" { value = var.letsencrypt_email }
```

<details>
<summary>🖱️ A mano en la consola AWS — A record + permiso DNS-01 del rol EC2</summary>

1. **Route 53 → Hosted zones** → entrá a tu zona (`midominio.com`) → **Create record**:
   *Record name* `airflow` (o el subdominio que uses) · *Record type* **A** · *Value* la **Elastic
   IP** de la EC2 (§5.3, output `public_ip`) · *TTL* `300` · routing **Simple**.
2. Anotá el **Hosted zone ID** de esa zona (columna *Hosted zone ID*): lo necesita el paso 3.
3. **IAM → Roles** → el rol de la EC2 (`pyspark-stack-ec2-role`) → *Add permissions → Create
   inline policy → JSON*. Pegá el documento de `policies/route53-certbot.json.tftpl`
   reemplazando `${zone_id}` por el ID del paso 2. Nombre: `ec2-route53-certbot`.
4. Verificá con `dig +short airflow.midominio.com`: tiene que devolver la Elastic IP antes de
   pedir el certificado. Sin el paso 3, certbot falla al crear el registro TXT del reto DNS-01.

**El alcance importa:** la política habilita `route53:ChangeResourceRecordSets` **solo** sobre esa
zona. Si la ampliás a `*`, cualquier proceso de la EC2 puede reescribir todo tu DNS.

</details>

**Definí las variables** en `terraform.tfvars` (§5.5) — vacías = no exponer:

```hcl
# infra/prod/terraform.tfvars — agregá estas 3 líneas al archivo que ya tenías (my_ip_cidr,
# ssh_public_key, §4). Es el ÚNICO archivo donde va esta sintaxis de asignación suelta.
airflow_domain    = "airflow.midominio.com"   # el FQDN de la web
dns_zone          = "midominio.com"           # tu hosted zone en Route 53
letsencrypt_email = "tu@email.com"
```

**Emitir el cert (una vez), todo con `terraform output`** — cero literales a mano:

Cuatro pasos, **desde la raíz del repo** (igual que la §5.5). Los pasos 3 y 4 usan las variables que
define el 2, así que corrélos en la misma terminal.

```bash
# ─── 1. Aplicar ──────────────────────────────────────────────────────────────
# Crea el A record en Route 53 y le da al rol de la EC2 el permiso para escribir
# en esa zona (lo necesita el desafío DNS-01 del paso 4).
terraform -chdir=infra/prod apply

# ─── 2. Leer los datos del state ─────────────────────────────────────────────
# Todo sale de terraform output: nada se escribe a mano, nada se desincroniza.
DOMAIN=$(terraform -chdir=infra/prod output -raw airflow_domain)
EMAIL=$(terraform -chdir=infra/prod output -raw letsencrypt_email)
IP=$(terraform -chdir=infra/prod output -raw public_ip)

# ─── 3. Verificar el DNS antes de pedir el cert ──────────────────────────────
# Tiene que imprimir la EIP. Si sale vacío, el A record todavía no propagó:
# esperá un minuto y repetí. Pedir el cert antes de que resuelva lo hace fallar
# y Let's Encrypt limita los reintentos (5 fallos por hora por dominio).
dig +short "$DOMAIN"

# ─── 4. Emitir el cert (una sola vez) ────────────────────────────────────────
# Desafío DNS-01: certbot crea un registro TXT temporal usando el rol de la EC2
# vía IMDS (sin access keys) y lo borra al terminar. No abre el puerto 80, así
# que el SG sigue cerrado salvo tu IP.
ssh -i ~/.ssh/pyspark_stack ec2-user@"$IP" "
  sudo docker run --rm -v /data/certs:/etc/letsencrypt certbot/dns-route53 certonly \
    --dns-route53 -d '$DOMAIN' -m '$EMAIL' --agree-tos -n &&
  sudo chmod -R g+rX /data/certs
"
# El chmod es necesario: el api-server corre con gid 0 (grupo root), y sin el
# permiso de grupo no puede leer privkey.pem — el contenedor arranca y muere.
```

El cert queda en `/data/certs/live/$DOMAIN/{fullchain.pem,privkey.pem}` (en el EBS, sobrevive al
stop/start de la EC2).

**Compose — activar el TLS nativo, editando el `airflow-apiserver` de tu `docker-compose.prod.yml`
directamente** (ya no hay dos archivos que fusionar: es el único archivo, así que esto se edita
in-place).

> **¿Dónde se edita esto — en tu máquina o en la EC2?** Siempre en tu **repo LOCAL**, el mismo
> `docker-compose.prod.yml` que creaste en el Paso 0. Es el mismo patrón de §5.5: editás local →
> `rsync` lo sube → `ssh` lo levanta en la EC2. **No lo edites a mano dentro de la EC2** (por ejemplo
> con `vim` en una sesión `ssh`): funciona en el momento, pero la próxima vez que corras el `rsync`
> del Paso 3 (§5.5) para subir código nuevo, ese `rsync` **pisa** el `docker-compose.prod.yml` de la
> EC2 con la versión vieja de tu repo local — sin el bloque TLS — y perdés el cambio sin ningún
> aviso. Editá siempre acá, en tu máquina, y volvé a desplegar (más abajo, en "Verificar").

El FQDN viaja como `AIRFLOW_DOMAIN` (no es secreto): agregalo a tu `.env` **local**, junto con las
4 variables derivadas que arma el bloque de abajo (`AIRFLOW_BASE_URL`, `AIRFLOW_EXECUTION_API_URL`,
`AIRFLOW_SSL_CERT`, `AIRFLOW_SSL_KEY`):

```bash
# EN TU MÁQUINA (repo local) — misma terminal donde corriste terraform: infra/ y su state
# viven acá, no en la EC2 (§5.5).
DOMAIN=$(terraform -chdir=infra/prod output -raw airflow_domain)
{
  echo "AIRFLOW_DOMAIN=$DOMAIN"
  echo "AIRFLOW_BASE_URL=https://$DOMAIN"
  echo "AIRFLOW_EXECUTION_API_URL=https://$DOMAIN:8080/execution/"
  echo "AIRFLOW_SSL_CERT=/opt/airflow/certs/live/$DOMAIN/fullchain.pem"
  echo "AIRFLOW_SSL_KEY=/opt/airflow/certs/live/$DOMAIN/privkey.pem"
} >> .env
```

> **Ojo: este `.env` que acabás de tocar es el de tu repo LOCAL — no llega solo a la EC2.** El
> `rsync` del Paso 3 (§5.5) excluye `.env` a propósito (`--exclude '.env'`), porque el `.env` de la
> EC2 lo genera `load-secrets.sh` desde SSM (§13.1): son dos archivos distintos que nunca se pisan
> entre sí. Sumá estas 5 variables también del lado de la EC2, por uno de estos dos caminos:
>
> - **Recomendado** (mismo mecanismo que el resto de los secretos): agregalas a SSM junto a las
>   demás (§13.1) — `load-secrets.sh` las baja solas la próxima vez que corras
>   `./scripts/load-secrets.sh && docker compose -f docker-compose.prod.yml up -d` en la EC2.
> - **Directo**, mismo patrón que el comando de certbot de arriba (variable local, comando remoto
>   por `ssh`):
>
>   ```bash
>   IP=$(terraform -chdir=infra/prod output -raw public_ip)
>   ssh -i ~/.ssh/pyspark_stack ec2-user@"$IP" "cd pyspark_stack && {
>     echo AIRFLOW_DOMAIN=$DOMAIN
>     echo AIRFLOW_BASE_URL=https://$DOMAIN
>     echo AIRFLOW_EXECUTION_API_URL=https://$DOMAIN:8080/execution/
>     echo AIRFLOW_SSL_CERT=/opt/airflow/certs/live/$DOMAIN/fullchain.pem
>     echo AIRFLOW_SSL_KEY=/opt/airflow/certs/live/$DOMAIN/privkey.pem
>   } >> .env"
>   ```
>
> **Si tu `.env` de la EC2 ya tenía `AIRFLOW_DOMAIN` de una corrida vieja de este comando (antes de
> sumar `AIRFLOW_SSL_CERT`/`AIRFLOW_SSL_KEY` acá), el bloque `{ ... } >> .env` de arriba solo
> AGREGA líneas al final — no pisa las viejas — así que corré `grep AIRFLOW_SSL .env` en la EC2
> después y, si no aparece nada, agregalas a mano (mismo `echo ... >> .env`, sin las 3 primeras
> líneas que ya tenías).**

Reemplazá el bloque `airflow-apiserver` (en tu `docker-compose.prod.yml` **local**) por este — nota
el `<<: *airflow-common-env` (el anchor **anidado** que definís junto con `x-airflow-common`,
§14.1): permite sumar las claves de TLS sin repetir todo el resto del environment a mano:

```yaml
services:
  airflow-apiserver:
    <<: *airflow-common
    container_name: airflow-apiserver
    command: api-server
    environment:
      <<: *airflow-common-env
      # SSL_CERT/SSL_KEY vienen de AIRFLOW_SSL_CERT/AIRFLOW_SSL_KEY, dos variables aparte que se
      # completan en el `.env` (mismo motivo que BASE_URL/EXECUTION_API_URL más abajo: docker
      # compose NO soporta interpolación anidada, "${AIRFLOW_DOMAIN:+/opt/.../${AIRFLOW_DOMAIN}/x}"
      # da "" SIEMPRE aunque AIRFLOW_DOMAIN esté seteado — se prueba con `docker compose config`,
      # pasó justo eso probando este archivo). Sin ellas en el .env (default, modo túnel-only)
      # ambas quedan en "" y Airflow sirve HTTP plano — es `if ssl_cert:` en
      # airflow/cli/commands/api_server_command.py::_get_ssl_cert_and_key_filepaths(), un string
      # vacío es falsy ahí. Si en cambio apuntaran siempre a una ruta fija, el api-server queda en
      # restart loop buscando un cert que nunca se generó apenas hacés `docker compose up` sin
      # haber corrido certbot todavía — pasó justo eso en un deploy real, con el puerto 8082 del
      # túnel dando "Connection refused" porque el contenedor nunca llegaba a levantar. Con esto
      # el mismo archivo sirve para los dos modos, sin tener que acordarte de "no tocar nada".
      AIRFLOW__API__SSL_CERT: '${AIRFLOW_SSL_CERT:-}'
      AIRFLOW__API__SSL_KEY:  '${AIRFLOW_SSL_KEY:-}'
      # BASE_URL y EXECUTION_API_SERVER_URL SÍ necesitan el valor de AIRFLOW_DOMAIN adentro (no
      # solo su presencia como en SSL_CERT/KEY arriba), y por la misma limitación de interpolación
      # anidada van por dos variables aparte, que completás en el `.env` recién ACÁ, cuando de
      # verdad configurás HTTPS (mismo patrón que el `echo AIRFLOW_DOMAIN=... >> .env` de arriba):
      #   echo "AIRFLOW_BASE_URL=https://$(terraform -chdir=infra/prod output -raw airflow_domain)" >> .env
      #   echo "AIRFLOW_EXECUTION_API_URL=https://$(terraform -chdir=infra/prod output -raw airflow_domain):8080/execution/" >> .env
      # Sin ellas (default, modo túnel-only) BASE_URL queda "" → Airflow arma el `<base href="/">`
      # del HTML relativo, funciona bien detrás del túnel a 8082. Si quedara fijo en
      # "https://${AIRFLOW_DOMAIN}" con el dominio vacío, el resultado es el string literal
      # "https://" y el `<base href>` sale roto: el navegador resuelve cualquier link relativo
      # (p.ej. el de login, "auth/login/?next=%2F") contra ESE base roto, y termina pidiendo
      # "https://auth/login/?next=%2F" (toma "auth" como si fuera el hostname) — pasó justo eso.
      AIRFLOW__API__BASE_URL: '${AIRFLOW_BASE_URL:-}'
      AIRFLOW__CORE__EXECUTION_API_SERVER_URL: '${AIRFLOW_EXECUTION_API_URL:-http://airflow-apiserver:8080/execution/}'
    ports:
      - "8082:8080"                                          # túnel local (seguís pudiendo usarlo)
      - "443:8080"                                            # HTTPS público; el SG lo limita a tu IP
    volumes:
      - ./dags:/opt/airflow/dags                              # el `<<:` no mergea `volumes`, hay que repetirlo
      # Monta la raíz de certbot completa, NO solo live/$AIRFLOW_DOMAIN: certbot deja
      # live/$DOMINIO/fullchain.pem como symlink RELATIVO a ../../archive/$DOMINIO/fullchain1.pem
      # (`ls -la /data/certs/live/$DOMINIO` lo muestra). Si montás nada más que la carpeta
      # `live/$DOMINIO`, ese `../../archive/...` resuelve DENTRO del contenedor contra
      # `/opt/archive/...`, que no existe — el symlink queda roto aunque el archivo exista en el
      # host, y el api-server tira "SSL related file does not exist /opt/airflow/certs/.../
      # fullchain.pem" (pasó justo eso en un deploy real). Montando `/data/certs` entero, `live/`
      # y `archive/` quedan hermanos igual que en el host y el symlink relativo resuelve bien.
      - /data/certs:/opt/airflow/certs:ro
    networks:
      hadoopnet:
        aliases: ["${AIRFLOW_DOMAIN}"]                       # <- adentro, el cert matchea este nombre
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }
```

> `<<:` en YAML mergea al nivel del mapping donde se usa: como acá reemplazás `environment`,
> `ports`, `volumes` y `networks` del servicio con tus propios valores, tenés que repetir lo que
> ya tenías en cada uno (por eso `./dags:/opt/airflow/dags` aparece de nuevo). Con
> `AIRFLOW_BASE_URL`/`AIRFLOW_EXECUTION_API_URL`/`AIRFLOW_SSL_CERT`/`AIRFLOW_SSL_KEY` sin definir en
> el `.env` (default de las cuatro: `""`, el hostname interno, `""` y `""`), este mismo bloque es
> seguro **aunque `airflow_domain` esté vacío** (sirve HTTP plano por el túnel 8082, sin crashear
> buscando un cert que no existe y sin el `<base href>` roto de la UI) — podés aplicar este
> `docker-compose.prod.yml` de una y recién sumar las 5 variables al `.env` cuando corras certbot
> más adelante, no hace falta coordinar el orden a mano.

**Renovación automática (una vez, EN LA EC2 — pegalo en la sesión `ssh` que ya tenés abierta, o
mandalo en un solo `ssh ec2-user@$IP '...'` desde tu máquina).** `certbot renew` es no-op si faltan
>30 días; corre semanal y recarga el cert reiniciando el api-server:

```bash
echo '0 3 * * 1 root docker run --rm -v /data/certs:/etc/letsencrypt certbot/dns-route53 renew --quiet && chmod -R g+rX /data/certs && docker restart airflow-apiserver' \
  | sudo tee /etc/cron.d/airflow-cert-renew
```

> **Chequeo previo — ¿el `.env` de la EC2 ya tiene `AIRFLOW_DOMAIN`?** El `up -d` del Paso 1 de abajo
> NO falla si no lo tiene: cae solo al modo túnel-only (HTTP plano en 8082/443, el fallback que
> describe el bloque de arriba) sin ningún error visible — así que si saltaste el paso "Directo"/SSM
> de más arriba, el resultado es un despliegue silenciosamente sin TLS, no un crash. Confirmalo
> ANTES de gastar tiempo debugueando un 2) que va a fallar por esto:
> ```bash
> ssh -i ~/.ssh/pyspark_stack ec2-user@"$(terraform -chdir=infra/prod output -raw public_ip)" \
>   'grep AIRFLOW_DOMAIN ~/pyspark_stack/.env 2>&1 || echo "falta: corré el paso Directo/SSM de arriba"'
> ```

**Verificar** — dos partes, cada una en su máquina:

```bash
# 1) EN TU MÁQUINA: subir el docker-compose.prod.yml editado (local) y redesplegar en la EC2 —
#    mismo Paso 3 + Paso 5 de §5.5 (rsync sube el código, ssh levanta el stack). El .env de la EC2
#    ya tiene AIRFLOW_DOMAIN gracias al paso de arriba (SSM o el ssh directo), así que este `up -d`
#    ya arranca en modo HTTPS:
IP=$(terraform -chdir=infra/prod output -raw public_ip)
rsync -avz --exclude '.git' --exclude 'infra' --exclude '.env' --exclude '__pycache__' \
  -e "ssh -i ~/.ssh/pyspark_stack" ./ ec2-user@$IP:/home/ec2-user/pyspark_stack/
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'cd pyspark_stack && docker compose -f docker-compose.prod.yml up -d'
# El `up -d` reimprime el progreso de cada contenedor a medida que arranca; vas a ver
# "Container airflow-init Exited" (varias veces, es el redraw del spinner, no 4 corridas
# distintas) — es intencional: airflow-init es one-shot (migra + crea el admin y sale, ver
# comentario en el compose más arriba), termina en exit 0 y se queda "Exited" para siempre,
# a diferencia de apiserver/scheduler/dag-processor/triggerer que quedan "Running". Si en vez
# de eso el que aparece en "Exited" es **apiserver**, ahí sí mirá los logs:
#   ssh -i ~/.ssh/pyspark_stack ec2-user@$IP docker logs airflow-apiserver --tail 50

# 2) EN TU MÁQUINA: verificar desde afuera (el SG solo deja pasar 443 a tu IP):
curl -sSfI "https://$(terraform -chdir=infra/prod output -raw airflow_domain)/" | head -1  # 200/302 desde tu IP
# Desde OTRA IP debe cortar (timeout): el SG solo deja 443 a var.my_ip_cidr.
```

Entrás a `https://airflow.midominio.com` con el usuario **admin** y la password que generó SSM
(§13.1). La restricción por IP es defensa-en-profundidad **sobre** el login de Airflow, no un reemplazo de este.

> **¿Cuál es la URL de Airflow ahora que tiene DNS/HTTPS?** Un solo comando, no hace falta acordarse
> del dominio a mano (`terraform output` ya lo tiene guardado en el state):
> ```bash
> terraform -chdir=infra/prod output -raw airflow_url
> # → https://airflow.midominio.com — antes de este §5.6 (sin DNS/TLS) era el túnel a
> #   localhost:8082 (tunnel_command, §5.5); después de este paso es esta URL pública directa.
> ```

> **Consecuencia en el túnel SSH (§5.5).** Con el TLS activo ya **no tuneleás Airflow**: entrás por la
> URL pública. El `-L 8082` del `tunnel_command` deja de aplicar para Airflow (si igual lo abrís,
> `localhost:8082` sirve HTTPS con el cert del FQDN → warning de nombre; usá la URL pública). El túnel
> sigue siendo para Grafana/Prometheus/Loki (`-L 9090 -L 3000 -L 9093 -L 3100`, §12.8). Es decir:
> la web de Airflow por 443, el resto por túnel.

<details>
<summary>🖱️ Alternativa: Caddy (reverse-proxy con auto-cert) en vez de TLS nativo</summary>

Caddy pide y **renueva** el cert solo (sin certbot ni cron) y evita el gotcha del alias — Airflow
queda en HTTP plano en 8080 y Caddy termina el TLS. **Pero** su emisión automática (HTTP-01/TLS-ALPN)
necesita el **puerto 80 abierto al mundo**; con el SG cerrado a tu IP, Let's Encrypt no llega y
tendrías que compilar Caddy con el módulo `caddy-dns/route53` (build custom) para usar DNS-01. Por eso,
con SG-a-tu-IP, el TLS nativo de arriba es más directo. Si igual preferís Caddy:

```yaml
# docker-compose.prod.yml — reemplaza el bloque TLS del api-server por este proxy
services:
  caddy:
    image: caddy:2
    restart: always
    ports: ["80:80", "443:443"]        # abrí 80 y 443 en el SG (al mundo si usás HTTP-01)
    volumes:
      - ./monitoring/caddy/Caddyfile:/etc/caddy/Caddyfile:ro
      - /data/caddy:/data
    networks: [hadoopnet]
```

```caddyfile
# monitoring/caddy/Caddyfile
{$AIRFLOW_DOMAIN} {
    reverse_proxy airflow-apiserver:8080
}
```

Con Caddy **no** pongas `AIRFLOW__API__SSL_*`, ni el alias, ni cambies `EXECUTION_API_SERVER_URL`
(el api-server sigue en HTTP interno). Trade-off: un contenedor más y el puerto 80 abierto (o build DNS).

</details>

---

## 6. Data lake en S3

Sin HDFS en prod, **todo el dato vive en S3**: es el data lake durable (sobrevive al apagado de la
EC2), barato, y la fuente/destino de los ETL (`raw/ → curated/ → analytics/`). Los jobs Spark de
**EMR Serverless** lo leen y escriben con `s3a://` usando **su propio rol de ejecución** (§6.4); las
tasks Python puro de Airflow (en la EC2) usan `s3://` con el **rol IAM de la EC2** — en ambos casos
sin access keys en disco. Esta sección crea los buckets (6.1), el permiso S3 del rol de la EC2 (6.2),
los backups del EBS (6.3), la app EMR Serverless con sus roles y su submit (6.4) y el VPC endpoint de
S3 (6.5).

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
  # OJO con la semántica: prevent_destroy NO saltea este recurso, ABORTA el `terraform destroy`
  # entero. Para el teardown de §8 hay que borrar esta línea primero. Y aun así el destroy falla
  # con BucketNotEmpty: el bucket tiene versionado, así que además de vaciarlo hay que borrar las
  # versiones y los delete markers (o agregar `force_destroy = true`).
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket" "artifacts" { bucket = local.artifacts }

# for_each necesita keys conocidas en tiempo de plan: un toset de ids fallaría en el PRIMER
# apply con "Invalid for_each argument"; un map con keys estáticas y values computados funciona.
locals {
  buckets = {
    datalake  = aws_s3_bucket.datalake.id
    artifacts = aws_s3_bucket.artifacts.id
  }
}

# Privados + cifrados + solo-TLS + versionado, para ambos buckets.
resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = local.buckets
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = local.buckets
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_versioning" "all" {
  for_each = local.buckets
  bucket   = each.value
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.buckets
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

# Lifecycle: transición a clases baratas para bajar el costo de almacenamiento.
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

<details>
<summary>🖱️ A mano en la consola AWS — buckets del data lake</summary>

1. **S3 → Create bucket** ×2: `pyspark-stack-datalake-<account-id>` y
   `pyspark-stack-artifacts-<account-id>` (us-east-1). En ambos: *Block Public Access* activado
   (default) · *Bucket Versioning* **Enable** · cifrado SSE-S3 (default).
2. Política solo-TLS: en cada bucket → *Permissions → Bucket policy* → pegá el JSON
   `DenyInsecureTransport` del Terraform (ajustando el nombre del bucket en los dos ARN).
3. Lifecycle (solo datalake): *Management → Create lifecycle rule* → nombre `tiering`, alcance
   todo el bucket → transiciones: **Standard-IA a los 30 días** y **Glacier Instant Retrieval a
   los 90**.
4. (Opcional) *Create folder* para `raw/`, `curated/`, `analytics/` — también aparecen solos con
   la primera escritura.

</details>

```bash
# comprobá
terraform -chdir=infra/prod apply          # crea los recursos nuevos de esta sección
aws s3 ls | grep pyspark-stack             # datalake + artifacts (2 buckets, además del tfstate)
```

### 6.2 IAM: permitir s3a a la EC2 (sin keys)

Se agrega una política al **rol de la EC2** (`aws_iam_role.ec2`, definido antes) para que las tasks
de Python puro de Airflow (pandas/`s3fs`, §9.0) lean y escriban en S3 con el *instance profile*, sin keys.
Los jobs Spark **no** usan este rol: corren en EMR Serverless con **su propio** rol de ejecución
(§6.4). El permiso para que Airflow *dispare* esos jobs (`emr-serverless:StartJobRun` + `PassRole`)
también se agrega al rol de la EC2, en §6.4.

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

<details>
<summary>🖱️ A mano en la consola AWS — permisos s3a del rol EC2</summary>

1. **IAM → Roles → `pyspark-stack-ec2-role` → Add permissions → Create inline policy** →
   pestaña JSON.
2. Pegá el documento del Terraform: `s3:GetObject/PutObject/DeleteObject` sobre
   `arn:aws:s3:::pyspark-stack-datalake-<acct>/*` y `.../artifacts-<acct>/*`, más
   `s3:ListBucket` + `s3:GetBucketLocation` sobre los ARN de los buckets (sin `/*`).
3. Nombre `ec2-s3a` → *Create policy*. No hay que tocar la EC2: el rol ya está asociado y los
   contenedores toman las credenciales del instance profile al instante.

</details>

En los jobs PySpark (que corren en **EMR Serverless**, §6.4), apuntá las rutas a `s3a://` — el rol de
ejecución de EMR resuelve las credenciales por sí solo, sin keys:

```python
df = spark.read.csv(f"s3a://{DATALAKE}/raw/customers.csv", header=True)
df.write.mode("overwrite").parquet(f"s3a://{DATALAKE}/curated/customers")
```

En las tasks Python puro de Airflow (en la EC2) es el mismo dato con `s3://` (pandas + `s3fs` toman
el instance profile de la EC2):

```bash
# comprobá
terraform -chdir=infra/prod apply   # crea la policy ec2-s3a de arriba — sin esto el s3 cp de abajo da AccessDenied
ACCT=$(aws sts get-caller-identity --query Account --output text)
# desde la EC2, para probar el instance profile (no tus keys locales)
ssh -i ~/.ssh/pyspark_stack ec2-user@"$IP" \
  'aws s3 cp /etc/hostname "s3://pyspark-stack-datalake-'"$ACCT"'/raw/smoke-iam.txt"'
```

### 6.3 Backups: snapshots EBS automáticos (DLM)

`/data` (EBS gp3) guarda Postgres + datos de monitoreo (Prometheus/Loki): es el estado que **no**
vive en S3 (sin HDFS, ya no hay bloques de datos ahí). **Data Lifecycle Manager (DLM)** toma
snapshots automáticos y retiene los últimos N — cero código.

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

<details>
<summary>🖱️ A mano en la consola AWS — snapshots automáticos (DLM)</summary>

1. **EC2 → Elastic Block Store → Lifecycle Manager → Create lifecycle policy** → tipo
   **EBS snapshot policy**.
2. *Target resources*: **Volume**, con tag `Name = pyspark-stack-data`.
3. *Schedule*: frecuencia **cada 24 h** a las **05:00 UTC**, retención **7** snapshots ·
   *Copy tags from source*: activado.
4. *IAM role*: dejá **Default role** (la consola usa el service role de DLM) · estado
   **Enable policy** → *Create*.

</details>

> Restore: creás un volumen desde el snapshot y lo montás en `/data` (o recreás la instancia con
> `user_data` que ya hace `mount ... /data`). S3 ya está versionado, así que el data lake
> tiene su propia protección.

```bash
# comprobá
terraform -chdir=infra/prod apply   # crea el rol DLM + la lifecycle policy de arriba
aws dlm get-lifecycle-policies --query 'Policies[].State'   # ["ENABLED"]
```

### 6.4 Cómputo Spark: EMR Serverless

Spark **salió de la EC2**. Los jobs corren en **EMR Serverless**: una aplicación Spark serverless
que arranca sola cuando llega un job, escala a cero cuando queda idle y **paga solo mientras
computa** (vCPU-seg + GB-seg). El *cold start* es ~1–2 min; a cambio, no hay cluster que mantener ni
instancia siempre encendida. Airflow (en la EC2) dispara cada job con `EmrServerlessStartJobOperator` y lo
pollea con `EmrServerlessJobSensor` (patrón de DAG en §9.0) — nunca corre `spark-submit` local.

**A) La aplicación EMR Serverless — `infra/prod/emr.tf` (archivo nuevo):**

```hcl
# infra/prod/emr.tf
resource "aws_emrserverless_application" "spark" {
  name          = "${var.name_prefix}-spark"
  type          = "SPARK"
  release_label = "emr-7.5.0"

  # Arranca sola al recibir un job y se apaga tras 15 min idle → escala a cero, cero mantenimiento.
  auto_start_configuration { enabled = true }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  # Techo de capacidad: acota el gasto máximo aunque un job pida de más.
  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  # network_configuration: NO hace falta para jobs S3-only (EMR sale por el service network de AWS).
  # Solo se agrega si el job accede a recursos DENTRO de tu VPC (RDS privada, ElastiCache, etc.):
  #   network_configuration {
  #     subnet_ids         = data.aws_subnets.default.ids
  #     security_group_ids = [aws_security_group.pyspark.id]
  #   }
}
```

**B) Rol de ejecución del job (least-privilege) — `infra/prod/emr.tf` (agregá esto al mismo
archivo, debajo de A):** EMR Serverless asume **este** rol para correr el Spark; solo puede tocar
los dos buckets y escribir sus logs. Sin Glue.

```hcl
data "aws_iam_policy_document" "emr_job_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "emr_job" {
  name               = "${var.name_prefix}-emr-serverless-job"
  assume_role_policy = data.aws_iam_policy_document.emr_job_assume.json
}

data "aws_iam_policy_document" "emr_job" {
  statement {
    sid       = "S3ReadWriteData"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/*", "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    sid       = "S3List"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.datalake.arn, aws_s3_bucket.artifacts.arn]
  }
  statement {   # el job escribe sus logs a este log group (cifrado, con retención — abajo)
    sid       = "CloudWatchLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*"]
  }
  # Tablas Iceberg (§16.1): el catálogo es Glue (GlueCatalog de Iceberg), así que el job necesita
  # leer/crear/actualizar la metadata de tabla ahí — sin esto el CREATE TABLE/INSERT del job falla
  # con AccessDenied al intentar registrar el snapshot nuevo en Glue.
  statement {
    sid     = "GlueCatalogIceberg"
    actions = [
      "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.analytics.arn,          # definida en §16.2
      "${aws_glue_catalog_database.analytics.arn}/*",   # Glue expone las tablas como sub-recurso de la DB
    ]
  }
}
resource "aws_iam_role_policy" "emr_job" {
  name   = "emr-serverless-job"
  role   = aws_iam_role.emr_job.id
  policy = data.aws_iam_policy_document.emr_job.json
}

# Logs del job cifrados con retención acotada (CloudWatch Logs cifra en reposo por defecto con
# clave AWS-managed; para KMS propia, agregá kms_key_id).
resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${var.name_prefix}"
  retention_in_days = 30
}

# Base de datos en el Glue Data Catalog: el catálogo lógico donde Iceberg (§16.1) registra las
# tablas de curated/analytics. Va acá (no en §16, que es la sección opcional de Athena) porque lo
# necesita el job Spark para escribir Iceberg, uses o no Athena para consultar después.
resource "aws_glue_catalog_database" "analytics" {
  name = "${replace(var.name_prefix, "-", "_")}_analytics"   # Glue no admite '-' en el nombre
}
```

**Config Iceberg del job (mismos `sparkSubmitParameters` de §9.0/§10.2, agregale estas líneas):**
el runtime `emr-7.5.0` trae el conector Iceberg embebido — no hay que instalar nada, solo
declarar el catálogo apuntando a Glue:

```text
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.glue_catalog.warehouse=s3://<datalake>/
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Con eso, el job escribe con `df.writeTo("glue_catalog.pyspark_stack_analytics.ventas").createOrReplace()`
(primera vez) o `.append()`/`.overwritePartitions()` (corridas siguientes) en vez de
`df.write.mode("overwrite").parquet(...)` — reemplaza el patrón de escritura de §9.0, no lo suma.

**C) Extensión del rol de la EC2 — `infra/prod/iam.tf` (agregar, junto al rol de la EC2):** permite que
Airflow (en la EC2) **envíe y consulte** jobs, y que **pase** el rol de ejecución a EMR Serverless. El
`iam:PassRole` con `iam:PassedToService` es la barrera: la EC2 puede pasar ese rol *solo* a EMR
Serverless, a nada más.

```hcl
# infra/prod/iam.tf   (agregar — permisos EMR Serverless para el rol de la EC2)
data "aws_iam_policy_document" "ec2_emr" {
  statement {
    sid = "EmrServerlessSubmit"
    actions = [
      "emr-serverless:StartJobRun",
      "emr-serverless:GetJobRun",
      "emr-serverless:StartApplication",
      "emr-serverless:GetApplication",
    ]
    # ARN de la app + sus jobruns (GetJobRun opera sobre el sub-recurso jobruns/*).
    resources = [
      aws_emrserverless_application.spark.arn,
      "${aws_emrserverless_application.spark.arn}/jobruns/*",
    ]
  }
  statement {
    # ListApplications NO tiene resource type en IAM: exige "*", no se puede acotar por ARN.
    # Lo usa scripts/load-secrets.sh (§13.1) para resolver EMR_APP_ID. Sin este statement el
    # script muere con AccessDenied por el `set -euo pipefail` y NO se genera el .env.
    sid       = "EmrServerlessList"
    actions   = ["emr-serverless:ListApplications"]
    resources = ["*"]
  }
  statement {
    sid       = "PassEmrJobRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_job.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }
}
resource "aws_iam_role_policy" "ec2_emr" {
  name   = "ec2-emr-serverless"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_emr.json
}

# Apagado job-aware: la task final `trigger_stop` del DAG (§10.3) invoca la Lambda startstop
# ({"action":"stop"}) al terminar el pipeline. Para eso el rol de la EC2 (bajo el que corre
# Airflow) necesita invocar ESA Lambda — y solo esa (least-privilege).
data "aws_iam_policy_document" "ec2_invoke_startstop" {
  statement {
    sid       = "InvokeStartStopLambda"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.startstop.arn]
  }
}
resource "aws_iam_role_policy" "ec2_invoke_startstop" {
  name   = "ec2-invoke-startstop"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_invoke_startstop.json
}

# Outputs que consumen los DAGs (los cargás como Airflow Variables o env AIRFLOW_VAR_*, §9.0/§14.1):
output "emr_app_id"       { value = aws_emrserverless_application.spark.id }
output "emr_job_role_arn" { value = aws_iam_role.emr_job.arn }
```

**D) Los entrypoints PySpark (archivos nuevos, copy-paste).** Antes de poder probar el submit del
punto **E** de abajo, necesitás el código que EMR va a ejecutar. Creá la carpeta `spark-apps/emr/`
en el repo (no existe todavía) con estos dos archivos. Son *self-contained*: no usan `.master()`
(EMR Serverless inyecta master/recursos), leen y escriben directo en S3 (`s3a://`), y la config de
Spark viaja por-job en `sparkSubmitParameters` — no hay `spark-defaults.conf` local que los toque.
El CI/CD sincroniza `spark-apps/emr/` a `s3://<artifacts>/emr/` en cada deploy (§11.3) — solo estos
entrypoints EMR, no el resto de `spark-apps/` (que es dev local) — y desde ahí los lanza el
`EmrServerlessStartJobOperator` de los DAGs (§10.2).

`spark-apps/emr/customer_etl.py` — ETL de fidelidad de clientes: lee `raw/` (CSV/JSON), calcula el
segmento de lealtad y escribe Parquet particionado por fecha en `curated/`:

```python
"""customer_etl para EMR Serverless — S3 in/out, sin HDFS, sin master hardcodeado.

Se sube a s3://<artifacts>/emr/customer_etl.py (deploy, §11.3) y lo ejecuta
EmrServerlessStartJobOperator (dags/customer_etl_emr_dag.py, §9/§10.2).

Args: 1) datalake_bucket (sin s3://)   2) run_date (YYYY-MM-DD, para particionar).
"""
import sys

from pyspark.sql import SparkSession


def main(datalake: str, run_date: str) -> None:
    base = f"s3a://{datalake}"
    raw = f"{base}/raw/customer_etl"
    out = f"{base}/curated/customer_loyalty/dt={run_date}"

    # Sin .master(): EMR Serverless inyecta master/recursos. La config de Spark viaja
    # por-job en sparkSubmitParameters (no hay spark-defaults.conf local en prod).
    spark = SparkSession.builder.appName("CustomerLoyaltyETL").getOrCreate()

    spark.read.option("header", True).csv(f"{raw}/orders.csv").createOrReplaceTempView("orders")
    spark.read.option("multiline", "true").json(f"{raw}/products.json").createOrReplaceTempView(
        "products"
    )
    spark.read.option("header", True).csv(f"{raw}/customers.csv").createOrReplaceTempView(
        "customers"
    )

    df = spark.sql("""
        WITH enriched AS (
            SELECT o.order_id, o.customer_id, o.product_id, o.quantity, o.order_date,
                   p.category, p.unit_price, o.quantity * p.unit_price AS total_price
            FROM orders o JOIN products p ON o.product_id = p.product_id
        ),
        metrics AS (
            SELECT customer_id,
                   COUNT(order_id) AS total_orders,
                   SUM(total_price) AS total_spent,
                   COUNT(DISTINCT order_date) AS days_active,
                   COUNT(DISTINCT category) AS categories_bought
            FROM enriched GROUP BY customer_id
        )
        SELECT m.customer_id, c.customer_name, c.city, c.state, c.signup_date,
               m.total_orders, m.total_spent, m.days_active, m.categories_bought,
               CASE
                   WHEN m.total_orders >= 3 AND m.days_active >= 2 AND m.categories_bought >= 2
                       THEN 'Premium'
                   WHEN m.total_orders >= 2 AND (m.days_active >= 2 OR m.categories_bought >= 2)
                       THEN 'Engaged'
                   ELSE 'Casual'
               END AS loyalty_status
        FROM metrics m JOIN customers c ON m.customer_id = c.customer_id
    """)

    # Parquet particionado por fecha: barato de escanear por Athena (§16, partition projection).
    df.write.mode("overwrite").parquet(out)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: customer_etl.py <datalake_bucket> [run_date]")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "latest")
```

`spark-apps/emr/wordcount.py` — el "hola mundo" de Spark en EMR, para validar la app de punta a punta
sin depender de datos en `raw/`:

```python
"""wordcount para EMR Serverless — self-contained, sin master hardcodeado.

Args: 1) output_uri (opcional): s3a://.../analytics/wordcount ; si falta, solo imprime.
"""
# EMR Serverless 7.x corre PySpark con Python 3.9 por defecto (3.11 está instalado pero no es
# el intérprete salvo que fijes PYSPARK_PYTHON). El `str | None` de abajo es sintaxis 3.10+ y
# se evalúa al definir la función: sin este import el módulo revienta con TypeError ANTES de
# crear la SparkSession. Ojo, el ruff del CI usa target py312 y no lo detecta.
from __future__ import annotations

import sys

from pyspark.sql import SparkSession


def main(output_uri: str | None) -> None:
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    lines = [
        "spark hadoop spark airflow",
        "hadoop hdfs spark etl",
        "airflow dag spark etl etl",
    ]
    counts = (
        spark.sparkContext.parallelize(lines)
        .flatMap(str.split)
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: kv[1], ascending=False)
    )
    rows = counts.collect()
    for word, count in rows:
        print(f"{word}\t{count}")
    if output_uri:
        spark.createDataFrame(rows, ["word", "count"]).write.mode("overwrite").parquet(output_uri)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
```

**E) Empaquetado y submit.** Con los entrypoints del punto **D** ya creados, subilos a S3 bajo
`s3://<artifacts>/emr/` — el CI/CD hace este sync solo en cada deploy (§11.3), pero para probar a
mano ahora mismo:

```bash
aws s3 sync spark-apps/emr/ "s3://pyspark-stack-artifacts-$(aws sts get-caller-identity --query Account --output text)/emr/"
```

Los logs del job van a `s3://<artifacts>/emr/logs/`. Un `StartJobRun` (así lo arma por vos el
operator de Airflow; equivalente CLI para probar a mano):

```bash
aws emr-serverless start-job-run \
  --application-id "$(terraform -chdir=infra/prod output -raw emr_app_id)" \
  --execution-role-arn "$(terraform -chdir=infra/prod output -raw emr_job_role_arn)" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://pyspark-stack-artifacts-<acct>/emr/customer_etl.py",
      "entryPointArguments": ["pyspark-stack-datalake-<acct>", "2026-07-16"],
      "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=2"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": { "logUri": "s3://pyspark-stack-artifacts-<acct>/emr/logs/" }
    }
  }'
```

> ⚠️ **`<acct>` es un placeholder, no lo dejes literal.** Reemplazalo por tu Account ID real en
> **las tres apariciones** (`entryPoint`, `entryPointArguments` y `logUri`) — por ejemplo con
> `$(aws sts get-caller-identity --query Account --output text)`. Si lo copiás tal cual del bloque
> de arriba, `logUri` queda como una ruta S3 inválida y el job falla en seco con
> `Unable to push logs ... Parameter validation failed`, sin llegar a correr una línea de Spark.
> `customer_etl.py` además necesita datos ya subidos en `raw/customer_etl/` (`orders.csv`,
> `products.json`, `customers.csv`) dentro del bucket datalake — para el primer smoke test es más
> simple apuntar el `entryPoint` a `wordcount.py`, que no depende de datos.

La config de Spark va **por-job** (en `sparkSubmitParameters`), no en un `spark-defaults.conf` local:
en EMR Serverless no hay una instancia donde montarlo. EMR escribe los logs a S3 (`emr/logs/`) y a CloudWatch
(`/aws/emr-serverless/pyspark-stack`), y expone la UI de Spark de cada corrida desde la consola de EMR.

<details>
<summary>🖱️ A mano en la consola AWS — EMR Serverless (app + rol del job)</summary>

1. **EMR → EMR Serverless → Get started / Create application**: nombre `pyspark-stack-spark`, tipo
   **Spark**, release **emr-7.5.0**. *Application setup options*: **Custom** →
   - **Auto-start**: On · **Auto-stop**: On, *idle timeout* **15 min**.
   - **Maximum capacity**: **16 vCPU / 64 GB** (techo de gasto).
   - *Network*: dejala sin VPC (los jobs solo tocan S3). Agregá VPC solo si el job accede a
     recursos privados de tu red.
2. **IAM → Roles → Create role** → *Trusted entity*: **Custom trust policy** con principal
   `emr-serverless.amazonaws.com` (`sts:AssumeRole`). Nombre `pyspark-stack-emr-serverless-job`.
   Inline policy JSON con los statements del Terraform (S3 R/W sobre `datalake/*` y `artifacts/*`,
   `s3:ListBucket`/`GetBucketLocation` sobre los dos buckets, y `logs:*` sobre
   `/aws/emr-serverless/*`). **Sin Glue.**
3. **CloudWatch → Log groups → Create**: `/aws/emr-serverless/pyspark-stack`, *Retention* **30 días**
   (cifrado en reposo por defecto).
4. Al rol de la EC2 (`pyspark-stack-ec2-role`) agregale una inline policy con
   `emr-serverless:StartJobRun/GetJobRun/StartApplication/GetApplication` sobre el ARN de la app
   (+ `.../jobruns/*`) y `iam:PassRole` sobre el ARN del rol del job con condición
   `iam:PassedToService = emr-serverless.amazonaws.com`. Agregá además
   `lambda:InvokeFunction` sobre el ARN de la Lambda `pyspark-stack-startstop`, para que la task
   `trigger_stop` del DAG (§10.3) pueda apagar la EC2 al terminar.
5. **Subí los entrypoints**: `aws s3 sync spark-apps/emr/ s3://pyspark-stack-artifacts-<acct>/emr/`
   (el CI/CD lo hace solo, §11.3).

</details>

```bash
# comprobá
terraform -chdir=infra/prod apply
aws emr-serverless list-applications --query 'applications[?name==`pyspark-stack-spark`].[id,state]'
# subir los entrypoints y lanzar un job de prueba (ver el start-job-run de arriba)
aws s3 sync spark-apps/emr/ "s3://pyspark-stack-artifacts-$(aws sts get-caller-identity --query Account --output text)/emr/"
```

### 6.5 S3 VPC Gateway Endpoint

Para que el tráfico **EC2↔S3** no salga a internet (menor superficie de
ataque, y **gratis** — el gateway endpoint de S3 no cobra por hora ni por GB), se agrega un VPC
Gateway Endpoint de S3 asociado a la route table de la VPC default:

> **El endpoint NO cubre a EMR Serverless.** Un gateway endpoint inyecta una ruta en la route
> table de tu VPC, así que solo afecta tráfico que sale de ENIs de esa VPC. Como la app EMR se crea
> sin `network_configuration` (§6.4), sus workers corren en la red administrada de AWS, fuera de tu
> VPC: no hay ENI tuya y el endpoint no les aplica. Solo aplicaría si le configuraras subnets.

```hcl
# infra/prod/network.tf  (agregar)
data "aws_route_tables" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${local.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = data.aws_route_tables.default.ids
  tags              = { Name = "${var.name_prefix}-s3-endpoint" }
}
```

<details>
<summary>🖱️ A mano en la consola AWS — S3 VPC Gateway Endpoint</summary>

1. **VPC → Endpoints → Create endpoint**: *Service category* **AWS services** → buscá
   `com.amazonaws.<region>.s3` con *Type* **Gateway** (no Interface).
2. *VPC*: la **default**. *Route tables*: marcá **todas** las de la VPC default (así el tráfico a
   S3 se enruta por el endpoint).
3. *Policy*: **Full access** (los buckets ya están cerrados con sus bucket policies) → *Create*.
   Es gratis y no cobra transferencia.

</details>

```bash
# comprobá
terraform -chdir=infra/prod apply   # crea el gateway endpoint de arriba
aws ec2 describe-vpc-endpoints --query 'VpcEndpoints[?ServiceName==`com.amazonaws.us-east-1.s3`].[VpcEndpointId,State]'
```

---

## 7. Orquestación: Lambda trigger-airflow (SSM) + EventBridge + event-driven

Airflow corre dentro de la EC2. Aunque la **web** se publique por HTTPS restringida a tu IP (§5.6),
esa puerta **no** sirve para automatizar: la Lambda no está en tu IP y ensanchar el SG para incluirla no es una opción.
Para dispararlo desde AWS (por cron o cuando llega un archivo a S3) se usa una **Lambda que ejecuta
`airflow dags trigger` vía SSM `SendCommand`** — sin abrir puertos ni depender de la web. Es el mismo
patrón para los dos disparadores.

### 7.1 Lambda que dispara los DAGs vía SSM

Dos mejoras sobre la versión mínima: **(a)** ya no falla en silencio si la EC2 está apagada —
detecta el estado, la prende, y deja que el transporte (SQS para eventos S3, el retry async de
Lambda para el cron) reintente en unos minutos, cuando ya esté lista; **(b)** un **contrato de
datos** liviano rechaza archivos con columnas faltantes **antes** de gastar en cómputo de EMR —
sin Lambda Layers ni dependencias nuevas, solo `csv`/`json` de la stdlib.

```python
# infra/prod/lambda/trigger_airflow.py
import os
import csv
import json
import hashlib
import urllib.parse
import boto3

ssm = boto3.client("ssm")
ec2 = boto3.client("ec2")
s3  = boto3.client("s3")

INSTANCE_ID = os.environ["INSTANCE_ID"]
DEFAULT_DAG = os.environ.get("DEFAULT_DAG", "customer_etl_emr")

# Contrato mínimo por archivo: columnas/keys requeridas. Lo que no está acá no se valida (pasa
# igual) — ampliá a medida que sumes fuentes. Esto es un gate BARATO (mira solo el header/las
# primeras keys); la validación de contenido de verdad es Great Expectations (§20), después del ETL.
CONTRACTS = {
    "orders.csv":    {"order_id", "customer_id", "product_id", "quantity", "order_date"},
    "customers.csv": {"customer_id", "customer_name", "city", "state", "signup_date"},
    "products.json": {"product_id", "category", "unit_price"},
}


class ContractViolation(Exception):
    pass


def _peek_columns(bucket, key):
    """Lee los primeros ~2 KB del objeto (Range GET, NO descarga el archivo entero) y devuelve
    sus columnas. CSV: el header. JSON: las keys del primer registro (soporta array u objeto)."""
    body = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-2047")["Body"].read()
    head = body.decode("utf-8", errors="replace")
    if key.endswith(".csv"):
        return set(next(csv.reader([head.splitlines()[0]])))
    if key.endswith(".json"):
        # products.json es un array multilínea (ver docs/04 Ej. 7): el Range GET puede cortar a
        # mitad de objeto. "Mejor esfuerzo": si no parsea con la muestra, NO bloqueamos — un falso
        # negativo acá es preferible a un falso positivo que frena un archivo válido.
        try:
            data = json.loads(head)
        except json.JSONDecodeError:
            return None
        first = data[0] if isinstance(data, list) and data else data
        return set(first.keys()) if isinstance(first, dict) else None
    return None


def _validar_contrato(bucket, key):
    esperado = CONTRACTS.get(key.rsplit("/", 1)[-1])
    if esperado is None:
        return
    columnas = _peek_columns(bucket, key)
    if columnas is None:
        return
    faltan = esperado - columnas
    if faltan:
        raise ContractViolation(f"{key}: faltan columnas {sorted(faltan)} (esperadas {sorted(esperado)})")


def _ec2_lista(instance_id):
    """True si la instancia está running Y el agente SSM está Online. Si está stopped, dispara el
    start (idempotente) y devuelve False: NO esperamos adentro de la Lambda con un sleep — eso solo
    quema tiempo de ejecución sin ganar nada. El caller propaga el estado "todavía no" para que el
    transporte reintente en unos minutos."""
    state = ec2.describe_instances(InstanceIds=[instance_id]) \
               ["Reservations"][0]["Instances"][0]["State"]["Name"]
    if state == "stopped":
        ec2.start_instances(InstanceIds=[instance_id])
        return False
    if state != "running":  # pending, stopping, shutting-down
        return False
    infos = ssm.describe_instance_information(
        Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
    )["InstanceInformationList"]
    return bool(infos) and infos[0]["PingStatus"] == "Online"


def _disparar_dag(dag, conf, run_id=None):
    trigger = f"airflow dags trigger {dag}"
    if run_id:
        # Determinístico (derivado de bucket+key): si SQS reintenta un mensaje que YA disparó el
        # DAG con éxito (SendCommand es fire-and-forget, la Lambda no confirma el resultado antes
        # de retornar), `airflow dags trigger` con el MISMO --run-id falla en vez de crear un
        # segundo dagrun para el mismo archivo. Auditoría §1.3: sin esto, el retry (que es lo que
        # nos da la resiliencia de §7.3) podía convertirse en un doble-procesamiento silencioso.
        trigger += f" --run-id '{run_id}'"
    if conf:
        trigger += f" --conf '{json.dumps(conf)}'"
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Comment=f"trigger airflow dag {dag}",
        Parameters={"commands": [f"docker exec airflow-scheduler {trigger}"]},
    )
    return resp["Command"]["CommandId"]


def handler(event, context):
    """Dos formas de entrada:
    - Cron (EventBridge Scheduler, invocación async directa): {"dag": "customer_etl_emr"}.
    - Evento S3 (vía la cola SQS primaria, §7.3): {"Records": [{"body": "<S3 event JSON>"}]}.
    """
    bucket = key = run_id = None
    if "Records" in event and event["Records"] and "body" in event["Records"][0]:
        # batch_size=1 (§7.3): un mensaje SQS = un evento S3 = una invocación.
        rec = json.loads(event["Records"][0]["body"])["Records"][0]["s3"]
        key = urllib.parse.unquote_plus(rec["object"]["key"])  # S3 codifica espacios/especiales
        bucket = rec["bucket"]["name"]
        dag, conf = DEFAULT_DAG, {"bucket": bucket, "key": key}
        # run_id determinístico por archivo: un reintento del MISMO objeto
        # produce el MISMO run_id, así que un doble-trigger no crea un doble dagrun. El cron
        # (rama de abajo) no lo necesita tanto: dispara una vez al día, y su propio retry async
        # de Lambda es 1-2 intentos en minutos, no una cola que puede reintentar 5 veces.
        run_id = "s3-" + hashlib.sha256(f"{bucket}/{key}".encode()).hexdigest()[:16]
    else:
        dag, conf = event.get("dag", DEFAULT_DAG), {}

    try:
        if bucket and key:
            _validar_contrato(bucket, key)  # ContractViolation: NO reintentar, no tiene sentido
    except ContractViolation as e:
        print(f"RECHAZADO por contrato de datos: {e}")  # log-based metric filter si querés alertar
        return {"status": "rejected", "reason": str(e)}

    if not _ec2_lista(INSTANCE_ID):
        # Se propaga sin capturar: dispara el retry de SQS (evento S3) o el retry async de Lambda
        # (cron) — a los pocos minutos la EC2 ya debería estar arriba y este mismo intento pasa.
        raise RuntimeError(f"EC2 {INSTANCE_ID} no está lista todavía (arrancando); reintentar")

    return {"dag": dag, "conf": conf, "commandId": _disparar_dag(dag, conf, run_id)}
```

> **Por qué no un `time.sleep()` esperando a que la EC2 arranque.** Boot + agente SSM online tarda
> ~2-5 min (§5.5). Bloquear la Lambda ese tiempo cuesta (facturás por duración) y arriesga el
> timeout. Devolver el estado "todavía no" y dejar que **el transporte** reintente es gratis: SQS ya
> tiene *visibility timeout* + redrive, y el cron ya tiene el retry async de Lambda — reusar esa
> mecánica en vez de reinventarla adentro del handler.

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
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ssm:DescribeInstanceInformation"]
    resources = ["*"]   # DescribeInstanceInformation no admite ARN de recurso
  }
  statement {   # DescribeInstances no admite ARN de recurso (a diferencia de StartInstances, abajo)
    sid       = "DescribeEc2"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }
  statement {   # arrancar la EC2 si el evento la encuentra apagada (§7.1) — SOLO nuestra instancia,
    # nunca "*": StartInstances sí admite scoping por ARN a diferencia de Describe. Auditoría §1.1.
    sid       = "StartEc2IfStopped"
    actions   = ["ec2:StartInstances"]
    resources = ["arn:aws:ec2:${local.region}:${local.account_id}:instance/${aws_instance.pyspark.id}"]
  }
  statement {   # el contrato de datos (§7.1) hace un Range GET del objeto que disparó el evento
    sid       = "ContractPeek"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/raw/*"]
  }
  statement {   # consumir la cola SQS primaria (§7.3) — lo exige el event source mapping
    sid       = "ConsumeTriggerQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.trigger_events.arn]
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
# Mismo criterio que §5.4: retención acotada, no infinita.
resource "aws_cloudwatch_log_group" "trigger_airflow" {
  name              = "/aws/lambda/${var.name_prefix}-trigger-airflow"
  retention_in_days = 14
}

resource "aws_lambda_function" "trigger_airflow" {
  function_name    = "${var.name_prefix}-trigger-airflow"
  filename         = data.archive_file.trigger_airflow.output_path
  source_code_hash = data.archive_file.trigger_airflow.output_base64sha256
  handler          = "trigger_airflow.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.trigger_airflow.arn
  timeout          = 60
  # Techo de invocaciones concurrentes: sin esto, subir 50
  # archivos a la vez dispara hasta 50 invocaciones en paralelo, cada una intentando un
  # EmrServerlessStartJobOperator contra un `maximum_capacity` de 16 vCPU (§6.4). Con el límite en 2,
  # SQS deja el resto de los mensajes en cola (no los pierde, no los reintenta antes de tiempo) y se
  # van procesando de a poco. Complementa —no reemplaza— el `max_active_runs=1` del DAG (§10.4).
  reserved_concurrent_executions = 2
  environment {
    variables = {
      INSTANCE_ID = aws_instance.pyspark.id
      DEFAULT_DAG = "customer_etl_emr" # el DAG de producción (EMR Serverless, §10.2) — no el flujo dev local
    }
  }
  # DLQ (dead_letter_config) todavía NO va acá: aws_sqs_queue.trigger_airflow_dlq recién se crea
  # en §18.1, que la engancha agregando este bloque a este mismo resource (sin duplicarlo). Si lo
  # ponés ahora, el `apply` de esta sección falla con "Reference to undeclared resource".
  depends_on = [aws_cloudwatch_log_group.trigger_airflow]
}
```

<details>
<summary>🖱️ A mano en la consola AWS — Lambda trigger-airflow</summary>

1. **Lambda → Create function**: nombre `pyspark-stack-trigger-airflow`, runtime **Python 3.12**
   → pegá `trigger_airflow.py` en el editor y cambiá el handler a **`lambda_function.handler`**
   (*Runtime settings → Edit*; el código define `def handler`, no `lambda_handler`).
   *Configuration → General*: timeout **60 s**.
2. *Environment variables*: `INSTANCE_ID=<i-xxxxxxxx>` (tu instancia) y
   `DEFAULT_DAG=customer_etl_emr` (el DAG de producción, §10.2).
3. Al rol de ejecución (*Permissions*) agregale una inline policy JSON con los statements del
   Terraform: `ssm:SendCommand` **solo** sobre el ARN de tu instancia y sobre
   `arn:aws:ssm:us-east-1::document/AWS-RunShellScript`, más
   `ssm:GetCommandInvocation`/`ListCommandInvocations` (los logs ya los cubre el basic execution
   role que crea la consola).
4. Probala con *Test* → evento `{"dag": "customer_etl_emr"}` → en la EC2 debería aparecer un
   DAG run nuevo (`airflow dags list-runs customer_etl_emr`).

</details>

```bash
# comprobá
terraform -chdir=infra/prod apply   # crea la Lambda trigger-airflow + su rol de arriba
# el agente SSM Online es prerrequisito de toda la §7
ID=$(terraform -chdir=infra/prod output -raw instance_id)
aws ssm describe-instance-information --query "InstanceInformationList[?InstanceId=='$ID'].PingStatus"  # ["Online"]
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --cli-binary-format raw-in-base64-out --payload '{"dag":"customer_etl_emr"}' /dev/stdout
# en la EC2: dag_id posicional (en Airflow 3 no existe -d)
docker compose exec -T airflow-scheduler airflow dags list-runs customer_etl_emr
```

> **`dags/customer_etl_emr_dag.py` todavía no existe en la EC2 acá.** El `lambda invoke` de arriba
> devuelve 200 igual (SSM `SendCommand` es fire-and-forget, §7.1 no confirma el resultado), pero el
> `list-runs` va a fallar con `DAG customer_etl_emr not found`: el DAG recién se escribe y se sube al
> servidor en §10.2. Es esperable en esta primera pasada — repetí este mismo chequeo después de §10.2
> para validarlo de punta a punta.

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
  name = "${var.name_prefix}-daily-etl"
  # 12:00 UTC, L-V: dentro de la ventana de encendido (start 11:00 / stop 22:00 UTC, §5.4).
  # Fuera de la ventana, el SendCommand se perdería en silencio (§7.1).
  schedule_expression          = "cron(0 12 ? * MON-FRI *)"
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.trigger_airflow.arn
    role_arn = aws_iam_role.sched_etl.arn
    input    = jsonencode({ dag = "customer_etl_emr" }) # DAG de producción (§10.2)
  }
}
```

<details>
<summary>🖱️ A mano en la consola AWS — cron del ETL</summary>

1. **EventBridge → Scheduler → Create schedule**: nombre `pyspark-stack-daily-etl`.
2. *Recurring* → cron **`0 12 ? * MON-FRI *`** (UTC — dentro de la ventana de encendido del
   auto start/stop) · *Flexible time window*: **Off**.
3. *Target*: **AWS Lambda → Invoke** → `pyspark-stack-trigger-airflow` → *Payload*:
   `{"dag": "customer_etl_emr"}`.
4. El rol de invocación lo crea la consola automáticamente → *Create schedule*.

</details>

```bash
# comprobá
terraform -chdir=infra/prod apply   # crea el schedule + su rol de invocación de arriba
aws scheduler list-schedules --query 'Schedules[].Name'   # aparece pyspark-stack-daily-etl
```

### 7.3 Disparo por evento (archivo nuevo en S3, vía SQS)

Cuando llega un archivo a `raw/`, S3 **no** invoca la Lambda directo: escribe un mensaje en una
cola **SQS primaria**, y la Lambda la consume (`batch_size=1`, un mensaje = un archivo = una
invocación). ETL 100% event-driven, sin polling — la vuelta por SQS no le suma latencia perceptible
(milisegundos) y es lo que le da a §7.1 su reintento gratis cuando la EC2 está apagada: si el
handler levanta una excepción, el mensaje **no se borra** de la cola, y vuelve a estar visible
pasado el *visibility timeout* — la Lambda lo reprocesa sola unos minutos después, sin que nadie
haga nada.

```hcl
# infra/prod/orchestration.tf  (continuación)

# Cola primaria: S3 escribe acá, no invoca la Lambda directo (eso es lo que habilita el retry
# transparente de §7.1). visibility_timeout ~6x el timeout de la Lambda (60s) Y suficiente para
# cubrir un boot completo de la EC2 (~2-5 min, §5.5): 360s cumple las dos cosas a la vez.
resource "aws_sqs_queue" "trigger_events" {
  name                       = "${var.name_prefix}-trigger-events"
  visibility_timeout_seconds = 360
  # redrive_policy (hacia aws_sqs_queue.trigger_airflow_dlq) todavía NO va acá: esa cola recién
  # se crea en §18.1, que la engancha agregando este bloque a este mismo resource. Ponerlo ahora
  # rompe el `apply` de esta sección con "Reference to undeclared resource".
}

# Permite que S3 (y SOLO el bucket datalake) escriba en la cola.
data "aws_iam_policy_document" "trigger_events_queue" {
  statement {
    sid       = "AllowS3Send"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.trigger_events.arn]
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.datalake.arn]
    }
  }
}
resource "aws_sqs_queue_policy" "trigger_events" {
  queue_url = aws_sqs_queue.trigger_events.id
  policy    = data.aws_iam_policy_document.trigger_events_queue.json
}

resource "aws_s3_bucket_notification" "on_upload" {
  bucket = aws_s3_bucket.datalake.id
  queue {
    queue_arn     = aws_sqs_queue.trigger_events.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
  }
  depends_on = [aws_sqs_queue_policy.trigger_events]
}

# La Lambda consume la cola (no hace falta aws_lambda_permission: eso es solo para invocación
# directa por un servicio; acá Lambda hace polling de SQS con los permisos sqs:* de §7.1).
resource "aws_lambda_event_source_mapping" "trigger_events" {
  event_source_arn = aws_sqs_queue.trigger_events.arn
  function_name    = aws_lambda_function.trigger_airflow.arn
  batch_size       = 1   # 1 archivo = 1 invocación: así un archivo lento/rechazado no bloquea a los demás
}
```

<details>
<summary>🖱️ A mano en la consola AWS — evento S3 → SQS → Lambda</summary>

1. **SQS → Create queue** → *Standard* → nombre `pyspark-stack-trigger-events` → *Visibility
   timeout* **360 seconds** → *Dead-letter queue*: **Enabled**, cola `pyspark-stack-trigger-airflow-dlq`
   (la de §18.1), *Maximum receives* **5** → **Create queue**.
2. En esa cola → **Access policy** → pegá el statement que permite `s3.amazonaws.com` con
   `aws:SourceArn` = el ARN del bucket datalake (el JSON del Terraform de arriba).
3. **S3 → bucket `pyspark-stack-datalake-…` → Properties → Event notifications → Create event
   notification** → nombre `on-upload-raw` · *Prefix*: `raw/` · *Event types*: **All object create
   events** · *Destination*: **SQS queue** → `pyspark-stack-trigger-events`.
4. **Lambda → `pyspark-stack-trigger-airflow` → Configuration → Triggers → Add trigger** → **SQS**
   → la misma cola → *Batch size* **1** → **Add**.

</details>

> Los dos disparadores (cron y evento S3) apuntan al DAG de producción `customer_etl_emr`
> (§10.2) — no al `customer_etl_dag` dev-local, que usa el Spark/HDFS deshabilitado en prod.
> Tal como viene, `customer_etl_emr` tampoco lee `dag_run.conf` (procesa por `{{ ds }}`):
> dispararlo por evento S3 lo corre, pero ignora el archivo puntual que llegó. Para el camino
> event-driven real, hacé que lea `{{ dag_run.conf['bucket'] }}` / `{{ dag_run.conf['key'] }}`
> y los pase como `entryPointArguments` del `EmrServerlessStartJobOperator` (patrón de §9.0);
> el job Spark en EMR Serverless lee entonces justo ese objeto de `s3a://`.

```bash
# comprobá
terraform -chdir=infra/prod apply   # crea la cola SQS + la notificación S3 de arriba
```

> Verificá el retry: apagá la EC2 a mano (`aws ec2 stop-instances`), subí un archivo a `raw/`, y
> mirá `aws sqs get-queue-attributes --queue-url <url> --attribute-names ApproximateNumberOfMessages
> ApproximateNumberOfMessagesNotVisible` — el mensaje va a aparecer "not visible" (siendo procesado o
> esperando el próximo intento) hasta que la EC2 esté arriba y el DAG se dispare solo, sin que
> reinicies nada a mano.

---

## 8. Operación diaria y diagnóstico

Esta sección es el punto de entrada después de `terraform apply`. El orden de diagnóstico es:

```text
AWS → EC2/SSM → Docker → Airflow → EMR Serverless → datos → alertas
```

Detente en la primera capa que falle. Un DAG no se puede diagnosticar correctamente si SSM está
offline o si el scheduler no está sano.

### 8.1 Cargar el contexto de producción

**Dónde:** terminal local, desde la raíz del repositorio.

**Objetivo:** evitar copiar IDs, IP y nombres manualmente.

```bash
export AWS_REGION="${AWS_REGION:-us-east-1}"
export NAME_PREFIX="${NAME_PREFIX:-pyspark-stack}"
export INSTANCE_ID="$(terraform -chdir=infra/prod output -raw instance_id)"
export PUBLIC_IP="$(terraform -chdir=infra/prod output -raw public_ip)"
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export DATALAKE_BUCKET="${NAME_PREFIX}-datalake-${ACCOUNT_ID}"
export ARTIFACTS_BUCKET="${NAME_PREFIX}-artifacts-${ACCOUNT_ID}"
export EMR_APP_ID="$(terraform -chdir=infra/prod output -raw emr_app_id)"
```

Comprueba la cuenta antes de continuar:

```bash
aws sts get-caller-identity
printf 'EC2=%s\nIP=%s\nEMR=%s\n' "$INSTANCE_ID" "$PUBLIC_IP" "$EMR_APP_ID"
```

### 8.2 Smoke test después de un cambio

**Dónde:** terminal local.

**Objetivo:** demostrar que la plataforma, no solo Terraform, quedó operativa.

```bash
terraform -chdir=infra/prod fmt -check -recursive
terraform -chdir=infra/prod validate

aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"

aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
  --query 'InstanceInformationList[0].PingStatus' \
  --output text

aws emr-serverless get-application \
  --application-id "$EMR_APP_ID" \
  --query 'application.state' \
  --output text
```

Resultado esperado:

- SSM devuelve `Online`.
- EMR Serverless devuelve `CREATED`, `STARTED` o `STOPPED`. Esos estados no representan un error;
  la aplicación puede iniciar automáticamente al recibir un job.

Ahora valida el host mediante SSM. Esto prueba el mismo canal que usan las automatizaciones:

```bash
PARAMS='{"commands":[
  "cd /home/ec2-user/pyspark_stack",
  "mountpoint /data",
  "docker compose -f docker-compose.prod.yml config --quiet",
  "docker compose -f docker-compose.prod.yml ps",
  "docker compose -f docker-compose.prod.yml exec -T airflow-scheduler airflow dags list-import-errors --output json"
]}'

COMMAND_ID="$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' \
  --output text)"

aws ssm wait command-executed \
  --command-id "$COMMAND_ID" \
  --instance-id "$INSTANCE_ID"

aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" \
  --instance-id "$INSTANCE_ID" \
  --query '{status:Status,stdout:StandardOutputContent,stderr:StandardErrorContent}'
```

El comando debe finalizar con `Success`. La lista JSON de errores de importación debe estar vacía.

### 8.3 Prueba end-to-end

**Dónde:** terminal local, después del smoke test.

**Objetivo:** comprobar Lambda → SSM → Airflow → EMR Serverless.

```bash
aws lambda invoke \
  --function-name "${NAME_PREFIX}-trigger-airflow" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"dag":"customer_etl_emr","conf":{"source":"manual-smoke"}}' \
  /tmp/trigger-response.json

cat /tmp/trigger-response.json
```

Una respuesta HTTP 200 de Lambda solo confirma que Lambda atendió la solicitud. Si devuelve un
`commandId`, comprueba el resultado real:

```bash
COMMAND_ID="$(jq -r '.commandId' /tmp/trigger-response.json)"

aws ssm wait command-executed \
  --command-id "$COMMAND_ID" \
  --instance-id "$INSTANCE_ID"

aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" \
  --instance-id "$INSTANCE_ID"
```

Después consulta las últimas ejecuciones:

```bash
aws emr-serverless list-job-runs \
  --application-id "$EMR_APP_ID" \
  --max-results 10 \
  --query 'jobRuns[].{id:id,name:name,state:state,created:createdAt}' \
  --output table
```

Para validar el camino por archivo:

```bash
SMOKE_KEY="raw/_smoke/$(date -u +%Y%m%dT%H%M%SZ)/_SUCCESS"
printf 'ready\n' | aws s3 cp - "s3://${DATALAKE_BUCKET}/${SMOKE_KEY}"

aws sqs get-queue-attributes \
  --queue-url "$(aws sqs get-queue-url \
    --queue-name "${NAME_PREFIX}-trigger-events" \
    --query QueueUrl --output text)" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

S3 entrega notificaciones **al menos una vez** y puede duplicarlas o desordenarlas. Por eso el DAG
y el job deben ser idempotentes; `max_active_runs=1` limita concurrencia, pero no elimina duplicados.

### 8.4 Comandos de operación diaria

| Necesidad | Comando o fuente |
|---|---|
| Encender la EC2 | Lambda `startstop` con `{"action":"start"}` |
| Apagar con guardia | Lambda `startstop` con `{"action":"stop"}` |
| Forzar apagado | `{"action":"stop","force":true}` solo durante un incidente |
| Disparar un DAG | Lambda `trigger-airflow` |
| Ver jobs Spark | `aws emr-serverless list-job-runs` |
| Ver un job | `aws emr-serverless get-job-run` |
| Cancelar un job | `aws emr-serverless cancel-job-run` |
| Ver el stack | SSM o SSH: `docker compose ps` |
| Ver errores de DAG | `airflow dags list-import-errors --output json` |
| Ver colas pendientes | métricas o atributos de SQS |

Invocaciones manuales:

```bash
aws lambda invoke \
  --function-name "${NAME_PREFIX}-startstop" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"action":"start"}' \
  /dev/stdout

aws lambda invoke \
  --function-name "${NAME_PREFIX}-trigger-airflow" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"dag":"customer_etl_emr"}' \
  /dev/stdout
```

### 8.5 Teardown seguro

Las guardas `prevent_destroy` y el versionado de S3 existen para impedir una eliminación
accidental. El procedimiento completo está en la sección 21.4. No ejecutes `terraform destroy`
directamente contra producción.

### 8.6 Diagnóstico rápido

| Síntoma | Primera comprobación | Causa frecuente |
|---|---|---|
| SSM `Offline` | estado EC2, rol y agente SSM | boot incompleto o IAM |
| DAG no aparece | `list-import-errors` | dependencia faltante o error Python |
| DAG queda en cola | pausa, scheduler, pools | DAG pausado o sin capacidad |
| EMR queda `PENDING` | capacidad y cuotas | máximo de vCPU o concurrencia |
| EMR `FAILED` | `stateDetails` y logs | permisos S3, código o memoria |
| EC2 no se apaga | DAG runs activos | guardia *job-aware* funcionando |
| No llega un archivo | SQS y DLQ | filtro S3, policy o Lambda |
| Grafana no responde | túnel y contenedor | EC2 apagada o servicio caído |

Para un job EMR fallido:

```bash
JOB_ID="<job-id>"

aws emr-serverless get-job-run \
  --application-id "$EMR_APP_ID" \
  --job-run-id "$JOB_ID" \
  --query 'jobRun.{state:state,detail:stateDetails,driver:jobDriver}'

aws logs tail /aws/emr-serverless \
  --since 30m \
  --follow
```

---

## 9. Patrones de tareas DataOps

### 9.1 Elegir el motor

| Trabajo | Motor recomendado | Razón |
|---|---|---|
| API, archivo pequeño, control o notificación | Python en Airflow | arranque rápido |
| joins, ventanas o grandes volúmenes | PySpark en EMR Serverless | cómputo elástico |
| transformación SQL repetible | dbt sobre Athena | modelo versionado y testeable |
| validación puntual de una tabla | Athena o Python | menor complejidad |

No uses Spark por costumbre. El arranque y la infraestructura de un job distribuido no compensan
para archivos pequeños.

### 9.2 Contrato mínimo de un DAG productivo

Todo DAG nuevo debe definir:

- `owner`, `retries`, `retry_delay` y `execution_timeout`.
- `catchup=False`, salvo que exista un plan explícito de *backfill*.
- `max_active_runs` y, si corresponde, un pool.
- escritura idempotente por partición, clave de negocio o `MERGE`.
- `deferrable=True` en operadores EMR para liberar el worker mientras espera.
- parámetros de entrada mediante `dag_run.conf`, no rutas rígidas.
- logs con `run_id`, `bucket`, `key`, partición y job ID.

### 9.3 Dependencias

**Archivo:** `requirements.txt`.

```text
apache-airflow-providers-amazon[aiobotocore]==9.29.0
pandas
pyarrow
s3fs
boto3
```

El pin del provider debe coincidir con el constraints usado por la imagen Airflow. No actualices
Airflow, providers y Python en el mismo cambio.

### 9.4 DAG de referencia para EMR Serverless

**Archivo:** `dags/customer_etl_emr_dag.py`.

```python
from datetime import datetime, timedelta

import boto3
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
)
from airflow.sdk import DAG, task


with DAG(
    dag_id="customer_etl_emr",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["prod", "emr", "customer"],
) as dag:
    run_emr = EmrServerlessStartJobOperator(
        task_id="run_customer_etl",
        name="customer-etl-{{ ts_nodash }}",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_job_role_arn }}",
        deferrable=True,
        job_driver={
            "sparkSubmit": {
                "entryPoint": (
                    "s3://{{ var.value.artifacts }}/emr/customer_etl.py"
                ),
                "entryPointArguments": [
                    "{{ dag_run.conf.get('bucket', var.value.datalake) }}",
                    "{{ dag_run.conf.get('key', '') }}",
                    "{{ ds }}",
                ],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g"
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://{{ var.value.artifacts }}/emr/logs/"
                },
                "cloudWatchLoggingConfiguration": {
                    "enabled": True,
                    "logGroupName": "/aws/emr-serverless",
                },
            }
        },
    )

    @task(trigger_rule="all_done")
    def request_safe_stop() -> None:
        boto3.client("lambda").invoke(
            FunctionName="pyspark-stack-startstop",
            InvocationType="Event",
            Payload=b'{"action":"stop"}',
        )

    run_emr >> request_safe_stop()
```

El bloque hace tres cosas: dispara el job, espera sin ocupar un worker y solicita el apagado seguro
cuando termina. La Lambda vuelve a comprobar si existen otros DAGs activos antes de apagar.

### 9.5 Idempotencia

El job debe escribir un resultado repetible. Para una partición Parquet:

```python
(
    dataframe.dropDuplicates(["customer_id", "event_id"])
    .write.mode("overwrite")
    .partitionBy("dt")
    .parquet(f"s3a://{datalake}/curated/customer")
)
```

Para actualizaciones concurrentes o por clave de negocio, usa Iceberg con `MERGE`; no simules un
upsert con archivos Parquet sueltos.

---

## 10. Flujo de desarrollo y despliegue

```text
feature branch → CI → revisión → merge a main → OIDC → S3 → SSM → Airflow → EMR
```

### 10.1 Iteración rápida

**Archivo:** `scripts/deploy-dev.sh`.

```bash
#!/usr/bin/env bash
set -euo pipefail

IP="$(terraform -chdir=infra/prod output -raw public_ip)"
KEY="${SSH_KEY:-$HOME/.ssh/pyspark_stack}"

rsync -az --delete \
  --exclude __pycache__ \
  -e "ssh -i $KEY" \
  dags spark-apps \
  "ec2-user@${IP}:/home/ec2-user/pyspark_stack/"

ssh -i "$KEY" "ec2-user@${IP}" \
  "cd /home/ec2-user/pyspark_stack &&
   docker compose -f docker-compose.prod.yml exec -T airflow-dag-processor airflow dags reserialize &&
   docker compose -f docker-compose.prod.yml exec -T airflow-scheduler airflow dags list-import-errors --output json"
```

Úsalo solo para desarrollo. Producción se despliega mediante el workflow de la sección 11.

### 10.2 Qué se despliega

- `dags/`: baja a la EC2.
- `spark-apps/emr/`: queda en S3; EMR lo lee al iniciar cada job.
- `requirements.txt`, Dockerfile y Compose: requieren reconstruir la imagen.
- `infra/`: requiere `terraform plan` y aprobación separada.
- `monitoring/`: requiere validar configuración y reiniciar solo el servicio afectado.

### 10.3 Rollback

El rollback de aplicación es `git revert` seguido de un nuevo despliegue. No edites archivos
directamente en la EC2: genera diferencias imposibles de auditar.

Si el cambio modificó dependencias o Compose:

```bash
git revert COMMIT_SHA
git push origin main

ssh -i ~/.ssh/pyspark_stack "ec2-user@${PUBLIC_IP}" \
  "cd /home/ec2-user/pyspark_stack &&
   docker compose -f docker-compose.prod.yml up -d --build"
```

---

## 11. CI/CD con GitHub Actions y OIDC

OIDC evita almacenar `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY` en GitHub. El rol de despliegue
debe confiar únicamente en el repositorio y el environment `production`.

### 11.1 Controles obligatorios

- CI no modifica AWS.
- CD usa `environment: production` con aprobadores.
- `id-token: write` solo existe en el job que asume el rol.
- El rol escribe únicamente en el bucket de artifacts y ejecuta SSM sobre la EC2 prevista.
- `terraform apply` no comparte el mismo rol que el despliegue de DAGs.

### 11.2 Workflow de CI

**Archivo:** `.github/workflows/ci.yml`.

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - name: Dependencias
        run: pip install -r requirements.txt pytest ruff

      - name: Python y DAGs
        run: |
          ruff check dags spark-apps tests
          ruff format --check dags spark-apps tests
          pytest -q tests/test_dag_integrity.py

      - uses: hashicorp/setup-terraform@v3

      - name: Terraform
        run: |
          terraform -chdir=infra/prod fmt -check -recursive
          terraform -chdir=infra/prod init -backend=false
          terraform -chdir=infra/prod validate

      - name: Compose
        run: docker compose -f docker-compose.prod.yml config --quiet
```

### 11.3 Test de integridad de DAGs

**Archivo:** `tests/test_dag_integrity.py`.

```python
from airflow.models import DagBag


def test_dags_import_without_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert dag_bag.import_errors == {}


def test_dags_have_operational_defaults():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    for dag in dag_bag.dags.values():
        assert dag.tags
        assert dag.max_active_runs >= 1
        for task in dag.tasks:
            assert task.owner
            assert task.retries >= 1
```

### 11.4 Workflow de despliegue

**Archivo:** `.github/workflows/deploy.yml`.

```yaml
name: Deploy

on:
  push:
    branches: [main]
    paths:
      - "dags/**"
      - "spark-apps/emr/**"
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

concurrency:
  group: production-deploy
  cancel-in-progress: false

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}
          aws-region: ${{ vars.AWS_REGION }}

      - name: Publicar artifacts
        env:
          BUCKET: ${{ vars.ARTIFACTS_BUCKET }}
        run: |
          aws s3 sync dags/ "s3://$BUCKET/deploy/dags/" --delete
          aws s3 sync spark-apps/emr/ "s3://$BUCKET/emr/" --delete

      - name: Resolver EC2
        id: ec2
        run: |
          ID="$(aws ec2 describe-instances \
            --filters "Name=tag:Name,Values=pyspark-stack-node" \
                      "Name=instance-state-name,Values=running" \
            --query 'Reservations[0].Instances[0].InstanceId' \
            --output text)"
          echo "id=$ID" >> "$GITHUB_OUTPUT"

      - name: Aplicar y validar
        if: steps.ec2.outputs.id != 'None' && steps.ec2.outputs.id != ''
        env:
          INSTANCE_ID: ${{ steps.ec2.outputs.id }}
          BUCKET: ${{ vars.ARTIFACTS_BUCKET }}
        run: |
          PARAMS="$(jq -nc --arg b "$BUCKET" '{
            commands: [
              "set -euo pipefail",
              "cd /home/ec2-user/pyspark_stack",
              ("aws s3 sync s3://" + $b + "/deploy/dags/ dags/ --delete"),
              "docker compose -f docker-compose.prod.yml exec -T airflow-dag-processor airflow dags reserialize",
              "docker compose -f docker-compose.prod.yml exec -T airflow-scheduler airflow dags list-import-errors --output json > /tmp/import-errors.json",
              "python3 -c \"import json; assert not json.load(open('/tmp/import-errors.json'))\""
            ]
          }')"

          COMMAND_ID="$(aws ssm send-command \
            --instance-ids "$INSTANCE_ID" \
            --document-name AWS-RunShellScript \
            --parameters "$PARAMS" \
            --query 'Command.CommandId' \
            --output text)"

          aws ssm wait command-executed \
            --command-id "$COMMAND_ID" \
            --instance-id "$INSTANCE_ID"

          aws ssm get-command-invocation \
            --command-id "$COMMAND_ID" \
            --instance-id "$INSTANCE_ID" \
            --query '{status:Status,stdout:StandardOutputContent,stderr:StandardErrorContent}'
```

Si la EC2 está apagada, el código queda publicado en S3. El script de arranque debe sincronizar
`deploy/dags/` antes de levantar Airflow.

---

## 12. Observabilidad e incidentes

### 12.1 Qué debe verse

| Capa | Señales mínimas | Fuente |
|---|---|---|
| EC2 | CPU, memoria, disco `/data`, estado | node-exporter, CloudWatch |
| Docker | reinicios, memoria, salud | cAdvisor, `docker compose ps` |
| Airflow | DAG success/failure/duration, scheduler | StatsD exporter |
| EMR | jobs failed/running, vCPU, memoria | CloudWatch `AWS/EMRServerless` |
| Orquestación | errores Lambda, edad SQS, DLQ | CloudWatch |
| Datos | filas, nulos, duplicados, frescura | checks del pipeline |

Prometheus, Grafana y Loki se apagan con la EC2. Las alarmas críticas de SQS, Lambda, EMR y costo
deben vivir en CloudWatch/SNS para seguir operando cuando el host esté apagado.

### 12.2 Prometheus

**Archivo:** `monitoring/prometheus/prometheus.yml`.

```yaml
global:
  scrape_interval: 30s

rule_files:
  - /etc/prometheus/alerts.yml

alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["prometheus:9090"]

  - job_name: host
    static_configs:
      - targets: ["node-exporter:9100"]

  - job_name: containers
    static_configs:
      - targets: ["cadvisor:8080"]

  - job_name: airflow
    static_configs:
      - targets: ["statsd-exporter:9102"]
```

**Archivo:** `monitoring/prometheus/alerts.yml`.

```yaml
groups:
  - name: platform
    rules:
      - alert: HostDiskAlmostFull
        expr: 100 * (1 - node_filesystem_avail_bytes{mountpoint="/data"} / node_filesystem_size_bytes{mountpoint="/data"}) > 85
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "/data supera 85%"

      - alert: AirflowSchedulerMissing
        expr: absent(airflow_scheduler_heartbeat)
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "No hay heartbeat del scheduler"
```

Valida el nombre real de la métrica del scheduler en StatsD exporter antes de activar la segunda
regla; el mapping puede cambiar según la versión de Airflow.

### 12.3 Acceso

Los puertos locales se enlazan a `127.0.0.1` en Compose. Accede mediante un túnel:

```bash
ssh -i ~/.ssh/pyspark_stack \
  -L 8082:127.0.0.1:8082 \
  -L 3000:127.0.0.1:3000 \
  -L 9090:127.0.0.1:9090 \
  -L 9093:127.0.0.1:9093 \
  -L 3100:127.0.0.1:3100 \
  "ec2-user@${PUBLIC_IP}"
```

Comprueba salud desde otra terminal:

```bash
curl -fsS http://127.0.0.1:3000/api/health
curl -fsS http://127.0.0.1:9090/-/healthy
curl -fsS http://127.0.0.1:9093/-/healthy
curl -fsS http://127.0.0.1:3100/ready
```

### 12.4 Playbooks

**DAG fallido**

1. Lee el log de la task.
2. Identifica si falló Airflow o el servicio remoto.
3. Si existe job ID de EMR, revisa `stateDetails` y logs.
4. Corrige la causa; después limpia o reintenta la task.

**Evento no procesado**

1. Comprueba que el objeto cumple prefijo y sufijo.
2. Revisa mensajes visibles y no visibles en SQS.
3. Revisa errores y throttles de Lambda.
4. Revisa la DLQ.
5. Reprocesa con el mismo `bucket`, `key` y `sequencer`.

**EC2 sin espacio**

1. No borres `/data/postgres`.
2. Revisa logs Docker, Loki y Prometheus.
3. Aplica retención o amplía EBS.
4. Crea snapshot antes de una modificación de volumen.

---

## 13. Hardening y secretos

### 13.1 Reglas

- No guardes access keys en EC2, Airflow, `.env` o GitHub.
- Usa roles distintos para EC2, EMR job, Lambda y GitHub.
- Restringe SSM al ARN de la instancia y al documento requerido.
- Exige IMDSv2.
- Mantén S3 privado, cifrado y con política `aws:SecureTransport`.
- Enlaza UIs a loopback o limita 443 a un `/32`.
- Registra CloudTrail y revisa Access Analyzer.
- No uses tags flotantes como `latest`; actualiza imágenes de forma deliberada.

### 13.2 Crear secretos

**Dónde:** terminal administrativa, una sola vez.

```bash
PREFIX="/pyspark-stack"

put_secret() {
  aws ssm put-parameter \
    --name "${PREFIX}/$1" \
    --type SecureString \
    --value "$2" \
    --overwrite
}

put_secret postgres_password "$(openssl rand -hex 24)"
put_secret airflow_jwt_secret "$(openssl rand -hex 32)"
put_secret airflow_admin_password "$(openssl rand -hex 20)"
put_secret grafana_admin_password "$(openssl rand -hex 20)"
```

No imprimas los valores ni uses `set -x`. Si eliges crear `SecureString` con Terraform, recuerda
que el valor queda almacenado en el state.

### 13.3 Permitir lectura desde EC2

**Archivo:** `infra/prod/iam.tf`.

```hcl
data "aws_iam_policy_document" "ec2_parameters" {
  statement {
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
    ]
    resources = [
      "arn:aws:ssm:${local.region}:${local.account_id}:parameter/${var.name_prefix}/*"
    ]
  }
}

resource "aws_iam_role_policy" "ec2_parameters" {
  name   = "${var.name_prefix}-parameters"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_parameters.json
}
```

Agrega `kms:Decrypt` únicamente si usas una KMS administrada por el cliente, y limita el recurso a
esa clave.

### 13.4 Materializar `.env`

**Archivo:** `scripts/load-secrets.sh`, ejecutado en la EC2.

```bash
#!/usr/bin/env bash
set -euo pipefail
umask 077

PREFIX="${PARAMETER_PREFIX:-/pyspark-stack}"
REGION="${AWS_REGION:-us-east-1}"

get_secret() {
  aws ssm get-parameter \
    --name "${PREFIX}/$1" \
    --with-decryption \
    --query Parameter.Value \
    --output text \
    --region "$REGION"
}

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
EMR_APP_ID="$(aws emr-serverless list-applications \
  --query "applications[?name=='pyspark-stack-spark'].id | [0]" \
  --output text \
  --region "$REGION")"

cat > .env <<EOF
POSTGRES_USER=airflow
POSTGRES_DB=airflow
POSTGRES_PASSWORD=$(get_secret postgres_password)
AIRFLOW_JWT_SECRET=$(get_secret airflow_jwt_secret)
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$(get_secret airflow_admin_password)
GRAFANA_ADMIN_PASSWORD=$(get_secret grafana_admin_password)
EMR_APP_ID=${EMR_APP_ID}
EMR_JOB_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/pyspark-stack-emr-serverless-job
DATALAKE_BUCKET=pyspark-stack-datalake-${ACCOUNT_ID}
ARTIFACTS_BUCKET=pyspark-stack-artifacts-${ACCOUNT_ID}
EOF

chmod 600 .env
```

`.env` es un archivo efímero con secretos. Debe estar en `.gitignore`, no copiarse mediante rsync
y regenerarse en cada host nuevo.

### 13.5 Riesgos aceptados

`cAdvisor` usa acceso privilegiado y Promtail lee `docker.sock`. Son componentes sensibles:

- no publiques sus puertos;
- no ejecutes workloads de usuario dentro de ellos;
- mantén imágenes pineadas;
- elimínalos si CloudWatch Container Insights cubre tus necesidades.

---

## 14. Compose canónico de producción

### 14.1 `docker-compose.prod.yml`

Este es el único Compose de producción. Spark, HDFS y Jupyter no se ejecutan en la EC2.

**Archivo:** `docker-compose.prod.yml`.

```yaml
x-airflow-common: &airflow-common
  image: pyspark_stack-airflow-prod:3.2.2
  build:
    context: .
    dockerfile: Dockerfile.airflow.prod
  env_file: [.env]
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@airflow-db:5432/${POSTGRES_DB}
    AIRFLOW__CORE__LOAD_EXAMPLES: "False"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "False"
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: "30"
    AIRFLOW__METRICS__STATSD_ON: "True"
    AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
    AIRFLOW__METRICS__STATSD_PORT: "9125"
    AIRFLOW__METRICS__STATSD_PREFIX: airflow
    AIRFLOW_VAR_EMR_APP_ID: ${EMR_APP_ID}
    AIRFLOW_VAR_EMR_JOB_ROLE_ARN: ${EMR_JOB_ROLE_ARN}
    AIRFLOW_VAR_DATALAKE: ${DATALAKE_BUCKET}
    AIRFLOW_VAR_ARTIFACTS: ${ARTIFACTS_BUCKET}
  volumes:
    - ./dags:/opt/airflow/dags:ro
  restart: unless-stopped
  logging:
    driver: json-file
    options:
      max-size: 10m
      max-file: "3"
  deploy:
    resources:
      limits:
        memory: 1g
  networks: [platform]

x-common-logging: &common-logging
  logging:
    driver: json-file
    options:
      max-size: 10m
      max-file: "3"

services:
  airflow-db:
    image: postgres:16
    container_name: airflow-db
    restart: unless-stopped
    <<: *common-logging
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - /data/postgres:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      timeout: 5s
      retries: 12
    deploy:
      resources:
        limits:
          memory: 768m
    networks: [platform]

  airflow-init:
    <<: *airflow-common
    container_name: airflow-init
    restart: "no"
    depends_on:
      airflow-db:
        condition: service_healthy
    command: >
      bash -euc '
        airflow db migrate;
        airflow fab-db migrate;
        airflow users list | grep -q "$${AIRFLOW_ADMIN_USER}" ||
        airflow users create
          --username "$${AIRFLOW_ADMIN_USER}"
          --firstname Admin
          --lastname User
          --role Admin
          --email admin@example.com
          --password "$${AIRFLOW_ADMIN_PASSWORD}"
      '

  airflow-apiserver:
    <<: *airflow-common
    container_name: airflow-apiserver
    command: api-server
    ports:
      - 127.0.0.1:8082:8080
    depends_on:
      airflow-db:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    volumes:
      - ./dags:/opt/airflow/dags:ro
      - ./dbt:/opt/dbt:ro
      - ./quality:/opt/quality:ro
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-dag-processor:
    <<: *airflow-common
    container_name: airflow-dag-processor
    command: dag-processor
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-triggerer:
    <<: *airflow-common
    container_name: airflow-triggerer
    command: triggerer
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  prometheus:
    image: prom/prometheus:v2.54.1
    container_name: prometheus
    restart: unless-stopped
    <<: *common-logging
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=15d
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./monitoring/prometheus/alerts.yml:/etc/prometheus/alerts.yml:ro
      - /data/prometheus:/prometheus
    ports:
      - 127.0.0.1:9090:9090
    networks: [platform]

  alertmanager:
    image: prom/alertmanager:v0.27.0
    container_name: alertmanager
    restart: unless-stopped
    <<: *common-logging
    command: ["--config.file=/etc/alertmanager/alertmanager.yml"]
    volumes:
      - ./monitoring/alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
    ports:
      - 127.0.0.1:9093:9093
    networks: [platform]

  grafana:
    image: grafana/grafana:11.2.0
    container_name: grafana
    restart: unless-stopped
    <<: *common-logging
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD}
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - /data/grafana:/var/lib/grafana
    ports:
      - 127.0.0.1:3000:3000
    networks: [platform]

  node-exporter:
    image: prom/node-exporter:v1.8.2
    container_name: node-exporter
    restart: unless-stopped
    <<: *common-logging
    command:
      - --path.rootfs=/host
    pid: host
    volumes:
      - /:/host:ro,rslave
    networks: [platform]

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.1
    container_name: cadvisor
    restart: unless-stopped
    <<: *common-logging
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker:/var/lib/docker:ro
    networks: [platform]

  statsd-exporter:
    image: prom/statsd-exporter:v0.27.1
    container_name: statsd-exporter
    restart: unless-stopped
    <<: *common-logging
    command:
      - --statsd.mapping-config=/etc/statsd/statsd_mapping.yml
      - --statsd.listen-udp=:9125
      - --web.listen-address=:9102
    volumes:
      - ./monitoring/statsd/statsd_mapping.yml:/etc/statsd/statsd_mapping.yml:ro
    networks: [platform]

  loki:
    image: grafana/loki:3.1.1
    container_name: loki
    restart: unless-stopped
    <<: *common-logging
    command: ["-config.file=/etc/loki/loki-config.yml"]
    volumes:
      - ./monitoring/loki/loki-config.yml:/etc/loki/loki-config.yml:ro
      - /data/loki:/loki
    ports:
      - 127.0.0.1:3100:3100
    networks: [platform]

  promtail:
    image: grafana/promtail:3.1.1
    container_name: promtail
    restart: unless-stopped
    <<: *common-logging
    command: ["-config.file=/etc/promtail/promtail-config.yml"]
    volumes:
      - ./monitoring/promtail/promtail-config.yml:/etc/promtail/promtail-config.yml:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks: [platform]

networks:
  platform:
```

Antes de arrancar:

```bash
./scripts/load-secrets.sh
docker compose -f docker-compose.prod.yml config --quiet
docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml ps
```

---

## 15. Runbook de puesta en producción

### Paso 1 — Validar localmente

```bash
aws sts get-caller-identity
terraform -chdir=infra/prod fmt -check -recursive
terraform -chdir=infra/prod init
terraform -chdir=infra/prod validate
terraform -chdir=infra/prod plan -out=tfplan
terraform -chdir=infra/prod show tfplan
docker compose -f docker-compose.prod.yml config --quiet
pytest -q
```

No apliques si el plan reemplaza la EC2, EBS o buckets sin que ese sea el objetivo explícito.

### Paso 2 — Aplicar infraestructura

```bash
terraform -chdir=infra/prod apply tfplan
```

### Paso 3 — Preparar el host

```bash
aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"

ssh -i ~/.ssh/pyspark_stack "ec2-user@${PUBLIC_IP}" \
  "cloud-init status --wait &&
   mountpoint /data &&
   systemctl is-active docker"
```

### Paso 4 — Desplegar

```bash
rsync -az \
  --exclude .git \
  --exclude .env \
  --exclude infra \
  -e "ssh -i ~/.ssh/pyspark_stack" \
  ./ "ec2-user@${PUBLIC_IP}:/home/ec2-user/pyspark_stack/"

ssh -i ~/.ssh/pyspark_stack "ec2-user@${PUBLIC_IP}" \
  "cd /home/ec2-user/pyspark_stack &&
   ./scripts/load-secrets.sh &&
   docker compose -f docker-compose.prod.yml up -d --build"
```

### Paso 5 — Publicar entrypoints EMR

```bash
aws s3 sync spark-apps/emr/ "s3://${ARTIFACTS_BUCKET}/emr/" --delete
```

### Paso 6 — Validar

Ejecuta las secciones 8.2 y 8.3. La promoción termina solo cuando:

- no existen errores de importación;
- el DAG termina;
- el job EMR termina en `SUCCESS`;
- los datos aparecen en `curated/`;
- las métricas y logs son consultables;
- no existen mensajes inesperados en DLQ.

### Paso 7 — Registrar evidencia

Guarda el commit, el `terraform plan`, el DAG run ID, el EMR job ID y el resultado del smoke test.
Eso convierte un despliegue manual en un cambio auditable.

---

## 16. Athena e Iceberg

Athena se usa para consumo SQL, controles y dbt. Spark sigue siendo el motor principal para ETL
pesado.

### 16.1 Workgroup

**Archivo:** `infra/prod/athena.tf`.

```hcl
resource "aws_athena_workgroup" "analytics" {
  name = "${var.name_prefix}-analytics"

  configuration {
    enforce_workgroup_configuration = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 10737418240

    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
    }
  }
}
```

El corte de 10 GiB evita una consulta accidentalmente costosa. Ajusta el valor al tamaño real de
las tablas.

### 16.2 Consultas operativas

```sql
SELECT dt, count(*) AS filas
FROM pyspark_stack_analytics.customer
WHERE dt >= current_date - interval '7' day
GROUP BY dt
ORDER BY dt DESC;
```

Control de calidad:

```sql
SELECT
  count(*) AS filas,
  count_if(customer_id IS NULL) AS customer_id_nulo,
  count(*) - count(DISTINCT event_id) AS duplicados
FROM pyspark_stack_analytics.customer
WHERE dt = current_date;
```

El pipeline debe fallar si `customer_id_nulo > 0` o `duplicados > 0`.

### 16.3 Mantenimiento Iceberg

Ejecuta mantenimiento fuera de la ventana ETL:

```sql
OPTIMIZE pyspark_stack_analytics.customer
REWRITE DATA USING BIN_PACK
WHERE dt >= current_date - interval '7' day;

VACUUM pyspark_stack_analytics.customer;
```

La retención de snapshots debe respetar la política de auditoría y la necesidad de *time travel*.

---

## 17. Qué motor usar para cada tarea

### 17.1 Python puro

Úsalo para APIs, archivos pequeños y tareas de control:

```python
from airflow.sdk import task


@task(retries=2)
def validate_manifest(manifest: dict) -> None:
    required = {"bucket", "key", "checksum"}
    missing = required - manifest.keys()
    if missing:
        raise ValueError(f"Campos faltantes: {sorted(missing)}")
```

### 17.2 PySpark

Úsalo cuando necesites distribución:

```python
from pyspark.sql import functions as F

clean = (
    raw.filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["event_id"])
    .withColumn("dt", F.to_date("event_time"))
)
```

### 17.3 SQL/dbt

Úsalo para modelos analíticos:

```sql
select
  customer_id,
  count(*) as orders,
  sum(total_amount) as revenue
from {{ ref('stg_orders') }}
group by customer_id
```

No mezcles los tres motores en un mismo paso sin necesidad. Cada frontera agrega logs, permisos y
puntos de fallo.

---

## 18. Gobierno, resiliencia y costos

### 18.1 DLQ según el origen

No existe una DLQ universal:

| Camino | Mecanismo correcto |
|---|---|
| S3 → SQS → Lambda | `redrive_policy` en la cola SQS |
| EventBridge Scheduler → Lambda | `dead_letter_config` y `retry_policy` del schedule |
| Invocación Lambda asíncrona directa | DLQ o destination de Lambda |

Fragmento para la cola de eventos:

```hcl
resource "aws_sqs_queue" "trigger_dlq" {
  name                      = "${var.name_prefix}-trigger-dlq"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "trigger_events" {
  name                       = "${var.name_prefix}-trigger-events"
  visibility_timeout_seconds = 360

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.trigger_dlq.arn
    maxReceiveCount     = 5
  })
}
```

Fragmento dentro de cada `aws_scheduler_schedule`:

```hcl
target {
  arn      = aws_lambda_function.trigger_airflow.arn
  role_arn = aws_iam_role.scheduler.arn

  dead_letter_config {
    arn = aws_sqs_queue.scheduler_dlq.arn
  }

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 5
  }
}
```

El rol del Scheduler necesita `sqs:SendMessage` sobre su DLQ.

### 18.2 Alarmas

Como mínimo:

- mensajes visibles en cualquier DLQ;
- errores y throttles de Lambda;
- edad del mensaje más antiguo en SQS;
- jobs fallidos de EMR;
- EC2 en ejecución fuera de la ventana;
- gasto real y gasto proyectado.

### 18.3 Budget

```hcl
variable "monthly_budget_usd" {
  type        = number
  description = "Presupuesto mensual de producción"
}

variable "alert_email" {
  type        = string
  description = "Destino de alertas operativas y de costo"
}

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
```

No copies un monto fijo de otra cuenta. Calcula una línea base con Cost Explorer y añade margen.

### 18.4 Cost Anomaly Detection y Access Analyzer

```hcl
resource "aws_ce_anomaly_monitor" "services" {
  name              = "${var.name_prefix}-services"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"
}

resource "aws_accessanalyzer_analyzer" "account" {
  analyzer_name = "${var.name_prefix}-external-access"
  type          = "ACCOUNT"
}
```

El analizador de acceso externo es regional. Créalo en cada región donde existan recursos
compatibles.

### 18.5 Palancas de ahorro

Orden recomendado:

1. Evitar ejecuciones duplicadas mediante idempotencia y un evento centinela.
2. Mantener EMR sin capacidad preinicializada cuando la latencia no sea crítica.
3. Apagar la EC2 al terminar el último DAG.
4. Limitar capacidad máxima y duración de jobs EMR.
5. Aplicar lifecycle a S3, logs y snapshots.
6. Revisar EIP, NAT Gateway y recursos sin uso.
7. Etiquetar `Environment`, `Service`, `Owner` y `CostCenter`.

EMR Serverless inicia automáticamente con el job y, por defecto, puede detenerse al quedar ocioso.
La capacidad máxima es un límite de seguridad y costo, no una reserva.

---

## 19. Transformaciones con dbt

### 19.1 Estructura

```text
dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   └── marts/
└── tests/
```

### 19.2 Modelo

**Archivo:** `dbt/models/marts/customer_summary.sql`.

```sql
{{ config(materialized='table', format='parquet') }}

select
  customer_id,
  count(*) as orders,
  sum(total_amount) as revenue,
  max(order_ts) as last_order_ts
from {{ ref('stg_orders') }}
group by customer_id
```

### 19.3 Ejecución desde Airflow

```python
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator

dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command=(
        "cd /opt/dbt && "
        "dbt deps && "
        "dbt build --target prod --select state:modified+"
    ),
    execution_timeout=timedelta(minutes=45),
)
```

En CI usa una database y un prefijo S3 separados. Un pull request nunca debe escribir en las tablas
de producción.

---

## 20. Calidad de datos

La calidad no es un reporte posterior: es una puerta entre `curated` y `analytics`.

### 20.1 Controles mínimos

- esquema y tipos;
- clave primaria no nula;
- duplicados;
- rango y dominio;
- frescura;
- volumen frente a la línea base;
- integridad entre datasets.

### 20.2 Gate SQL desde Airflow

Configura la conexión Airflow `athena_default` con región, workgroup y ubicación de resultados.

```python
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

quality_gate = SQLCheckOperator(
    task_id="quality_gate",
    conn_id="athena_default",
    sql="""
    SELECT
      count(*) > 0 AS tiene_filas,
      count_if(customer_id IS NULL) = 0 AS clave_completa,
      count(*) = count(DISTINCT event_id) AS sin_duplicados
    FROM pyspark_stack_analytics.customer
    WHERE dt = DATE '{{ ds }}'
    """,
)
```

`SQLCheckOperator` convierte cada valor de la primera fila a booleano y falla si alguno es falso.
Para suites grandes, usa Great Expectations con una versión fijada y un checkpoint ejecutado como
una task separada.

### 20.3 Orden del pipeline

```text
ingesta → validación básica → ETL → calidad → dbt → publicación → lineage
```

Los datos que no pasan calidad no deben promoverse a `analytics`.

---

## 21. Control de cambios y límites

### 21.1 Límites aceptados

- Airflow, Postgres y monitoreo comparten una EC2.
- Cuando la EC2 está apagada, no hay UI ni alertas locales.
- El state de Terraform tiene un radio de impacto amplio.
- SSM permite ejecución remota privilegiada.
- EBS snapshots y S3 versioning no sustituyen una prueba de restauración.
- El apagado seguro prefiere mantener la EC2 encendida si no puede comprobar DAGs activos.

### 21.2 Cambio seguro

```bash
terraform -chdir=infra/prod fmt -check -recursive
terraform -chdir=infra/prod init -upgrade=false
terraform -chdir=infra/prod validate
terraform -chdir=infra/prod plan -out=tfplan
terraform -chdir=infra/prod show tfplan

docker compose -f docker-compose.prod.yml config --quiet
python -m compileall infra/prod/lambda dags spark-apps
pytest -q
```

Secuencia:

1. Cambia una categoría: infraestructura, imagen o aplicación.
2. Revisa el plan y el diff.
3. Despliega desde un solo canal.
4. Ejecuta smoke test y una corrida controlada.
5. Promueve o revierte.

### 21.3 Recuperación

Prueba trimestralmente:

1. crear una EC2 de recuperación;
2. adjuntar un snapshot de `/data`;
3. regenerar `.env` desde SSM;
4. levantar Postgres y Airflow;
5. sincronizar DAGs desde artifacts;
6. disparar un DAG de prueba;
7. registrar RTO y problemas encontrados.

### 21.4 Teardown

El teardown de una plataforma con datos es destructivo. Debe:

- cancelar jobs EMR activos;
- destruir `infra/prod` antes de `infra/bootstrap`;
- vaciar todas las versiones y delete markers de S3 solo con aprobación;
- desactivar temporalmente `prevent_destroy`;
- restaurar las guardas aunque el proceso falle;
- comprobar en AWS que no quedaron recursos facturando.

Usa el script de teardown de la edición anterior únicamente en entornos descartables. En producción,
la eliminación de EBS, buckets y backend requiere una revisión separada y respaldo verificado.

---

## 22. Lineage con OpenLineage

OpenLineage responde qué job produjo un dataset y qué entradas utilizó.

### 22.1 Cobertura

| Capa | Cobertura |
|---|---|
| Airflow | DAG, task, inputs y outputs declarados |
| dbt | modelos, dependencias y columnas |
| Spark | requiere el listener OpenLineage |

### 22.2 Recomendación

No uses un archivo local compartido como backend definitivo: varios procesos pueden escribir al
mismo archivo, el host puede apagarse y EMR Serverless no puede acceder a ese disco.

Para evaluación, el transporte de archivo es suficiente. Para producción, usa un backend HTTP
alcanzable y autenticado, como Marquez u otra plataforma compatible.

Configuración Airflow:

```yaml
AIRFLOW__OPENLINEAGE__NAMESPACE: pyspark-stack-prod
AIRFLOW__OPENLINEAGE__TRANSPORT: >
  {"type":"http","url":"${OPENLINEAGE_URL}","auth":{"type":"api_key","apiKey":"${OPENLINEAGE_API_KEY}"}}
```

Configuración Spark:

```text
--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
--conf spark.openlineage.namespace=pyspark-stack-prod
--conf spark.openlineage.transport.type=http
--conf spark.openlineage.transport.url=<endpoint>
```

El endpoint debe ser alcanzable desde EMR Serverless. No expongas Marquez en la EC2 solo para
resolver lineage: evalúa el impacto de red, autenticación, disponibilidad y costo.

---

## Referencias operativas oficiales

- [EMR Serverless: comportamiento de aplicaciones](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html)
- [EMR Serverless: métricas y monitoreo](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-job-metrics.html)
- [EMR Serverless: almacenamiento de logs](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/logging.html)
- [S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)
- [DLQ de EventBridge Scheduler](https://docs.aws.amazon.com/scheduler/latest/UserGuide/configuring-schedule-dlq.html)
- [IAM Access Analyzer](https://docs.aws.amazon.com/IAM/latest/UserGuide/what-is-access-analyzer.html)
- [Airflow: operadores deferrable](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
