# Guía experta — Producción en AWS (Terraform, Airflow en EC2 + EMR Serverless)

> Guía única de producción para el stack. Implementa una sola arquitectura **híbrida** —la de
> [`docs/03-arquitectura.md`](03-arquitectura.md)— con Terraform copy-paste, estado remoto y
> automatización con EventBridge + Lambda para bajar el costo al mínimo.
>
> La arquitectura, en un solo camino: una EC2 **chica** (`t3.large`) corre solo el *orquestador*
> —Airflow + Postgres + monitoreo— en Docker, y **Spark ya no corre en la EC2**: los DAGs disparan los
> jobs en **EMR Serverless** (pago por uso, escala a cero). Alrededor, servicios AWS lo
> complementan: S3 como data lake (`s3a://`/`s3://` con rol IAM), Lambda + EventBridge para
> disparar los DAGs (por cron o por evento) y para el auto start/stop de la EC2, Prometheus +
> Grafana + Alertmanager + Loki para observabilidad, y CI/CD con GitHub Actions (OIDC). Airflow
> **sigue siendo el orquestador** (no Step Functions); usa EMR Serverless para el cómputo Spark, y
> no usa MWAA ni Glue (el porqué está en la guía de arquitectura).

Índice:
1. [Panorama de la arquitectura](#1-panorama-de-la-arquitectura)
2. [Costo](#2-costo)
3. [Prerrequisitos](#3-prerrequisitos)
4. [Fundamentos: backend Terraform (S3)](#4-fundamentos-backend-terraform)
5. [Núcleo: EC2 con Docker](#5-núcleo-ec2-con-docker)
   - 5.1 [Variables y red (SSH + web de Airflow a tu IP)](#51-variables-y-red)
   - 5.2 [IAM + key pair](#52-iam--key-pair)
   - 5.3 [EC2 + EBS + user_data](#53-ec2--ebs--user_data)
   - 5.4 [**Automatización: EventBridge + Lambda (auto start/stop)**](#54-automatización-eventbridge--lambda)
   - 5.5 [Desplegar, subir código y túnel SSH](#55-desplegar-subir-código-y-túnel-ssh)
   - 5.6 [**Exponer la web de Airflow (HTTPS nativo, solo tu IP)**](#56-exponer-la-web-de-airflow-https-nativo-solo-tu-ip)
6. [Data lake en S3 (s3a con rol IAM) + backups](#6-data-lake-en-s3)
   - 6.1 [Buckets S3 (data lake + artifacts)](#61-buckets-s3)
   - 6.2 [IAM: permitir s3a a la EC2 (sin keys)](#62-iam-permitir-s3a-a-la-ec2-sin-keys)
   - 6.3 [Backups: snapshots EBS automáticos (DLM)](#63-backups-snapshots-ebs-automáticos-dlm)
   - 6.4 [**Cómputo Spark: EMR Serverless (app + roles + submit)**](#64-cómputo-spark-emr-serverless)
   - 6.5 [S3 VPC Gateway Endpoint (EC2↔S3 sin salir a internet)](#65-s3-vpc-gateway-endpoint)
7. [Orquestación: Lambda trigger-airflow (SSM) + EventBridge + event-driven](#7-orquestación-lambda-trigger-airflow-ssm--eventbridge--event-driven)
   - 7.1 [Lambda que dispara los DAGs vía SSM](#71-lambda-que-dispara-los-dags-vía-ssm)
   - 7.2 [Disparo por cron (EventBridge Scheduler)](#72-disparo-por-cron-eventbridge-scheduler)
   - 7.3 [Disparo por evento (archivo nuevo en S3, vía SQS)](#73-disparo-por-evento-archivo-nuevo-en-s3-vía-sqs)
8. [Operación, seguridad y ahorro](#8-operación-seguridad-y-ahorro)
    - 8.1 [Validación (smoke tests), capa por capa](#81-validación-smoke-tests-capa-por-capa)
    - 8.2 [Cheat-sheet de operación](#82-cheat-sheet-de-operación)
    - 8.3 [Seguridad (checklist)](#83-seguridad-checklist)
    - 8.4 [Palancas de ahorro (orden de impacto)](#84-palancas-de-ahorro-orden-de-impacto)
    - 8.5 [Teardown limpio: terraform destroy que sí termina](#85-teardown-limpio-terraform-destroy-que-sí-termina)
9. [Notebooks: dónde viven y cómo se ejecutan](#9-notebooks-dónde-viven-y-cómo-se-ejecutan)
   - 9.0 [Patrones de tarea para ETL batch (¿PySpark o Python puro?)](#90-patrones-de-tarea-para-etl-batch-pyspark-o-python-puro)
   - 9.1 [Instalar los providers de Airflow (amazon, papermill) y sus dependencias](#91-instalar-los-providers-de-airflow-amazon-papermill-y-sus-dependencias)
   - 9.2 [Parametrizar el notebook](#92-parametrizar-el-notebook)
   - 9.3 [DAG que ejecuta el notebook](#93-dag-que-ejecuta-el-notebook--dagsrun_notebook_dagpy)
10. [Flujo local → servidor → los DAGs corren solos](#10-flujo-local--servidor--los-dags-corren-solos)
    - 10.1 [Modo rápido (dev): deploy manual sin CI](#101-modo-rápido-dev-deploy-manual-sin-esperar-ci)
    - 10.2 [Del laptop a producción — el loop completo](#102-del-laptop-a-producción--el-loop-completo-dev--deploy--corre-solo--se-apaga)
    - 10.3 [Apagado job-aware: se apaga cuando terminan los jobs](#103-apagado-job-aware-se-apaga-cuando-terminan-los-jobs-no-a-hora-fija)
    - 10.4 [Concurrencia y sizing — muchos jobs a la vez](#104-concurrencia-y-sizing--muchos-jobs-a-la-vez)
11. [CI/CD con GitHub Actions (OIDC, sin claves)](#11-cicd-con-github-actions-oidc-sin-claves)
    - 11.1 [Terraform: OIDC provider + rol](#111-terraform-oidc-provider--rol--infraprodcicdtf)
    - 11.1b [Un segundo rol OIDC, solo para dbt Slim CI](#111b-un-segundo-rol-oidc-solo-para-dbt-slim-ci-menos-privilegios-sin-gate-de-aprobación)
    - 11.2 [Workflow de CI](#112-workflow-de-ci--githubworkflowsciyml)
    - 11.3 [Workflow de Deploy](#113-workflow-de-deploy--githubworkflowsdeployyml)
    - 11.4 [Puesta en marcha](#114-puesta-en-marcha-una-vez)
12. [Monitoreo (Prometheus + Grafana + Alertmanager + Loki)](#12-monitoreo-prometheus--grafana--alertmanager--loki)
    - 12.1 [Qué se monitorea y análisis de completitud](#121-qué-se-monitorea-y-análisis-de-completitud)
    - 12.2 [Estructura de archivos](#122-estructura-de-archivos-monitoring)
    - 12.3 [Servicios de monitoreo (compose)](#123-servicios-de-monitoreo-compose)
    - 12.4 [Prometheus, reglas de alerta y Alertmanager](#124-prometheus-reglas-de-alerta-y-alertmanager)
    - 12.5 [Métricas de Airflow (StatsD)](#125-métricas-de-airflow-statsd)
    - 12.6 [Grafana (datasources + dashboard)](#126-grafana-datasources--dashboard)
    - 12.7 [Logs: Loki + Promtail](#127-logs-loki--promtail)
    - 12.8 [Acceso, verificación y observabilidad de EMR Serverless](#128-acceso-verificación-y-observabilidad-de-emr-serverless)
13. [Hardening de producción (secretos, restart, límites, s3a)](#13-hardening-de-producción)
    - 13.1 [Secretos y parámetros con AWS (Parameter Store + Secrets Manager)](#131-secretos-y-parámetros-con-aws-parameter-store--secrets-manager)
    - 13.2 [restart + límites + logging en los servicios del stack](#132-restart--límites--logging-en-los-servicios-del-stack)
    - 13.3 [Spark ↔ S3 con el rol IAM (s3a) en EMR Serverless](#133-spark--s3-con-el-rol-iam-s3a-en-emr-serverless)
    - 13.4 [docker.sock en Airflow](#134-dockersock-en-airflow)
    - 13.5 [Higiene del repo](#135-higiene-del-repo)
    - 13.6 [UI de jobs terminados (Spark UI de EMR Serverless)](#136-ui-de-jobs-terminados-spark-ui-de-emr-serverless)
    - 13.7 [Checklist final (production-ready)](#137-checklist-final-production-ready)
14. [Archivos compose completos (copy-paste)](#14-archivos-compose-completos)
    - 14.1 [docker-compose.prod.yml (producción, completo)](#141-docker-composeprodyml-producción-completo)
15. [Puesta en producción — runbook final](#15-puesta-en-producción--runbook-final)
16. [Athena — capa de consumo SQL/BI (opcional)](#16-athena--capa-de-consumo-sqlbi-opcional)
    - 16.1 [Tablas Iceberg: ACID, time travel y MERGE desde SQL, sin crawler](#161-tablas-iceberg-acid-time-travel-y-merge-desde-sql-sin-crawler)
    - 16.1b [Mantenimiento: compactación y expiración de snapshots](#161b-mantenimiento-compactación-y-expiración-de-snapshots-auditoría-21)
    - 16.2 [Terraform mínimo — workgroup + resultados](#162-terraform-mínimo--workgroup--resultados)
    - 16.3 [IAM — permitir que un DAG consulte (rol de la EC2)](#163-iam--permitir-que-un-dag-consulte-rol-de-la-ec2)
    - 16.4 [Uso en un DAG — assert de calidad post-ETL](#164-uso-en-un-dag--assert-de-calidad-post-etl)
    - 16.5 [Costo y cuándo (no) usarla](#165-costo-y-cuándo-no-usarla)
17. [Airflow, 3 sabores: ejemplo + monitoreo de cada uno](#17-airflow-3-sabores-ejemplo--monitoreo-de-cada-uno)
    - 17.1 [Python puro (en la EC2)](#171-python-puro-en-la-ec2)
    - 17.2 [PySpark en EMR Serverless](#172-pyspark-en-emr-serverless)
    - 17.3 [SQL con Athena](#173-sql-con-athena)
18. [Gobierno, costo y resiliencia (extras)](#18-gobierno-costo-y-resiliencia-extras)
    - 18.1 [DLQ para las Lambdas trigger-airflow y startstop](#181-dlq-para-las-lambdas-trigger-airflow-y-startstop)
    - 18.2 [AWS Budgets](#182-aws-budgets)
    - 18.3 [Cost Anomaly Detection](#183-cost-anomaly-detection)
    - 18.4 [IAM Access Analyzer](#184-iam-access-analyzer)
    - 18.5 [Verificación](#185-verificación)
19. [Transformaciones SQL con dbt](#19-transformaciones-sql-con-dbt)
    - 19.1 [Dos targets, uno por tipo de transformación](#191-dos-targets-uno-por-tipo-de-transformación)
    - 19.2 [Instalación](#192-instalación)
    - 19.3 [Estructura del proyecto y profiles.yml](#193-estructura-del-proyecto-y-profilesyml)
    - 19.4 [Modelo de ejemplo (target athena) y task de Airflow](#194-modelo-de-ejemplo-target-athena-y-task-de-airflow)
    - 19.5 [Docs de dbt (opcional)](#195-docs-de-dbt-opcional)
20. [Calidad de datos con Great Expectations](#20-calidad-de-datos-con-great-expectations)
    - 20.1 [Instalación](#201-instalación)
    - 20.2 [Estructura del proyecto](#202-estructura-del-proyecto)
    - 20.3 [Expectation suite de ejemplo](#203-expectation-suite-de-ejemplo)
    - 20.4 [Task de Airflow](#204-task-de-airflow)
    - 20.5 [Reporte HTML (Data Docs, opcional)](#205-reporte-html-data-docs-opcional)
21. [Lineage de datos con OpenLineage](#21-lineage-de-datos-con-openlineage)
    - 21.1 [Qué cubre y qué no](#211-qué-cubre-y-qué-no)
    - 21.2 [Instalación](#212-instalación)
    - 21.3 [Config: eventos a un archivo local, sincronizados a S3](#213-config-eventos-a-un-archivo-local-sincronizados-a-s3)
    - 21.4 [Consultar el lineage desde Athena](#214-consultar-el-lineage-desde-athena)
    - 21.5 [Upgrade opcional: Marquez (UI navegable + lineage de Spark)](#215-upgrade-opcional-marquez-ui-navegable--lineage-de-spark)

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

Estructura de carpetas Terraform:

```text
infra/
├── bootstrap/           # crea el backend S3 (una vez, state local)
└── prod/                # toda la infra del stack (un solo estado)
    ├── lambda/startstop.py
    ├── lambda/trigger_airflow.py
    └── *.tf              # backend, providers, variables, locals, outputs,
                          # network, iam, ec2, s3, emr (EMR Serverless), backups,
                          # automation, orchestration, cicd, secrets
```

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
> **Opción A — Terraform lo sigue gestionando (recomendada).** Reaplicá con tu IP actual en una línea,
> sin tocar el `tfvars`:
>
> ```bash
> terraform -chdir=infra/prod apply -var "my_ip_cidr=$(curl -s https://checkip.amazonaws.com)/32"
> ```
>
> (Equivalente: actualizás `my_ip_cidr` en `terraform.tfvars` y hacés `apply`.) Sin setup extra ni
> permisos IAM nuevos. Es la que conviene si tu IP cambia cada tantos días o semanas.
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
  user_data                   = templatefile("${path.module}/user_data.sh.tftpl", {})
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
dnf update -y && dnf install -y docker git && systemctl enable --now docker

# Versión PINEADA (mismo criterio que las imágenes por @sha256): un boot de hoy y uno de dentro
# de 6 meses instalan lo mismo. Actualizala a propósito, no dejes que "latest" decida por vos.
COMPOSE_VERSION=v5.3.1
DOCKER_CONFIG=/usr/local/lib/docker
mkdir -p $DOCKER_CONFIG/cli-plugins
# OJO con el $$: este archivo pasa por templatefile(), así que Terraform interpretaría un ${...}
# como variable SUYA y fallaría ("vars map does not contain key"). $${...} emite un ${...} literal
# que expande bash en el boot. Todo ${...} que agregues acá y sea de bash necesita el mismo escape.
curl -fSL "https://github.com/docker/compose/releases/download/$${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
  -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
usermod -aG docker ec2-user

# Disco de datos (Nitro => /dev/nvme1n1). Solo formatea si está vacío (no borra en recreación).
# El attach del volumen llega después del primer boot: esperar hasta 2 min a que aparezca.
DATA_DEV=""
for i in $(seq 1 60); do
  DATA_DEV=$(lsblk -dpno NAME | grep -E 'nvme1n1|xvdf' | head -n1 || true)
  [ -n "$DATA_DEV" ] && break
  sleep 2
done
if [ -n "$DATA_DEV" ]; then
  blkid "$DATA_DEV" || mkfs -t xfs "$DATA_DEV"
  mkdir -p /data && mount "$DATA_DEV" /data
  # Por UUID, NO por nombre de device: la enumeración NVMe puede cambiar entre boots (AWS lo
  # documenta), y esta EC2 para y arranca todos los días (§5.4). Si el nombre baila, `nofail`
  # hace exactamente lo que promete —no falla, no monta— y los servicios que escriben en /data
  # quedan en crash-loop por permisos sobre un /data vacío del disco root.
  echo "UUID=$(blkid -s UUID -o value "$DATA_DEV") /data xfs defaults,nofail 0 2" >> /etc/fstab
  chown -R ec2-user:ec2-user /data
  # Bind mounts del compose de prod. Prometheus (65534), Grafana (472) y Loki (10001) corren
  # sin privilegios: sin este chown quedan en crash-loop por "permission denied".
  # Sin HDFS en prod (Spark salió a EMR Serverless), /data ya no lleva namenode/datanode.
  mkdir -p /data/postgres /data/prometheus /data/grafana /data/loki
  chown 65534:65534 /data/prometheus
  chown 472:472     /data/grafana
  chown 10001:10001 /data/loki
fi
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
    Con force=true apaga igual: es el cierre duro de las 22:00 (§7.2)."""
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
        # force=True SALTEA el guard, y solo lo manda el cron de cierre de las 22:00 (§7.2).
        # Sin él, un DAG colgado —que sigue en 'running' para siempre— o un agente SSM caído
        # (_dags_activos devuelve 1 por precaución) dejaban la EC2 prendida indefinidamente:
        # el respaldo de costo no existía justo en el caso que decía cubrir.
        # Contrapartida asumida: un job legítimo que cruce las 22:00 UTC se corta. Si tenés
        # jobs largos, corré el cron más tarde (var.stop_cron) en vez de sacar el force.
        if event.get("force"):
            ec2.stop_instances(InstanceIds=ids)
            return {"action": action, "instances": ids, "forced": True}

        activos = _dags_activos(ids[0])       # un solo nodo pyspark-stack-node
        if activos > 0:
            return {"msg": f"{activos} DAG run(s) activos, no apago", "instances": ids}
        ec2.stop_instances(InstanceIds=ids)

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
# defecto (auditoría §1.2) — a este volumen no pesa en dólares, pero es basura acumulándose para
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
    # force: es el cierre DURO del día. Saltea el guard job-aware a propósito (ver startstop.py):
    # sin esto, un DAG colgado dejaba la instancia encendida indefinidamente. El apagado normal
    # —el que sí respeta los jobs en vuelo— lo dispara la task trigger_stop del DAG, sin force.
    input    = jsonencode({ action = "stop", force = true })
  }
}
```

Los crons quedan activos desde este mismo `apply`: esa misma noche la EC2 se apaga, y como
`docker-compose.prod.yml` (Paso 0, §5.5) ya trae `restart: unless-stopped` en todos sus servicios,
todo el stack vuelve solo al prender — no hace falta esperar a §12-14 para eso.

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

Apagar/prender no degrada el rendimiento — cuatro garantías de diseño:

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
4. **El stack vuelve solo** — Docker arranca en boot y `restart: unless-stopped` relevanta los
   contenedores. Esa política ya está en `docker-compose.prod.yml` desde el Paso 0 (§5.5): todo el
   archivo la trae desde el primer `up`, no hace falta esperar al hardening de §13.2.

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

```hcl
# infra/prod/terraform.tfvars
my_ip_cidr     = "203.0.113.7/32"                     # curl -s https://checkip.amazonaws.com  (agregale /32)
ssh_public_key = "ssh-ed25519 AAAA...tu_clave... pyspark_stack" # cat ~/.ssh/pyspark_stack.pub
```

**Paso 0 — antes que nada, creá `docker-compose.prod.yml` en la raíz de tu repo LOCAL** (todavía no
existe). A diferencia de `docker-compose.yml` (el del dev local), **este NO es un override que se
fusiona**: es un archivo standalone, completo y autosuficiente, que arranca solo con
`-f docker-compose.prod.yml` — sin Spark, sin HDFS, sin Jupyter (acá no se usa: es solo un dashboard
de exploración de dev, y el ETL real de prod corre por Airflow + EMR Serverless + papermill, no
interactivo — si alguna vez lo necesitás para explorar datos productivos, hacé un túnel a un
Jupyter local apuntando a S3, no lo agregues a esta caja).
Es obligatorio, no opcional: si le hicieras `docker compose up` pelado a `docker-compose.yml` (el
de dev), levantarías Spark standalone + HDFS en la EC2 orquestadora, justo lo que este stack evita
moviendo Spark a EMR Serverless (§1, §6.4).

Esta es la versión **mínima** (Airflow + Postgres); las secciones 9 (papermill), 12 (monitoreo) y 13
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
  image: pyspark_stack-airflow:3.2.2
  build:
    context: .
    dockerfile: Dockerfile.airflow
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
# (la 1ª vez tarda: compila imágenes de Airflow) ───
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

> Lo aplicado hasta acá es el núcleo, no la infra final: se arma incrementalmente. El `apply` de acá crea solo
> lo definido hasta la §5. Las secciones 6-7 (data lake S3, orquestación), 11 (CI/CD) y 13
> (secretos) agregan más `.tf` a `infra/prod/`; cada vez que sumás archivos, volvés a correr
> `terraform apply`. Del mismo modo, el `docker-compose.prod.yml` del Paso 0 ya arranca **sin**
> Spark/HDFS (nunca estuvieron en el archivo); la puesta en producción real —monitoreo, env de EMR
> Serverless, hardening y secretos desde SSM— reemplaza ese archivo por una versión más completa
> (§12-14, final en §14.1): `./scripts/load-secrets.sh && docker compose -f docker-compose.prod.yml up -d`.

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
in-place). El FQDN viaja como `AIRFLOW_DOMAIN` (no es secreto): agregalo al `.env` con
`echo "AIRFLOW_DOMAIN=$(terraform -chdir=infra/prod output -raw airflow_domain)" >> .env`
(o metelo en SSM junto a los demás, §13.1). Reemplazá el bloque `airflow-apiserver` por este —
nota el `<<: *airflow-common-env` (el anchor **anidado** que definís junto con `x-airflow-common`,
§14.1): permite sumar las 3 claves de TLS sin repetir todo el resto del environment a mano:

```yaml
services:
  airflow-apiserver:
    <<: *airflow-common
    container_name: airflow-apiserver
    command: api-server
    environment:
      <<: *airflow-common-env
      # Fuerza que el tráfico INTERNO scheduler → api-server use el FQDN del cert (resuelve el
      # gotcha de Airflow 3: api-server, UI y Task Execution API comparten el puerto 8080, §5.6).
      AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "https://${AIRFLOW_DOMAIN}:8080/execution/"
      AIRFLOW__API__SSL_CERT: /opt/airflow/certs/fullchain.pem
      AIRFLOW__API__SSL_KEY:  /opt/airflow/certs/privkey.pem
      AIRFLOW__API__BASE_URL: "https://${AIRFLOW_DOMAIN}"    # links/redirects correctos
    ports:
      - "8082:8080"                                          # túnel local (seguís pudiendo usarlo)
      - "443:8080"                                            # HTTPS público; el SG lo limita a tu IP
    volumes:
      - ./dags:/opt/airflow/dags                              # el `<<:` no mergea `volumes`, hay que repetirlo
      - /data/certs/live/${AIRFLOW_DOMAIN}:/opt/airflow/certs:ro
    networks:
      hadoopnet:
        aliases: ["${AIRFLOW_DOMAIN}"]                       # <- adentro, el cert matchea este nombre
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }
```

> `<<:` en YAML mergea al nivel del mapping donde se usa: como acá reemplazás `environment`,
> `ports`, `volumes` y `networks` del servicio con tus propios valores, tenés que repetir lo que
> ya tenías en cada uno (por eso `./dags:/opt/airflow/dags` aparece de nuevo). Si `airflow_domain`
> está vacío, no toques nada: dejá el `airflow-apiserver` de §14.1 como está (8082 por túnel).

**Renovación automática (una vez, en la EC2).** `certbot renew` es no-op si faltan >30 días; corre
semanal y recarga el cert reiniciando el api-server:

```bash
echo '0 3 * * 1 root docker run --rm -v /data/certs:/etc/letsencrypt certbot/dns-route53 renew --quiet && chmod -R g+rX /data/certs && docker restart airflow-apiserver' \
  | sudo tee /etc/cron.d/airflow-cert-renew
```

**Verificar:**

```bash
docker compose -f docker-compose.prod.yml up -d   # con AIRFLOW_DOMAIN en el .env
curl -sSfI "https://$(terraform -chdir=infra/prod output -raw airflow_domain)/" | head -1  # 200/302 desde tu IP
# Desde OTRA IP debe cortar (timeout): el SG solo deja 443 a var.my_ip_cidr.
```

Entrás a `https://airflow.midominio.com` con el usuario **admin** y la password que generó SSM
(§13.1). La restricción por IP es defensa-en-profundidad **sobre** el login de Airflow, no un reemplazo de este.

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
# comprobá — desde la EC2, para probar el instance profile (no tus keys locales)
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'aws s3 cp /etc/hostname s3://pyspark-stack-datalake-<acct>/raw/smoke-iam.txt'
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
aws dlm get-lifecycle-policies --query 'Policies[].State'   # ["ENABLED"]
```

### 6.4 Cómputo Spark: EMR Serverless

Spark **salió de la EC2**. Los jobs corren en **EMR Serverless**: una aplicación Spark serverless
que arranca sola cuando llega un job, escala a cero cuando queda idle y **paga solo mientras
computa** (vCPU-seg + GB-seg). El *cold start* es ~1–2 min; a cambio, no hay cluster que mantener ni
instancia siempre encendida. Airflow (en la EC2) dispara cada job con `EmrServerlessStartJobOperator` y lo
pollea con `EmrServerlessJobSensor` (patrón de DAG en §9.0) — nunca corre `spark-submit` local.

**A) La aplicación EMR Serverless — `infra/prod/emr.tf`:**

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

**B) Rol de ejecución del job (least-privilege) — `infra/prod/emr.tf`:** EMR Serverless asume
**este** rol para correr el Spark; solo puede tocar los dos buckets y escribir sus logs. Sin Glue.

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

**C) Extensión del rol de la EC2 — `infra/prod/iam.tf` (junto al rol de la EC2):** permite que Airflow
(en la EC2) **envíe y consulte** jobs, y que **pase** el rol de ejecución a EMR Serverless. El `iam:PassRole`
con `iam:PassedToService` es la barrera: la EC2 puede pasar ese rol *solo* a EMR Serverless, a nada más.

```hcl
# infra/prod/iam.tf   (permisos EMR Serverless para el rol de la EC2)
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

**D) Empaquetado y submit.** Los entrypoints PySpark van en el repo bajo `spark-apps/emr/` (todavía
no existen: los creás en el punto **E**, más abajo — el `aws s3 sync` de acá falla hasta entonces)
(`spark-apps/emr/customer_etl.py`, `spark-apps/emr/wordcount.py`) y en S3 bajo `s3://<artifacts>/emr/`;
los logs del job van a `s3://<artifacts>/emr/logs/`. El CI/CD sincroniza `spark-apps/emr/` a `emr/` en
cada deploy (§11.3) — solo los entrypoints EMR, no el resto de `spark-apps/` (que es dev local).
Un `StartJobRun` (así lo arma por vos el operator de Airflow; equivalente CLI para probar a mano):

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

**E) Los entrypoints PySpark (copy-paste).** Estos son los dos entrypoints que menciona el punto
D), para crear en `spark-apps/emr/`. Son *self-contained*: no usan `.master()` (EMR Serverless inyecta master/recursos), leen y
escriben directo en S3 (`s3a://`), y la config de Spark viaja por-job en `sparkSubmitParameters`. Se
suben a `s3://<artifacts>/emr/` con el `aws s3 sync` de arriba (o el CI/CD de §11.3), y desde ahí los
lanza el `EmrServerlessStartJobOperator` de los DAGs (§10.2).

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

> Verificá: subilos con el `aws s3 sync spark-apps/emr/ s3://<artifacts>/emr/` de §11.3 (el CI/CD lo
> hace solo) o a mano con el `aws s3 sync` del bloque `comprobá` de arriba. Después, un `StartJobRun`
> (el del punto D, o el que arma el operator) los ejecuta desde S3.

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
        # run_id determinístico por archivo (auditoría §1.3): un reintento del MISMO objeto
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
# Mismo criterio que §5.4: retención acotada, no infinita (auditoría §1.2).
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
  # Techo de invocaciones concurrentes (auditoría §3.1 — "thundering herd"): sin esto, subir 50
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
  # dead_letter_config sigue cubriendo el camino CRON (invocación async de EventBridge Scheduler,
  # §7.2): 2 reintentos y después acá. El camino de eventos S3 (§7.3) NO usa esto — para invocaciones
  # disparadas por SQS, el redrive lo maneja la COLA (redrive_policy de aws_sqs_queue.trigger_events,
  # §7.3), no el dead_letter_config de la función. Misma DLQ final en los dos casos.
  dead_letter_config {
    target_arn = aws_sqs_queue.trigger_airflow_dlq.arn   # definida en §18.1
  }
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
# comprobá — el agente SSM Online es prerrequisito de toda la §7
ID=$(terraform -chdir=infra/prod output -raw instance_id)
aws ssm describe-instance-information --query "InstanceInformationList[?InstanceId=='$ID'].PingStatus"  # ["Online"]
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --cli-binary-format raw-in-base64-out --payload '{"dag":"customer_etl_emr"}' /dev/stdout
# en la EC2: dag_id posicional (en Airflow 3 no existe -d)
docker compose exec -T airflow-scheduler airflow dags list-runs customer_etl_emr
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
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.trigger_airflow_dlq.arn  # misma DLQ del cron (§18.1)
    maxReceiveCount      = 5   # ~5 x 360s ≈ 30 min de reintentos antes de darse por vencido
  })
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

> Verificá el retry: apagá la EC2 a mano (`aws ec2 stop-instances`), subí un archivo a `raw/`, y
> mirá `aws sqs get-queue-attributes --queue-url <url> --attribute-names ApproximateNumberOfMessages
> ApproximateNumberOfMessagesNotVisible` — el mensaje va a aparecer "not visible" (siendo procesado o
> esperando el próximo intento) hasta que la EC2 esté arriba y el DAG se dispare solo, sin que
> reinicies nada a mano.

---

## 8. Operación, seguridad y ahorro

Cuatro artefactos operativos independientes, para momentos distintos. **8.1** se corre una vez tras
cada `apply`, para confirmar que lo que creaste funciona. **8.2** es la referencia rápida del día a
día. **8.3** se repasa antes de exponer cualquier cosa. **8.4** se consulta cuando la factura sube.

### 8.1 Validación (smoke tests), capa por capa

De abajo hacia arriba: infra → red/host → stack → negocio → monitoreo. **Pará en la primera que
falle** — no tiene sentido probar un DAG si el agente SSM no está `Online`.

Antes de arrancar, tres cosas que valen para todas las capas:

- **Todo corre desde la raíz del repo**, salvo lo marcado `[EC2]`.
- **La capa 1 define `$ID`, `$IP` y `$ACCT`**, que usan las capas 2 a 4. Misma terminal, o volvé a
  definirlas. Si te saltás la capa 1, las rutas quedan como `s3://pyspark-stack-datalake-/raw/…` y
  el error no te va a decir por qué.
- **`[LOCAL]` usa TUS credenciales; `[EC2]` usa el instance profile.** La distinción no es cosmética:
  es la diferencia entre validar que el bucket existe y validar que el rol IAM funciona.

**Capa 0 — Pre-apply** `[LOCAL]`, antes de tocar AWS:

```bash
aws sts get-caller-identity                        # ¿la cuenta correcta?
terraform -chdir=infra/prod fmt -check -recursive  # formato canónico
terraform -chdir=infra/prod validate               # config válida
terraform -chdir=infra/prod plan                   # LEÉ el diff antes de aplicar
checkov -d infra/prod                              # opcional; ~18 hallazgos esperables (§11.2)
```

**Capa 1 — Infra AWS** `[LOCAL]`, después del `apply`. Acá se definen las variables:

```bash
ID=$(terraform -chdir=infra/prod output -raw instance_id)
IP=$(terraform -chdir=infra/prod output -raw public_ip)
ACCT=$(aws sts get-caller-identity --query Account --output text)

aws ec2 describe-instances --instance-ids "$ID" \
  --query 'Reservations[].Instances[].{estado:State.Name,imdsv2:MetadataOptions.HttpTokens}'
aws ssm describe-instance-information \
  --query "InstanceInformationList[?InstanceId=='$ID'].PingStatus"
aws s3 ls | grep pyspark-stack                            # buckets datalake + artifacts
aws scheduler list-schedules --query 'Schedules[].Name'   # start / stop / daily-etl
aws dlm get-lifecycle-policies --query 'Policies[].State' # ENABLED (backups)
```

Esperás: estado `running` + IMDSv2 `required`, PingStatus **`Online`**, los dos buckets, tres
schedules, DLM `ENABLED`. **Si PingStatus no es `Online`, frená acá**: sin agente SSM no funciona
ni el trigger de DAGs (§7) ni el apagado job-aware (§10.3). Suele ser el rol de la EC2 sin
`AmazonSSMManagedInstanceCore` (§5.2), o el user_data que no terminó (§5.5, paso 4). Si el DLM
devuelve `[]`, todavía no aplicaste §6.3.

**Capa 2 — Red y superficie de ataque** `[LOCAL]`:

```bash
nc -zv "$IP" 22 && echo "SSH ok"
curl --max-time 5 "http://$IP:8082" && echo "MAL: Airflow HTTP expuesto" || echo "OK: 8082 cerrado"

# Si exponés la web (§5.6): 443 debe abrir SOLO desde tu IP.
D=$(terraform -chdir=infra/prod output -raw airflow_domain 2>/dev/null)
[ -n "$D" ] && { curl -sSfI --max-time 5 "https://$D/" >/dev/null \
  && echo "OK: 443 abre desde tu IP" || echo "443 no responde (¿otra IP o cert pendiente?)"; }

ssh -i ~/.ssh/pyspark_stack ec2-user@"$IP" 'cd pyspark_stack && docker compose ps'
```

El `curl` a 8082 **tiene que fallar**: si responde, tenés Airflow abierto a internet. Y la prueba
real del 443 es desde **otra red** (datos del celular): debe cortar por timeout.

**Capa 3 — Stack funcional.** Sin túnel: todo va por `ssh` con comando, o por la API de AWS.

```bash
S="ssh -i ~/.ssh/pyspark_stack ec2-user@$IP"

# [EC2] DAGs sin errores de import — vacío = OK
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags list-import-errors'

# [LOCAL] Spark ya NO corre en la EC2: se valida la app EMR Serverless
aws emr-serverless list-applications \
  --query 'applications[?name==`pyspark-stack-spark`].[id,state]'   # STARTED/STOPPED/CREATED = OK

# [LOCAL] valida que el BUCKET existe (usa tus creds, NO prueba el rol de la EC2)
aws s3 cp README.md "s3://pyspark-stack-datalake-$ACCT/_smoke/smoke.txt"

# [EC2] valida el ROL IAM de la instancia (instance profile, sin access keys)
$S 'aws s3 cp /etc/hostname "s3://pyspark-stack-datalake-'"$ACCT"'/_smoke/smoke-iam.txt"'
```

> **Por qué `_smoke/` y no `raw/`:** si ya aplicaste §7.3, todo objeto que caiga en `raw/` dispara
> la Lambda y un DAG run. Escribir los smoke tests ahí ensucia el data lake, gasta corridas de EMR
> y, sobre todo, arruina la prueba event-driven de la capa 4: sus disparos ya no se distinguen de este
> ruido. Al terminar: `aws s3 rm "s3://pyspark-stack-datalake-$ACCT/_smoke/" --recursive`.

El rol de ejecución de EMR (el `s3a://` desde el job Spark) no se valida acá: necesita un
`StartJobRun` real, en §6.4 y en el runbook de §15.

**Capa 4 — Negocio end-to-end** (orquestación):

```bash
# [EC2] el compose base no setea DAGS_ARE_PAUSED_AT_CREATION: nacen pausados y el trigger
# quedaría en queued para siempre.
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags unpause customer_etl_emr'

# [LOCAL] disparo manual — mismo camino que usan EventBridge y S3
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --cli-binary-format raw-in-base64-out \
  --payload '{"dag":"customer_etl_emr"}' /dev/stdout
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags list-runs customer_etl_emr'

# [LOCAL] event-driven: un archivo nuevo en raw/ debe disparar el DAG solo
echo "smoke" > /tmp/datos.csv
aws s3 cp /tmp/datos.csv "s3://pyspark-stack-datalake-$ACCT/raw/"
aws logs tail /aws/lambda/pyspark-stack-trigger-airflow --since 5m   # ¿llegó S3 → Lambda?
```

`aws lambda invoke` devuelve 200 y un `commandId` **aunque el `airflow dags trigger` de adentro
haya fallado** (SSM SendCommand es fire-and-forget). Para ver qué pasó de verdad:
`aws ssm get-command-invocation --command-id <id> --instance-id "$ID"`.

Dos limitaciones conocidas de esta capa: `customer_etl_emr` **ignora** el `conf {bucket,key}` y procesa
por `{{ ds }}`, así que esto valida el plumbing S3→Lambda→SSM, no que se procese *ese* archivo (DAG
parametrizado: §9.0). Y la notificación es **por objeto**: subir N archivos a `raw/` dispara N
Lambdas y N DAG runs idénticos, cada uno con su cold start de EMR. Acotá con `filter_suffix` (un
centinela `_SUCCESS`) o `max_active_runs=1` en el DAG.

**Capa 5 — Monitoreo.** Recién aplica cuando exista el monitoreo (§12–§14); en la primera pasada,
saltala. Necesita **su propio túnel**, con puertos que `tunnel_command` no reenvía:

```bash
# [LOCAL] terminal aparte, y dejala abierta:
ssh -i ~/.ssh/pyspark_stack -L 9090:localhost:9090 -L 3000:localhost:3000 \
    -L 9093:localhost:9093 -L 3100:localhost:3100 ec2-user@"$IP"
```

```bash
# [LOCAL] en otra terminal, con el túnel de arriba abierto:
curl -sf localhost:9090/-/healthy && echo "Prometheus OK"
curl -s  localhost:9090/api/v1/targets | grep -o '"health":"[a-z]*"' | sort | uniq -c  # todos "up"
curl -sf localhost:3000/api/health   && echo "Grafana OK"
curl -sf localhost:9093/-/healthy    && echo "Alertmanager OK"
curl -sf localhost:3100/ready        && echo "Loki OK"
```

Si un target aparece `down`, el sospechoso habitual es Promtail (§12.7): monta
`/var/run/docker.sock` en `:ro` y falla por permisos sin decir nada. Diagnosticalo con
`$S 'cd pyspark_stack && docker compose logs promtail | tail -30'`.

> Estos smoke tests son la base de un check de CI post-deploy: el workflow de Deploy (§11) puede
> correr las capas 1-2 tras cada push para confirmar que la EC2 quedó sana.

### 8.2 Cheat-sheet de operación

```bash
# La Lambda startstop prende/apaga la EC2 sola por cron. Manual:
aws ec2 stop-instances --instance-ids $(terraform -chdir=infra/prod output -raw instance_id)
aws lambda invoke --function-name pyspark-stack-startstop \
  --cli-binary-format raw-in-base64-out --payload '{"action":"start"}' /dev/stdout

# Disparar un DAG a mano (mismo camino que usa EventBridge/S3):
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --cli-binary-format raw-in-base64-out --payload '{"dag":"customer_etl_emr"}' /dev/stdout

# Teardown total. NO sale de una: hay tres guardas que lo abortan, a propósito.
# Procedimiento completo (qué comentar, en qué orden, y qué hacer si igual se traba) en §8.5 —
# esto es solo el resumen de una línea de cada guarda:
#  1) prevent_destroy en el bucket de tfstate (§4) y en el volumen /data (§5.3): el destroy
#     ABORTA entero, no saltea el recurso. Comentá esas líneas del .tf primero (§8.5).
#  2) El bucket del data lake tiene versionado y no tiene force_destroy: falla con BucketNotEmpty
#     aunque lo vacíes, porque quedan versiones y delete markers. Agregá force_destroy = true (§8.5).
#  3) Destruí infra/prod ANTES que infra/bootstrap: el bucket del bootstrap guarda el state de
#     prod. Al revés te quedás sin state y con recursos huérfanos facturando.
terraform -chdir=infra/prod destroy
```

### 8.3 Seguridad (checklist)

- [ ] Buckets con `public_access_block`, cifrado y política solo-TLS (ya en el TF del data lake).
- [ ] SG de EC2: puerto 22 (y 443 si exponés la web, §5.6) **solo** desde tu IP; Grafana/Prometheus/
      Loki por túnel; los triggers automáticos van por SSM, no por la web (§7).
- [ ] IAM least-privilege: `startstop` **solo** actúa sobre instancias con el tag; `trigger-airflow`
      **solo** puede `ssm:SendCommand` sobre esta instancia (condiciones ya puestas).
- [ ] EMR Serverless con **su propio** rol de ejecución scopeado a los buckets; la EC2 solo puede
      `StartJobRun` + `iam:PassRole` (con `iam:PassedToService`) sobre ese rol (§6.4).
- [ ] Spark (EMR) y Airflow usan `s3a://`/`s3://` con rol IAM, sin access keys en disco.
- [ ] Tráfico EC2↔S3 por el **S3 VPC Gateway Endpoint** (§6.5), sin salir a internet. (EMR
      Serverless no aplica: corre sin `network_configuration`, fuera de tu VPC — ver §6.5.)
- [ ] Secretos en **SSM Parameter Store / Secrets Manager**, no en texto plano.
- [ ] `.env`, `terraform.tfvars`, `*.zip` de Lambdas y `alertmanager.yml` en `.gitignore`.

### 8.4 Palancas de ahorro (orden de impacto)

1. **EMR Serverless (escala a cero)** → Spark paga solo mientras corre el job (~$9/mes a este
   volumen), sin caja siempre encendida. Es la mayor palanca del rediseño híbrido.
2. **Auto start/stop de la EC2** → la `t3.large` pasa de ~$60 a ~$12/mes. Su impacto relativo es
   menor que antes (la EC2 ya es chica), pero sigue contando.
3. **S3 lifecycle a IA/Glacier** (ya aplicado): baja el costo de almacenamiento del lake.
4. **Snapshots DLM con retención acotada** (7 días): backups sin acumular costo.

### 8.5 Teardown limpio: `terraform destroy` que sí termina

El comentario de §8.2 ya adelantaba el problema: `terraform destroy` **no sale de una** contra este
stack, y es a propósito — dos protecciones (§4 y §5.3) existen específicamente para que un
`destroy` accidental no te borre el volumen de Postgres o el bucket con el state. Mientras estás
**iterando** (probando, todavía sin datos reales que te importe conservar) esas protecciones son un
estorbo, no una ayuda. Esta sección es el procedimiento completo — qué tocar, en qué orden, y qué
hacer si igual se traba — para poder tirar todo y volver a `apply` sin miedo, tantas veces como
haga falta, hasta que decidas que esto ya es producción de verdad.

**El principio: las protecciones son líneas de texto, no un flag.** `prevent_destroy` no admite
variables (es una limitación real de Terraform, a propósito: si aceptara `var.algo`, un
`-var prevent_destroy=false` tipeado en la terminal equivocada volaría el volumen de Postgres real
sin que el `.tf` lo delatara). Así que el interruptor **es** editar el archivo: comentás las líneas
mientras iterás, las volvés a poner cuando ya haya algo real corriendo ahí adentro.

**Mientras iterás — comentá esto (y quitalo del `apply` corriendo `terraform apply` de nuevo):**

```hcl
# infra/bootstrap/main.tf — bucket del state (§4)
resource "aws_s3_bucket" "tfstate" {
  bucket = local.state_bucket
  # lifecycle { prevent_destroy = true }   # ← comentado mientras iterás; sin esto, destroy no aborta acá
}
```

```hcl
# infra/prod/ec2.tf — volumen de datos (§5.3)
resource "aws_ebs_volume" "data" {
  availability_zone = var.availability_zone
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "${var.name_prefix}-data" }
  # lifecycle { prevent_destroy = true }   # ← comentado mientras iterás
}
```

```hcl
# infra/prod/s3.tf — los DOS buckets (§6.1), no solo el data lake
resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake
  # lifecycle { prevent_destroy = true }   # ← comentado mientras iterás

  # force_destroy SÍ es un argumento normal del resource (no vive en `lifecycle{}`, así que esto
  # no tiene la limitación de arriba): con el versionado activo (aws_s3_bucket_versioning.all,
  # §6.1, que aplica a datalake Y artifacts con un for_each), un bucket con una sola versión de un
  # objeto ya cuenta como "no vacío" para el destroy normal. force_destroy le dice a AWS que borre
  # objetos + versiones + delete markers él solo al destruir el bucket.
  force_destroy = true   # ← agregalo mientras iterás; quitalo (o volvé a false) antes de producción real
}
resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifacts
  # También versionado por el mismo aws_s3_bucket_versioning.all de arriba — sin este
  # force_destroy, el destroy falla acá TAMBIÉN, no solo en datalake (es el error que corregí
  # después de escribir esta sección la primera vez: había asumido que artifacts no lo necesitaba).
  force_destroy = true   # ← agregalo mientras iterás; quitalo (o volvé a false) antes de producción real
}
```

**Antes de dar por buena esta arquitectura para producción real** (la primera vez que corra algo
con datos que te importan): sacá el comentario de los dos `prevent_destroy`, sacá o volvé `false`
los dos `force_destroy` (datalake y artifacts), y `terraform apply` — no destruye nada, solo
actualiza esas cuatro líneas de protección. A partir de ahí, un `destroy` accidental vuelve a
abortar en vez de borrar.

**El orden importa — bootstrap se destruye AL FINAL, no antes:**

```bash
# 1) TODO lo de infra/prod primero — bootstrap todavía tiene que existir para guardar/leer el state
terraform -chdir=infra/prod destroy

# 2) Recién ahora, si además querés tirar el backend del state:
terraform -chdir=infra/bootstrap destroy
```

> Al revés (`bootstrap` primero) te quedás sin bucket de state mientras `infra/prod` todavía existe
> en AWS: Terraform pierde de dónde leer/escribir el estado y esos recursos quedan huérfanos,
> facturando, sin que `destroy` los pueda encontrar más.

**Si igual se traba — recuperación por capas, de menor a mayor intervención (la técnica que usaría
alguien que hace esto seguido, no la fuerza bruta de borrar todo a mano por consola):**

```bash
# a) Dry-run: qué va a intentar borrar y en qué orden, ANTES de que falle a mitad de camino
terraform -chdir=infra/prod plan -destroy

# b) -refresh=false: salta comparar el state contra AWS antes de destruir. Sirve cuando algo ya se
#    borró POR FUERA de Terraform (a mano, investigando un problema) y el refresh normal tira error
#    al no encontrarlo — bloqueando el plan de destroy ANTES de que arranque, por un recurso que ya
#    ni existe.
terraform -chdir=infra/prod destroy -refresh=false -auto-approve

# c) -target: aislá el recurso que traba y destruilo solo, para ver el error real sin ruido de los
#    demás. Después corré el destroy normal para el resto.
terraform -chdir=infra/prod destroy -target=aws_s3_bucket.datalake -auto-approve
terraform -chdir=infra/prod destroy -auto-approve

# d) state rm: el recurso YA NO EXISTE en AWS (lo borraste vos a mano en el paso anterior, o nunca
#    se llegó a crear del todo) pero Terraform sigue pensando que lo administra y se queja al no
#    encontrarlo. Esto lo saca del state SIN tocar AWS — Terraform simplemente deja de "verlo".
terraform -chdir=infra/prod state list                    # ¿qué sigue trackeado?
terraform -chdir=infra/prod state rm aws_sqs_queue.trigger_events
```

**Los tres bloqueos reales de ESTE stack, y por qué ya deberían estar resueltos con lo de arriba:**

| Bloqueo | Causa | Ya cubierto por |
|---|---|---|
| `Instance cannot be destroyed` / plan aborta entero | `prevent_destroy` en `aws_ebs_volume.data` o `aws_s3_bucket.tfstate` | Comentar las líneas (arriba) mientras iterás |
| `BucketNotEmpty` en el datalake **o** en artifacts | `aws_s3_bucket_versioning.all` (§6.1) aplica a los DOS buckets con un `for_each` — un bucket con una sola versión ya cuenta como no-vacío en cualquiera de los dos | `force_destroy = true` en **ambos** buckets (arriba) — ninguno de los dos tiene `prevent_destroy`, así que no hay lifecycle que comentar acá, solo agregar force_destroy |
| State inaccesible / recursos huérfanos facturando | Se destruyó `infra/bootstrap` antes que `infra/prod` | Orden correcto (arriba): prod primero, bootstrap al final |

**Lo que Terraform ya resuelve solo, sin que tengas que hacer nada — para que no los busques por
las dudas:** el `aws_glue_catalog_database` (§16.2) borra sus tablas en cascada al destruirse (es
comportamiento nativo de la API de Glue, no un flag de Terraform); la `aws_eip_association` y el
`aws_lambda_event_source_mapping` (§7.3) se destruyen antes que el recurso del que dependen porque
el grafo de dependencias de Terraform ya conoce ese orden. Donde sí hace falta un paso manual: un
job de EMR Serverless **activo** al momento del destroy puede trabar el borrado de la aplicación —
cancelalo antes con `aws emr-serverless cancel-job-run --application-id <id> --job-run-id <id>`.

**Lo que Terraform NO borra — no bloquea el destroy, pero queda facturando de más si lo olvidás:**
- **Snapshots del EBS `/data`** (`aws_dlm_lifecycle_policy.data`, §6.3): DLM los crea **fuera** del
  grafo de Terraform (Terraform administra la *policy* que los genera, no los snapshots en sí). Al
  destruir el volumen, los snapshots que ya sacó siguen ahí, cobrando (poco, pero indefinidamente).
  Borralos a mano si no los querés: `aws ec2 describe-snapshots --owner-ids self
  --filters "Name=tag:Name,Values=${var.name_prefix}-data" --query 'Snapshots[].SnapshotId' --output text
  | xargs -n1 aws ec2 delete-snapshot --snapshot-id`.
- **Certificado de Let's Encrypt** (§5.6, si lo activaste): vive en `/data/certs`, dentro del EBS —
  se va con el volumen, no es un recurso de Terraform aparte. Nada que limpiar por separado.

**Verificación — que no quede nada facturando:**

```bash
terraform -chdir=infra/prod state list   # vacío = destroy completo
aws ec2 describe-instances --filters "Name=tag:Name,Values=pyspark-stack-node" \
  --query 'Reservations[].Instances[].State.Name'   # sin resultados, o "terminated"
aws s3 ls | grep pyspark-stack                       # ningún bucket (o solo tfstate, si no lo tiraste)
aws emr-serverless list-applications --query 'applications[?name==`pyspark-stack-spark`]'
```

> 🖱️ **Si venís del camino de consola** (`docs/02b`, sin Terraform): no hay `prevent_destroy` que
> comentar — el equivalente es borrar cada recurso a mano, en el orden correcto. Ese checklist
> completo (14 pasos, orden inverso al de creación) ya está en
> [`docs/02b` §15.3](02b-produccion-aws-consola.md#153-teardown-manual-borrar-todo-en-orden-inverso).

> Si al volver a `apply` después de un destroy limpio te aparece `ResourceAlreadyExistsException` en
> uno de los `aws_cloudwatch_log_group` de las Lambdas (§5.4/§7.1): algo invocó esa Lambda entre el
> destroy y el apply (Lambda recrea su log group solo, sin retención, en la primera invocación). No
> es un bloqueo del destroy en sí — se resuelve borrando ese log group a mano
> (`aws logs delete-log-group --log-group-name /aws/lambda/pyspark-stack-trigger-airflow`) y
> reaplicando.

### Resumen: qué corre y dónde

| Subsistema | Dónde corre | Storage durable |
|---|---|---|
| Spark (jobs ETL) | **EMR Serverless** (serverless, escala a cero) | lee/escribe `s3a://` con su rol de ejecución |
| Airflow (5 svcs) + Postgres | contenedores en EC2 `t3.large` | Postgres en `/data` (EBS) + snapshots |
| Monitoreo (Prometheus/Grafana/Alertmanager/Loki) | contenedores en EC2 | Prometheus/Loki en `/data` (EBS) + snapshots |
| Disparo de DAGs | Lambda trigger-airflow (SSM) + EventBridge | — |
| Encendido | EC2 con auto start/stop | — |

---

## 9. Notebooks: dónde viven y cómo se ejecutan

Dos usos distintos del mismo `.ipynb`:

| Uso | Cómo | Dónde |
|---|---|---|
| **Explorar / desarrollar** | Interactivo en JupyterLab, **en el stack local** (`docs/01`) — no hay Jupyter en prod | `./notebooks` (montado en `/opt/notebooks`) |
| **Ejecutar programado** (parte de un pipeline) | **papermill** disparado por un DAG, en la EC2 de prod | el mismo `./notebooks`, output a `./notebooks/notebook-output` (§9.3) |

Regla: los notebooks se guardan en `./notebooks` (versionados en git). Para ejecutarlos de forma
automática se usa papermill, que inyecta parámetros y corre el notebook de punta a punta desde un
DAG de Airflow.

### 9.0 Patrones de tarea para ETL batch (¿PySpark o Python puro?)

Airflow es solo el orquestador: cada task elige su motor. La regla para no sobre-usar Spark:

| Tarea | Motor | Operador | Cuándo |
|---|---|---|---|
| Datos chicos (<~1 GB), transform simple, llamar una API, mover/validar archivos, gatillar dbt | **Python puro** (pandas/duckdb) | `@task` / `PythonOperator` | La mayoría de los pasos. No arranques un job Spark para 50 MB. |
| Datos medianos/grandes, joins/`groupBy` pesados, muchos archivos, paralelismo | **PySpark** en **EMR Serverless** | `EmrServerlessStartJobOperator` + `EmrServerlessJobSensor` | Cuando el dato no entra cómodo en una máquina o el *shuffle* es grande. |
| Análisis/reporte reproducible con evidencia | Notebook | `PapermillOperator` | Ver 9.1-9.3. |

> Aviso: los DAGs de esta sección son **patrones** ilustrativos — no los copies a `dags/` tal cual.
> Referencian scripts (`ventas/enriquecer.py`), datos y `<acct>` que tenés que crear/reemplazar;
> si los copiás incompletos, ensucian `airflow dags list-import-errors` (el check de §8).

**Python puro — el caso "no necesito Spark"** (`requirements.txt`: `pandas`, `s3fs`, `pyarrow`):

```python
# dags/small_etl_dag.py — sin Spark: pandas lee de S3 y escribe curated
from datetime import datetime
import pandas as pd
from airflow.sdk import DAG, task        # Airflow 3: DAG y TaskFlow @task en airflow.sdk

with DAG("small_etl", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False) as dag:
    @task
    def transform(ds=None):   # Airflow inyecta ds del context; un default "{{ ds }}" NO pasa por el templating
        base = "s3://pyspark-stack-datalake-<acct>"
        df = pd.read_csv(f"{base}/raw/ventas.csv")            # s3fs + rol IAM (sin keys)
        out = df[df["monto"] > 0].groupby("pais")["monto"].sum().reset_index()
        out.to_parquet(f"{base}/curated/ventas_por_pais/{ds}.parquet")
    transform()
```

**PySpark — el caso "sí necesito Spark"** (ahora en **EMR Serverless**, no en la EC2). El DAG
**dispara** el job con `EmrServerlessStartJobOperator` y **espera** su resultado con
`EmrServerlessJobSensor`; ambos vienen en el provider `apache-airflow-providers-amazon`, que hay que
**agregar** a `requirements.txt` (§9.1) para que los DAGs EMR parseen. El código PySpark se sube a
`s3://<artifacts>/emr/` (§6.4/§11.3):

```python
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor

run = EmrServerlessStartJobOperator(
    task_id="customer_etl",
    application_id="{{ var.value.emr_app_id }}",          # output emr_app_id (§6.4), como Airflow Variable
    execution_role_arn="{{ var.value.emr_job_role_arn }}", # output emr_job_role_arn (§6.4)
    job_driver={"sparkSubmit": {
        "entryPoint": f"s3://{ARTIFACTS}/emr/customer_etl.py",
        "entryPointArguments": [DATALAKE],
        "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g",
    }},
    configuration_overrides={"monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://{ARTIFACTS}/emr/logs/"}
    }},
)
```

Este operador reemplaza al `BashOperator`/`spark-submit` local de los DAGs actuales: **la EC2 ya no corre Spark**,
solo orquesta. El job lee/escribe `s3://` con el rol de ejecución de EMR; la config de Spark viaja
**por-job** en `sparkSubmitParameters` (no hay `spark-defaults.conf` local que montar). `application_id`
y `execution_role_arn` salen de los outputs de Terraform (§6.4), cargados como Airflow Variables (por
env `AIRFLOW_VAR_EMR_APP_ID` / `AIRFLOW_VAR_EMR_JOB_ROLE_ARN` en `docker-compose.prod.yml`, §14.1). El operador
puede además hacer la espera por sí mismo (`wait_for_completion=True`); si preferís separar disparo y espera, encadená
un `EmrServerlessJobSensor` sobre `application_id` + `job_run_id` del `run`.

**Cómo se une el almacenamiento (sin HDFS):**
- **Fuente y destino durable = S3.** Capas: `raw/` (crudo, tal cual llega) → `curated/` (limpio,
  tabla **Iceberg**, §16.1) → `analytics/` (agregados para consumo/BI, también Iceberg). Sobrevive
  al apagado de la EC2. El job Spark de EMR usa `s3a://`, las tasks Python puro usan `s3://` — ambos
  con rol IAM, sin keys. Solo `raw/` sigue siendo archivos sueltos (CSV/JSON tal como llegan): recién
  en `curated/` empieza el formato de tabla.
- **Scratch de Spark = disco efímero de los workers de EMR Serverless.** El *shuffle* y el *spill*
  van al almacenamiento local que EMR aprovisiona y libera con cada job. No lo configurás.
- **Sin HDFS en prod.** Si un pipeline multi-paso necesita materializar un intermedio compartido
  entre jobs, escribilo a `s3://…/staging/` (o encadenalo en un solo job). Ya no hay namenode/datanode.
- **Patrón:** leé de `s3a://…/raw/` → EMR Serverless procesa → escribí a `s3a://…/curated/`. Los
  DAGs de ejemplo que leen `hdfs://` son solo para el stack local (`docs/01`); en producción las
  rutas apuntan a `s3a://`/`s3://`.

**Pipeline unificado — Python puro (EC2) + PySpark (EMR Serverless) + S3, todo junto:**

```python
# dags/ventas_diario_dag.py
from datetime import datetime
import pandas as pd
from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

S3        = "s3://pyspark-stack-datalake-<acct>"          # pandas (Python puro, en la EC2)
S3A       = "s3a://pyspark-stack-datalake-<acct>"         # Spark (en EMR Serverless)
ARTIFACTS = "pyspark-stack-artifacts-<acct>"              # entrypoints en emr/, logs en emr/logs/

with DAG("ventas_diario", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False) as dag:

    @task  # 1) PYTHON PURO (EC2): ingesta/validación liviana → aterriza en S3 raw
    def ingesta(ds=None):
        df = pd.read_csv("/opt/spark-apps/landing/ventas.csv")
        assert not df.empty, "archivo vacío"
        df.to_csv(f"{S3}/raw/ventas/{ds}.csv", index=False)

    # 2) PYSPARK (EMR Serverless): join/limpieza pesada  S3 raw → S3 staging (intermedio)
    enriquecer = EmrServerlessStartJobOperator(
        task_id="enriquecer",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_job_role_arn }}",
        job_driver={"sparkSubmit": {
            "entryPoint": f"s3://{ARTIFACTS}/emr/enriquecer.py",
            "entryPointArguments": [S3A + "/raw/ventas/{{ ds }}.csv", S3A + "/staging/ventas/{{ ds }}"],
            "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g",
        }},
        configuration_overrides={"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": f"s3://{ARTIFACTS}/emr/logs/"}}},
    )

    # 3) PYSPARK (EMR Serverless): agrega desde S3 staging → S3 curated (durable)
    agregar = EmrServerlessStartJobOperator(
        task_id="agregar",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_job_role_arn }}",
        job_driver={"sparkSubmit": {
            "entryPoint": f"s3://{ARTIFACTS}/emr/agregar.py",
            "entryPointArguments": [S3A + "/staging/ventas/{{ ds }}", S3A + "/curated/ventas_por_pais/{{ ds }}"],
            "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g",
        }},
        configuration_overrides={"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": f"s3://{ARTIFACTS}/emr/logs/"}}},
    )

    @task  # 4) PYTHON PURO (EC2): post-proceso liviano (resumen / publicación)
    def publicar(ds=None):
        df = pd.read_parquet(f"{S3}/curated/ventas_por_pais/{ds}")
        print("filas curadas:", len(df))

    ingesta() >> enriquecer >> agregar >> publicar()
```

Qué usa cada pieza, y por qué corre en producción:

| Paso | Motor | Storage | Rol |
|---|---|---|---|
| 1 ingesta | **Python puro** (`@task`, EC2) | escribe **S3 `raw/`** | I/O liviano, no amerita Spark |
| 2 enriquecer | **PySpark** (EMR Serverless) | lee S3 `raw/` → escribe **S3 `staging/`** | transform pesado; deja intermedio en S3 |
| 3 agregar | **PySpark** (EMR Serverless) | lee **S3 `staging/`** → escribe **S3 `curated/`** | reusa el intermedio del paso anterior |
| 4 publicar | **Python puro** (`@task`, EC2) | lee **S3 `curated/`** | resumen/notificación liviana |

Así Airflow (en la EC2) orquesta, EMR Serverless hace lo pesado y escala a cero al terminar, S3
guarda todo lo durable (incluido el intermedio `staging/`) y Python puro cubre lo liviano — cada
componente tiene un trabajo real y solo pagás Spark mientras el job corre.

### 9.1 Instalar los providers de Airflow (amazon, papermill) y sus dependencias

Agregá los providers a `requirements.txt` (se instala en la imagen de Airflow), junto con las
dependencias de las tasks Python puro de §9.0 — sin ellas, DAGs como `ventas_diario` fallan en
la imagen actual. La línea **clave** es `apache-airflow-providers-amazon==9.29.0` (pin del
constraints de Airflow 3.2.2 / py3.12): sin ella, los DAGs EMR (`EmrServerlessStartJobOperator`,
§10.2) no parsean y su import protegido cae al `except`. Agregá a `requirements.txt`:

```text
# El extra [aiobotocore] NO es opcional: los DAGs de §10.2 usan deferrable=True y los hooks async
# del provider importan aiobotocore, que NO viene en las deps base (conflicto de botocore con
# boto3, el propio provider te lo deja a cargo). El import es lazy: los DAGs parsean bien, el CI
# pasa en verde, y recién revienta con ModuleNotFoundError en el triggerer al diferir la task.
apache-airflow-providers-amazon[aiobotocore]==9.29.0   # operadores EMR Serverless (§9.0/§10.2)
apache-airflow-providers-papermill==3.9.1 # notebooks (§9.3)
pandas    # tasks Python puro (§9.0)
s3fs      # pandas ↔ s3:// con el rol IAM
pyarrow   # parquet para pandas
```

Subí el `requirements.txt` a la EC2 (con el rsync completo de §5.5 — `deploy.sh` no lo sincroniza)
y rebuildeá la imagen: `docker compose -f docker-compose.prod.yml up -d --build`. Comprobá que quedó:
`docker compose exec -T airflow-scheduler airflow providers list | grep -E 'papermill|amazon'`
(el provider `apache-airflow-providers-amazon` trae los operadores EMR Serverless de §9.0).

Y montá `./notebooks` en el contenedor donde corren las tasks (con LocalExecutor es el
`scheduler`). Agregá el `volumes:` al servicio `airflow-scheduler` de tu `docker-compose.prod.yml`
(el que creaste en el Paso 0, §5.5 — todavía no tiene este mount):

```yaml
  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks   # papermill lee los .ipynb desde aquí (el `<<:` no mergea volumes)
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }
```

### 9.2 Parametrizar el notebook

En Jupyter, marcá **una celda con el tag `parameters`** (View → Cell Toolbar → Tags) con las
variables que querés inyectar:

```python
# celda tagueada 'parameters'
run_date = "2026-01-01"
```

### 9.3 DAG que ejecuta el notebook — `dags/run_notebook_dag.py`

Prerrequisitos: creá `notebooks/analysis.ipynb` con una celda tagueada `parameters` (§9.2) y
`mkdir -p notebooks/notebook-output` (papermill no crea el directorio de salida; al vivir dentro
de `./notebooks`, entrada y salida usan el único mount que el scheduler de prod tiene, §9.1/§14.1).

```python
from datetime import datetime, timedelta

from airflow.sdk import DAG   # Airflow 3: Task SDK (misma convención que el resto de los DAGs)

# Import PROTEGIDO (mismo patrón que los DAGs EMR de §10.2): el CI (§11.2) no instala el
# provider papermill — sin el try/except, este archivo rompería el DagBag y el gate dag-validate.
try:
    from airflow.providers.papermill.operators.papermill import PapermillOperator
except ImportError:
    PapermillOperator = None  # entorno sin el provider (CI, dev local sin rebuild de §9.1)

if PapermillOperator is not None:
    with DAG(
        dag_id="run_notebook_example",
        default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)},
        schedule=None,  # manual o disparado por otro DAG/Lambda
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["notebook", "papermill"],
    ) as dag:
        PapermillOperator(
            task_id="run_analysis_notebook",
            input_nb="/opt/notebooks/analysis.ipynb",
            output_nb="/opt/notebooks/notebook-output/analysis_{{ ds }}.ipynb",
            parameters={"run_date": "{{ ds }}"},  # inyecta la fecha de ejecución
        )
```

papermill guarda una **copia ejecutada con outputs** en `notebooks/notebook-output/` (queda como
evidencia/auditoría de cada corrida). Comprobalo con un run manual del DAG y
`ls notebooks/notebook-output/`.

---

## 10. Flujo local → servidor → los DAGs corren solos

El objetivo: editás local, hacés `git push`, y los DAGs aparecen y corren en la EC2 sin pasos
manuales.

```text
laptop (edita dags/, spark-apps/, notebooks/)
   │ git push a main
   ▼
GitHub Actions (CI valida → Deploy)
   │ aws s3 sync  →  s3://artifacts/deploy/dags/  +  spark-apps/emr/ → s3://artifacts/emr/ (entrypoints)
   ▼
SSM SendCommand  →  EC2: aws s3 sync s3://artifacts/deploy/dags/ → ./dags  (+ airflow dags reserialize)
   ▼
Airflow dag-processor detecta los DAGs nuevos (~30s con el refresh de docker-compose.prod.yml)
   │  (no quedan en pausa)  →  corren por su schedule
   ▼
Airflow dispara EMR Serverless (StartJobRun) · el job escribe a s3a://datalake/curated
```

Por qué "corren solos": el `dag-processor` de Airflow 3 re-escanea `./dags` periódicamente según
`AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL` (para forzar al instante:
`docker exec airflow-dag-processor airflow dags reserialize`), así que un archivo nuevo aparece
sin reiniciar nada. Además, `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` en `False` evita que los DAGs queden en
pausa al aparecer, así que arrancan según su `schedule`. Ambas variables van en el env de los
servicios de Airflow:

Agregalas al `environment:` de `x-airflow-common` en tu `docker-compose.prod.yml` (así las heredan
los 5 servicios `airflow-*` vía `<<: *airflow-common-env`):

```yaml
# docker-compose.prod.yml — dentro de x-airflow-common → environment → &airflow-common-env
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "False"
# El default de detección de archivos nuevos es 5 min; con 30s el deploy se ve al toque:
AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: "30"
```

Importante: ambos comportamientos dependen de que agregues estas dos líneas (ya vienen incluidas en
la versión completa de §14.1). Sin ellas, Airflow usa sus defaults: refresh de **5 min** y los DAGs
nuevos nacen **pausados** (`DAGS_ARE_PAUSED_AT_CREATION` default `True`).

> Para forzar una corrida inmediata (además del schedule), el paso de deploy puede terminar con
> un `airflow dags trigger <dag>` vía SSM, o usar la Lambda `trigger-airflow` de la sección de
> orquestación.

### 10.1 Modo rápido (dev): deploy manual sin esperar CI

Para iterar rápido mientras desarrollás, usá este script, que sube los archivos directo por `rsync`:

```bash
#!/usr/bin/env bash
# scripts/deploy.sh — deploy rápido a la EC2 (dev). Uso: ./scripts/deploy.sh
set -euo pipefail
IP=$(terraform -chdir=infra/prod output -raw public_ip)
KEY=~/.ssh/pyspark_stack

# Orígenes SIN barra final: con "dags/ ..." rsync volcaría el CONTENIDO mezclado en
# pyspark_stack/ y el --delete borraría el resto del proyecto en la EC2 (compose, .env, monitoring/).
rsync -avz --delete \
  --exclude '__pycache__' \
  -e "ssh -i $KEY" \
  dags spark-apps notebooks \
  ec2-user@"$IP":/home/ec2-user/pyspark_stack/

echo "deploy hecho — Airflow detecta los DAGs en ~30s (refresh de docker-compose.prod.yml, §10)"
```

```bash
chmod +x scripts/deploy.sh && ./scripts/deploy.sh   # la primera vez, dale permiso de ejecución
# Comprobación (o `airflow dags reserialize` para no esperar el refresh):
ssh -i ~/.ssh/pyspark_stack ec2-user@"$IP" \
  'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags list | grep <dag>'
```

Ojo: config nueva (`spark-conf/`, `scripts/`, `monitoring/`, `requirements.txt`) **no** la sube
este script — usá el rsync completo de §5.5 o el workflow de §11.

### 10.2 Del laptop a producción — el loop completo (dev → deploy → corre solo → se apaga)

El ciclo de punta a punta, con los archivos que vas creando con esta guía (copy-paste):

1. **Dev — probás el DAG local.** Los DAGs actuales (`dags/customer_etl_dag.py`,
   `dags/spark_trigger_dag.py`) corren contra el **Spark + HDFS local** del `docker-compose.yml`.
   Iterás con `./scripts/deploy.sh` (§10.1) o levantando el compose en tu máquina. Estos DAGs
   quedan **intactos** — son el camino de desarrollo.

2. **Prod — escribís la variante EMR.** Agregás el DAG de producción con
   `EmrServerlessStartJobOperator`: `dags/customer_etl_emr_dag.py` (dag_id `customer_etl_emr`,
   entrypoint `spark-apps/emr/customer_etl.py`) y `dags/spark_trigger_emr_dag.py` (dag_id
   `spark_wordcount_emr`, entrypoint `spark-apps/emr/wordcount.py`). Leen las Airflow Variables
   `emr_app_id`, `emr_job_role_arn`, `datalake` y `artifacts` (§14.1) para armar el `entryPoint`
   (`s3://<artifacts>/emr/customer_etl.py`), los `entryPointArguments` (`[datalake, "{{ ds }}"]`)
   y los logs (`s3://<artifacts>/emr/logs/`). Tienen `schedule=None`: se disparan por el patrón
   **cron → Lambda `trigger-airflow`** (§7), o quedan `is_paused_upon_creation` según convenga.
   **No corren en local** (no hay EMR): el import del provider amazon está **protegido**
   (try/except) para que el parseo del `dag-processor` local no se rompa aunque el provider no esté
   en la imagen de dev.

3. **`git push` → CI.** `.github/workflows/ci.yml` valida en cada PR/push: `ruff` (lint + format),
   **validación de DAGs** con `pytest tests/test_dag_integrity.py` sobre el `DagBag` (cero import
   errors), **security scan** con gitleaks, y `terraform validate` condicional (ver §11).

4. **Merge a `main` → CD.** `.github/workflows/deploy.yml` (OIDC, sin claves) despliega:
   `aws s3 sync dags/` → `s3://<artifacts>/deploy/dags/` y `aws s3 sync spark-apps/emr/` →
   `s3://<artifacts>/emr/`, y luego un **SSM sync-down** en la EC2 (tag `Name=pyspark-stack-node`)
   que baja los DAGs a la EC2. El `dag-processor` los toma en **~30 s** (§10) y quedan **activos**
   (no pausados: `DAGS_ARE_PAUSED_AT_CREATION=False`).

5. **Corre solo.** EventBridge **start-cron** prende la EC2 (§5.4); dentro de la ventana de
   encendido, EventBridge **ETL-cron → Lambda `trigger-airflow`** dispara el DAG (§7.2); el job
   pesado corre en **EMR Serverless** (`StartJobRun`), que **escala a cero** al terminar.

6. **Se apaga solo al terminar.** La task final `trigger_stop` del DAG apaga la EC2 cuando el
   pipeline termina — **no a una hora fija**. Detalle en §10.3.

**Los dos DAGs de producción, completos (copy-paste).** El paso 2 de arriba los nombra; acá van
enteros. Ambos protegen el import del provider `amazon` con `try/except`: si el provider no está
(dev local), el DAG **no se registra** y el `dag-processor` local no rompe. Tienen `schedule=None`:
los dispara la Lambda `trigger-airflow` (§7), no el Airflow local.

`dags/customer_etl_emr_dag.py` — orquesta `customer_etl` en EMR Serverless y apaga la EC2 al
terminar (`trigger_stop`, §10.3):

```python
"""customer_etl en producción: Airflow orquesta, EMR Serverless computa.

- schedule=None: lo dispara la Lambda trigger-airflow (cron/evento S3, §7); no corre
  en el Airflow local (no hay EMR ahí).
- Import del provider PROTEGIDO: si el provider amazon no está (dev local), el DAG no
  se registra y el dag-processor local no rompe.
- trigger_stop: al terminar (éxito o fallo) apaga la EC2 vía la Lambda startstop (§10.3).
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable, task

try:
    from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
except ImportError:
    EmrServerlessStartJobOperator = None  # dev local sin el provider amazon


if EmrServerlessStartJobOperator is not None:
    DATALAKE = Variable.get("datalake", default="pyspark-stack-datalake-<acct>")
    ARTIFACTS = Variable.get("artifacts", default="pyspark-stack-artifacts-<acct>")

    default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)}

    with DAG(
        dag_id="customer_etl_emr",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule=None,  # disparado por la Lambda trigger-airflow (§7)
        catchup=False,
        tags=["emr", "prod", "etl"],
    ) as dag:
        run = EmrServerlessStartJobOperator(
            task_id="customer_etl",
            application_id="{{ var.value.emr_app_id }}",
            execution_role_arn="{{ var.value.emr_job_role_arn }}",
            # deferrable: mientras EMR procesa, la task NO ocupa un worker slot — el
            # airflow-triggerer (ya en el stack) maneja la espera. Es la clave para correr
            # muchos jobs concurrentes en una EC2 chica sin quedarte sin RAM (ver §10.4).
            deferrable=True,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{ARTIFACTS}/emr/customer_etl.py",
                    "entryPointArguments": [DATALAKE, "{{ ds }}"],
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
                    ),
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{ARTIFACTS}/emr/logs/"}
                }
            },
        )

        @task(trigger_rule="all_done")  # apaga la EC2 aunque el ETL falle
        def trigger_stop():
            import json

            import boto3

            boto3.client("lambda").invoke(
                FunctionName="pyspark-stack-startstop",
                InvocationType="Event",
                Payload=json.dumps({"action": "stop"}).encode(),
            )

        run >> trigger_stop()
```

`dags/spark_trigger_emr_dag.py` — la variante demo (`wordcount`) para validar el camino
Airflow → EMR sin datos en `raw/`:

```python
"""wordcount en producción: Airflow dispara, EMR Serverless computa (demo)."""
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable

try:
    from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
except ImportError:
    EmrServerlessStartJobOperator = None


if EmrServerlessStartJobOperator is not None:
    ARTIFACTS = Variable.get("artifacts", default="pyspark-stack-artifacts-<acct>")
    DATALAKE = Variable.get("datalake", default="pyspark-stack-datalake-<acct>")

    default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)}

    with DAG(
        dag_id="spark_wordcount_emr",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["emr", "prod", "demo"],
    ) as dag:
        EmrServerlessStartJobOperator(
            task_id="wordcount",
            application_id="{{ var.value.emr_app_id }}",
            execution_role_arn="{{ var.value.emr_job_role_arn }}",
            deferrable=True,  # espera en el triggerer, sin ocupar worker slot (§10.4)
            job_driver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{ARTIFACTS}/emr/wordcount.py",
                    "entryPointArguments": [f"s3a://{DATALAKE}/analytics/wordcount"],
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=1 --conf spark.executor.memory=2g"
                    ),
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{ARTIFACTS}/emr/logs/"}
                }
            },
        )
```

> Verificá: CI valida el parseo (§11.2) — `pytest tests/` construye el `DagBag` y falla si alguno
> no importa; con el provider instalado en el runner, ambos DAGs se registran y cumplen los
> estándares mínimos (tags/owner/retries).

### 10.3 Apagado job-aware: se apaga cuando terminan los jobs, no a hora fija

El apagado no es a reloj: la EC2 se apaga **cuando el último job terminó**. Dos piezas se combinan.

**a) Task final `trigger_stop` en el DAG** (ya incluida en `dags/customer_etl_emr_dag.py`). Al
terminar el pipeline (`trigger_rule="all_done"`, así se apaga **aunque el ETL falle**), una task
invoca la Lambda `pyspark-stack-startstop` con `{"action":"stop"}` vía boto3:

```python
from airflow.sdk import task
import boto3, json
@task(trigger_rule="all_done")   # se apaga aunque el ETL falle
def trigger_stop():
    boto3.client("lambda").invoke(
        FunctionName="pyspark-stack-startstop",
        InvocationType="Event",
        Payload=json.dumps({"action": "stop"}).encode(),
    )
```

La invocación usa el **rol IAM de la EC2** (`lambda:InvokeFunction` sobre el ARN de la Lambda, §6.4)
— sin keys. `InvocationType="Event"` es asíncrona: la task no espera a la Lambda. Encadenala al
final del DAG (`... >> trigger_stop()`).

**b) Lambda `startstop` reforzada (guardia anti-corte).** El handler `stop` (§5.4) **no apaga a
ciegas**: antes consulta vía **SSM `SendCommand`** si hay **DAG runs activos** (`running`) en
Airflow, y si hay alguno **no apaga**. Así, con varios DAGs en vuelo, cada `trigger_stop` que corre
encuentra a los demás todavía `running` y se abstiene; **solo el último en terminar** deja apagar la
caja. Evita que un DAG corte a otro que sigue corriendo.

> Timing importante: `trigger_stop` invoca la Lambda de forma **asíncrona** (`InvocationType="Event"`)
> y retorna al instante, con lo que la task termina y el DAG run pasa a `success` en ~1–2 s. La
> Lambda, en cambio, recién hace su primer chequeo SSM unos segundos después (ver el loop de
> `_dags_activos` en §5.4), cuando el DAG que la disparó **ya no figura `running`** — así el guard
> no se bloquea a sí mismo y solo ve DAGs *ajenos* que sigan corriendo.

**c) El cron de stop de las 22:00 queda como red de seguridad** (*hard safety net*). El `schedule` de
stop de §5.4 sigue existiendo: si un DAG **cuelga** y nunca dispara su `trigger_stop`, el cron apaga
igual a la hora fija. Con el pipeline sano, la EC2 se apaga **apenas termina el último job** (mucho
antes de las 22:00) y solo pagás las horas que realmente usaste; el cron es el respaldo para el caso
patológico.

**d) IAM (ya aplicado en la guía).** El rol de la EC2 tiene `lambda:InvokeFunction` sobre el ARN de
`pyspark-stack-startstop` (§6.4, para que `trigger_stop` la invoque); y la Lambda `startstop` tiene
`ssm:SendCommand` + `ssm:GetCommandInvocation` sobre la instancia (§5.4, para el chequeo de DAGs
antes de apagar).

> Resultado: apagado **por evento** (fin de los jobs), no por reloj — con la red de seguridad del
> cron por si algo cuelga. Es exactamente el "se apaga solo al terminar" del loop de §10.2.

### 10.4 Concurrencia y sizing — muchos jobs a la vez

Regla mental clave: **la EC2 no procesa datos, solo orquesta.** El cómputo (los filtros, joins y
`groupBy` sobre millones de filas) corre en **EMR Serverless**; la `t3.large` solo dispara los jobs
y espera su resultado. Por eso el **volumen de datos no dimensiona la EC2** — dimensiona a EMR.

Ejemplo concreto: **~10 jobs moviendo 3 millones de filas × 20 columnas cada uno**. Esos 3M×20 son
~0.5–2 GB por job (según el ancho de las columnas; en Parquet, menos): **dato chico** para Spark. En
EMR Serverless entra holgado, incluso con joins/`groupBy`. La carga sobre la `t3.large` es despreciable. Hay **dos
perillas** distintas, una por cada capa:

**1) Concurrencia en la EC2 (orquestación) → `deferrable=True` + `airflow-triggerer`.** Cada task de
EMR **dispara el job y espera** que termine; esa espera es I/O (polling), no CPU. Con
`deferrable=True` (ya puesto en los DAGs de §10.2), mientras EMR procesa la task **no ocupa un worker
slot**: la maneja el `airflow-triggerer` (ya en el stack, §14.1). Un solo triggerer sostiene
**decenas o cientos** de esperas con RAM/CPU mínima. Sin deferrable, cada job en vuelo se lleva un
subproceso (~200–400 MB) y 10 concurrentes ponen al límite los 8 GB de la `t3.large`.

**2) Paralelismo real en EMR → `maximum_capacity` (§6.4).** Cuántos jobs corren **a la vez de
verdad** lo fija el tope de la aplicación EMR Serverless:

```hcl
maximum_capacity {   # ~2–4 jobs pesados en paralelo; el resto ENCOLA
  cpu    = "16 vCPU"
  memory = "64 GB"
}
```

Con 16 vCPU, ~2–4 jobs "pesados" corren simultáneos y los demás **encolan** (arrancan al liberarse
capacidad). Como cada job de 3M filas es corto, aunque los 10 se serialicen terminan rápido. Si
querés los **10 en paralelo real**, subí el tope (p. ej. `32`–`64 vCPU`): pagás igual **por uso**,
solo levantás el techo de concurrencia (mirá también las cuotas de EMR Serverless de tu cuenta).

**Cuándo la `t3.large` alcanza y cuándo saltar de tamaño:**

| Escenario | EC2 recomendada |
|---|---|
| 10 jobs escalonados o pocos simultáneos, con `deferrable` | `t3.large` (2/8) — sobra |
| 10 jobs **simultáneos** con `deferrable` + triggerer | `t3.large` — bien |
| 10 simultáneos **sin** `deferrable` (polling clásico) | `t3.xlarge` (4/16) — por RAM |

**3) Concurrencia del MISMO DAG → `max_active_runs=1` (auditoría §3.1).** Las dos perillas de
arriba acotan cuánto cómputo/RAM se usa corriendo en paralelo — pero no evitan el desperdicio de
fondo: si `customer_etl_emr` procesa por `{{ ds }}` (todo el día, no por archivo puntual, §7.3) y
suben 10 archivos el mismo día, los 10 disparos son 10 dagruns que hacen **el mismo trabajo**. El
`reserved_concurrent_executions = 2` de la Lambda (§7.1) frena cuántos triggers salen a la vez, pero
no evita que Airflow acepte igual esos triggers como runs separados. `max_active_runs=1` en el DAG sí:
Airflow encola el segundo trigger sin arrancar un segundo run mientras el primero siga activo.

```python
with DAG(
    "customer_etl_emr",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,   # el segundo disparo del mismo día espera, no duplica el trabajo
) as dag:
    ...
```

Escalar la EC2 es cambiar `instance_type` (§5.1), sin tocar el diseño; la alerta `HostLowMemory`/
`HostDiskAlmostFull` (§12.4) te avisa si la EC2 se queda corta antes de que sea un problema. En resumen: para
tu carga, **la `t3.large` está bien** — el ajuste fino es usar `deferrable` y, si querés concurrencia
real, subir el `maximum_capacity` de EMR, no agrandar la EC2.

---

## 11. CI/CD con GitHub Actions (OIDC, sin claves)

Dos workflows **completos**, para crear en `.github/workflows/` y versionar en el repo: `ci.yml`
(valida en cada PR/push) y `deploy.yml` (despliega al mergear a `main`). No son pseudocódigo: son el
pipeline **DataOps** real (verificado con `ruff` + `pytest`), listo para copiar y activar. GitHub Actions asume un rol IAM vía
**OIDC** — sin access keys guardadas en el repo.

- **CI** (`ci.yml`): `ruff` (lint + format), **validación de DAGs** con
  `pytest tests/test_dag_integrity.py` sobre el `DagBag` (cero import errors), **security scan** con
  gitleaks, y `terraform validate` condicional (solo si tocaste `infra/`). No toca AWS — corre en
  todo PR sin credenciales.
- **CD** (`deploy.yml`): autentica por OIDC (`configure-aws-credentials`), hace `aws s3 sync` de
  `dags/` → `deploy/dags/` y de `spark-apps/emr/` → `emr/`, dispara el **SSM sync-down** en la EC2 y
  corre un **smoke test**. Va con `environment: production` para el **gate de aprobación** manual.

**Variables de repo a setear** (Settings → Secrets and variables → Actions → *Variables*, no
secrets): `AWS_DEPLOY_ROLE_ARN` (el output del rol de abajo), `AWS_REGION` y `ARTIFACTS_BUCKET`
(`pyspark-stack-artifacts-<account-id>`).

### 11.1 Terraform: OIDC provider + rol — `infra/prod/cicd.tf`

```hcl
# GitHub Actions (deploy.yml) asume este rol vía OIDC. Puede: subir a S3 (deploy/dags/ + emr/),
# disparar el sync-down en la EC2 (SSM) y —opcional— leer el state para `terraform plan`. El
# `apply` queda manual/local. El ARN se guarda como VARIABLE de repo AWS_DEPLOY_ROLE_ARN.

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
      # OJO: cuando un job declara `environment:` (deploy.yml usa `environment: production`),
      # GitHub emite el sub como `repo:org/repo:environment:<nombre>`, NO como `ref:refs/heads/...`.
      # Con la forma `ref:` el AssumeRoleWithWebIdentity falla siempre. Si además querés permitir
      # workflows sin environment, usá `repo:${var.github_repo}:*` (más laxo).
      values   = ["repo:${var.github_repo}:environment:production"]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "${var.name_prefix}-github-actions"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

data "aws_iam_policy_document" "github_deploy" {
  # ListBucket sobre el bucket; escritura scopeada a los dos prefijos que toca el CD:
  # deploy/ (DAGs que baja la EC2) y emr/ (entrypoints que EMR Serverless toma directo de S3).
  statement {
    sid       = "S3ListArtifacts"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn]
  }
  statement {
    sid     = "S3DeployObjects"
    actions = ["s3:PutObject", "s3:DeleteObject", "s3:GetObject"]
    resources = [
      "${aws_s3_bucket.artifacts.arn}/deploy/*",
      "${aws_s3_bucket.artifacts.arn}/emr/*",
    ]
  }
  statement {
    # Lo usa el step "Resolver instancia" de deploy.yml (§11.3). DescribeInstances no admite ARN
    # de recurso: exige "*" (el filtro por tag va en el workflow, no en IAM). Va como statement
    # propio a propósito: antes solo lo cubría el ReadOnlyAccess de abajo, que el doc llama
    # "opcional" — quien lo quitaba por least-privilege rompía el deploy con AccessDenied.
    sid       = "Ec2Describe"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
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
  # TfState (y el ReadOnlyAccess de abajo) existe solo para correr `terraform plan` en CI;
  # el workflow de §11.2 no lo corre — si no lo agregás, podés quitar estos permisos (least-privilege).
  # DeleteObject NO es opcional: con use_lockfile el lock es un objeto <key>.tflock en el mismo
  # bucket, y sin permiso de borrado Terraform lo toma pero no lo libera (el próximo run queda trabado).
  statement {
    sid       = "TfState"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = ["arn:aws:s3:::*tfstate*", "arn:aws:s3:::*tfstate*/*"]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  name   = "deploy-policy"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.github_deploy.json
}

# ReadOnly para que `terraform plan` lea el estado real en CI (mismo aviso: opcional si no hay plan en CI).
resource "aws_iam_role_policy_attachment" "github_readonly" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions.arn
}
```

<details>
<summary>🖱️ A mano en la consola AWS — OIDC de GitHub Actions</summary>

1. **IAM → Identity providers → Add provider** → **OpenID Connect** · *Provider URL*:
   `https://token.actions.githubusercontent.com` · *Audience*: `sts.amazonaws.com`.
2. **IAM → Roles → Create role** → *Trusted entity*: **Web identity** → elegí ese provider y
   audience `sts.amazonaws.com`. **No uses el campo *branch* del asistente**: `deploy.yml` declara
   `environment: production`, y en ese caso GitHub emite el `sub` como
   `repo:org/repo:environment:production`, no como `ref:refs/heads/main`. Editá la trust policy a
   mano y poné esa condición, o el rol nunca se va a poder asumir.
3. Permisos: inline policy JSON con los statements del Terraform (`s3:PutObject`/`DeleteObject`/
   `GetObject` sobre `deploy/*` y `emr/*` del bucket de artifacts, `s3:ListBucket` sobre el bucket,
   `ssm:SendCommand` a tu instancia, y —opcional— lectura de tfstate/lock + `ReadOnlyAccess`).
4. Nombre `pyspark-stack-github-actions` → copiá el **ARN del rol** y guardalo en GitHub como
   **variable** (no secret) **`AWS_DEPLOY_ROLE_ARN`** (Settings → Secrets and variables → Actions →
   *Variables*). Creá también las variables **`AWS_REGION`** y **`ARTIFACTS_BUCKET`**.

</details>

> Necesitás el provider `tls` en `providers.tf`:
> ```hcl
> tls = { source = "hashicorp/tls", version = "~> 4.0" }
> ```

Tras `terraform apply`, copiá el output `github_actions_role_arn` y guardalo como **variable**
`AWS_DEPLOY_ROLE_ARN` en GitHub (Settings → Secrets and variables → Actions → *Variables*), junto con
`AWS_REGION` y `ARTIFACTS_BUCKET`.

### 11.1b Un segundo rol OIDC, solo para dbt Slim CI (menos privilegios, sin gate de aprobación)

`aws_iam_role.github_actions` de arriba está atado al *environment* `production` — a propósito,
porque `deploy.yml` sí necesita el gate de *required reviewers* (§11.4). Slim CI (§11.2) necesita
correr **automático en cada PR**, sin que nadie apruebe nada — si reusara el rol de deploy, o le
sacás el gate (y el deploy deja de estar protegido) o el CI de cada PR queda esperando aprobación
manual (y deja de ser CI). Por eso es un rol aparte, con su propio trust condition y permisos
acotados **solo** a la database `_ci` (§16.2) — nunca puede tocar `pyspark_stack_analytics` real.

```hcl
# infra/prod/cicd.tf (continuación) — reusa el mismo OIDC provider de arriba, NO crea uno nuevo
data "aws_iam_policy_document" "dbt_ci_assume" {
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
      # Sin `environment:`, así corre en PRs de cualquier rama sin gate de aprobación — es la
      # diferencia clave con aws_iam_role.github_actions. El acotamiento de seguridad no es el
      # trust condition acá, es la POLICY: least-privilege a la database _ci, nada más.
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}
resource "aws_iam_role" "dbt_ci" {
  name               = "${var.name_prefix}-dbt-ci"
  assume_role_policy = data.aws_iam_policy_document.dbt_ci_assume.json
}

data "aws_iam_policy_document" "dbt_ci" {
  statement {   # leer las fuentes (raw/curated) que los modelos dbt consultan
    sid       = "ReadSources"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.datalake.arn, "${aws_s3_bucket.datalake.arn}/*"]
  }
  statement {   # escribir SOLO bajo dbt-ci/ y athena-results/ del bucket de artifacts
    sid     = "WriteCiOutputs"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.artifacts.arn}/dbt-ci/*",
      "${aws_s3_bucket.artifacts.arn}/athena-results/*",
      "${aws_s3_bucket.artifacts.arn}/dbt-state/manifest.json",   # el --state que compara Slim CI
    ]
  }
  statement {
    sid       = "AthenaResultsBucket"   # mismo caso de §16.3: Athena valida a nivel bucket, no prefijo
    actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.artifacts.arn]
  }
  statement {
    sid       = "AthenaQuery"
    actions   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"]
    resources = ["arn:aws:athena:${local.region}:${local.account_id}:workgroup/${var.name_prefix}-analytics"]
  }
  statement {   # Glue SOLO sobre la database _ci — el corazón del aislamiento least-privilege
    sid     = "GlueCiOnly"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions"]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.analytics_ci.arn,
      "${aws_glue_catalog_database.analytics_ci.arn}/*",
    ]
  }
}
resource "aws_iam_role_policy" "dbt_ci" {
  name   = "dbt-ci-policy"
  role   = aws_iam_role.dbt_ci.id
  policy = data.aws_iam_policy_document.dbt_ci.json
}

output "dbt_ci_role_arn" { value = aws_iam_role.dbt_ci.arn }
```

Guardá el output como variable **`AWS_DBT_CI_ROLE_ARN`** en GitHub (junto a `AWS_DEPLOY_ROLE_ARN`).

### 11.2 Workflow de CI — `.github/workflows/ci.yml`

Es el workflow que corre en el repo, con cuatro jobs independientes: calidad de código + DAGs,
security scan, Terraform y **dbt Slim CI**. Los tres primeros no usan credenciales AWS (todo local
al runner); el cuarto sí — es la única excepción, y usa el rol acotado `dbt_ci` de §11.1b, no el de
deploy.

```yaml
# .github/workflows/ci.yml
name: CI

on:
  pull_request:
  push:
    branches-ignore:
      - main

permissions:
  contents: read

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  lint:
    name: Lint (ruff)
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - name: Instalar ruff
        run: pip install ruff==0.14.3
      - name: Ruff check
        run: ruff check .
      - name: Ruff format (check)
        run: ruff format --check .

  dag-validate:
    name: Validar DAGs (Airflow 3.2.2)
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - name: Instalar Airflow 3.2.2 + providers (con constraints)
        env:
          CONSTRAINTS: "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt"
        run: |
          python -m pip install --upgrade pip
          pip install "apache-airflow==3.2.2" --constraint "${CONSTRAINTS}"
          pip install \
            "apache-airflow-providers-amazon==9.29.0" \
            "apache-airflow-providers-apache-spark==6.0.2" \
            pytest
      - name: Pytest (integridad de DAGs)
        run: pytest tests/ -q

  security:
    name: Seguridad (gitleaks + IaC)
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: gitleaks (secret scan)
        uses: gitleaks/gitleaks-action@v2
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      - name: checkov (IaC scan de Terraform)
        if: hashFiles('infra/**/*.tf') != ''
        uses: bridgecrewio/checkov-action@v12
        with:
          directory: infra/
          framework: terraform
          quiet: true
          # true a propósito: contra este mismo Terraform checkov reporta ~18 hallazgos (Lambda sin
          # DLQ/X-Ray/CMK, EBS sin CMK, SG abierto al 0.0.0.0/0 de egress, bucket de tfstate sin
          # replicación...). Con false, TODO PR queda en rojo desde el día uno. Endurecelo cuando
          # hayas triado los hallazgos, con --skip-check o un baseline. (El de DLQ en las Lambdas ya
          # se resuelve en §18.1 — el resto sigue abierto a propósito, priorizado por vos.)
          soft_fail: true

  terraform-validate:
    name: Terraform validate
    runs-on: ubuntu-latest
    timeout-minutes: 10
    # OJO: acá NO va `if: hashFiles(...)`. hashFiles no es una función válida en `jobs.<id>.if`
    # (solo en `steps.if`): el workflow ENTERO falla a parsear con "Unrecognized function" y no
    # corre ningún job. Como infra/**/*.tf siempre existe en este repo, el guard no aportaba nada.
    steps:
      - uses: actions/checkout@v4
      - name: Setup Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_wrapper: false
      - name: terraform fmt
        run: terraform fmt -check -recursive
        working-directory: infra
      - name: terraform init (sin backend)
        run: terraform init -backend=false
        working-directory: infra
      - name: terraform validate
        run: terraform validate
        working-directory: infra

  dbt-slim-ci:
    name: dbt Slim CI (state:modified+)
    runs-on: ubuntu-latest
    timeout-minutes: 15
    # Único job con credenciales AWS de este workflow — id-token: write a nivel job (no de repo,
    # el `permissions: contents: read` de arriba sigue rigiendo para los otros tres).
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
        with:
          # Sin cambios en dbt/ -> no corre nada (Slim CI: solo modelos afectados por el PR).
          sparse-checkout: dbt
      - name: Setup Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - name: Configure AWS credentials (OIDC, rol dbt_ci — §11.1b)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_DBT_CI_ROLE_ARN }}
          aws-region: ${{ vars.AWS_REGION }}
      - name: Instalar dbt
        run: pip install "dbt-core==1.9.*" "dbt-athena-community==1.9.*"
      - name: Bajar el manifest de producción (baseline de state:modified+)
        run: |
          mkdir -p state
          aws s3 cp "s3://${{ vars.ARTIFACTS_BUCKET }}/dbt-state/manifest.json" state/manifest.json \
            || echo "sin baseline todavía (primera corrida) — Slim CI corre todos los modelos esta vez"
      - name: dbt build --select state:modified+
        working-directory: dbt
        env:
          ARTIFACTS_BUCKET: ${{ vars.ARTIFACTS_BUCKET }}
        run: |
          # --profiles-dir .: dbt busca profiles.yml en ~/.dbt/ por defecto, NO en el proyecto.
          if [ -s ../state/manifest.json ]; then
            dbt build --target ci --profiles-dir . --select state:modified+ --state ../state
          else
            dbt build --target ci --profiles-dir .   # sin baseline: corre todo, a propósito
          fi
```

> La validación de DAGs necesita Airflow instalado en el runner para construir el `DagBag`; el
> `conftest.py` de `tests/` prepara el entorno (setea las env vars de Airflow **antes** de importarlo).
> El test recorre `dags/` y afirma `dag_bag.import_errors == {}`, así un `EmrServerlessStartJobOperator`
> mal escrito o una Variable inexistente rompen el PR antes de llegar a `main`.

El job `dag-validate` corre `pytest tests/`, así que estos dos archivos de test tienen que existir en
el repo. `tests/conftest.py` — setea las env vars de Airflow **antes** de importarlo (si no, Airflow
crea su config con defaults y el `DagBag` no parsea limpio):

```python
"""Config global de pytest para validar los DAGs en CI (setea env vars ANTES de importar airflow)."""
import os
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_FOLDER = REPO_ROOT / "dags"
_AIRFLOW_HOME = tempfile.mkdtemp(prefix="airflow_ci_")

os.environ.setdefault("AIRFLOW_HOME", _AIRFLOW_HOME)
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))
```

`tests/test_dag_integrity.py` — el gate real: parseo sin import errors + estándares mínimos
(tags/owner/retries). Los DAGs de dev local previos al CI están *grandfathered* solo para los
estándares; la integridad estructural se les exige igual:

```python
"""Integridad de los DAGs (gate del job dag-validate): parseo + estándares mínimos."""
from pathlib import Path

import pytest
from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_FOLDER = REPO_ROOT / "dags"

# DAGs de dev local previos al CI: grandfathered SOLO para estándares (tags/owner/retries).
# La integridad estructural (import_errors, ciclos) SÍ se les exige. DAGs nuevos: no agregar acá.
LEGACY_DAGS_GRANDFATHERED = {
    "customer_etl_dag",
    "spark_wordcount_trigger",
    "spark_wordcount_trigger_hdfs",
}


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    if dagbag.import_errors:
        detalle = "\n".join(f"  - {f}: {e.strip().splitlines()[-1]}" for f, e in dagbag.import_errors.items())
        pytest.fail(f"DAGs que no parsean:\n{detalle}")


def test_dagbag_no_vacio(dagbag: DagBag) -> None:
    assert dagbag.dags, f"No se cargó ningún DAG desde {DAGS_FOLDER}"


@pytest.mark.parametrize("atributo", ["tags", "owner", "retries"])
def test_estandares_minimos(dagbag: DagBag, atributo: str) -> None:
    incumplen = []
    for dag_id, dag in dagbag.dags.items():
        if dag_id in LEGACY_DAGS_GRANDFATHERED:
            continue
        if atributo == "tags" and not getattr(dag, "tags", None):
            incumplen.append(dag_id)
        elif atributo == "owner" and not (getattr(dag, "owner", "") or "").strip():
            incumplen.append(dag_id)
        elif atributo == "retries" and "retries" not in (getattr(dag, "default_args", None) or {}):
            incumplen.append(dag_id)
    assert not incumplen, f"DAG(s) sin '{atributo}': {', '.join(sorted(incumplen))}"
```

El tooling que sigue mantiene consistente el lint entre tu máquina y el CI. `ruff.toml` — un único config de
ruff; el legacy dev-local queda excluido y el código **nuevo** (`tests/`, `dags/` nuevos,
`spark-apps/emr/`) sí se lintea:

```toml
target-version = "py312"
line-length = 100

# Legacy dev-local y artefactos quedan fuera; el código NUEVO (tests/, dags/ nuevos,
# spark-apps/emr/) SÍ se lintea.
extend-exclude = [
    "spark-events", "**/notebook-output", "**/output", "__pycache__", ".ipynb_checkpoints",
    "**/.terraform", "notebooks",
    "spark-apps/scripts", "spark-apps/customer_etl", "spark-apps/sales_etl",
    "spark-apps/project1", "spark-apps/cron", "spark-apps/wordcount.py", "spark-apps/wordcount_hdfs.py",
    "dags/customer_etl_dag.py", "dags/spark_trigger_dag.py", "dags/spark_trigger_hdfs_dag.py",
]

[lint]
select = ["E", "F", "I", "UP", "B"]
ignore = ["E501"]

[lint.isort]
known-first-party = ["dags", "tests"]

[format]
quote-style = "double"
indent-style = "space"
line-ending = "lf"
```

`.pre-commit-config.yaml` — corré los mismos checks localmente antes de pushear (ruff, higiene de
archivos, gitleaks), así el PR no rebota en CI:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.14.3
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: check-yaml
        args: [--allow-multiple-documents]
      - id: check-added-large-files
        args: [--maxkb=1024]
  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.21.2
    hooks:
      - id: gitleaks
```

`Makefile` — atajos para los mismos comandos que corre el CI (`make lint`, `make test`,
`make fmt`, `make precommit`):

```makefile
.DEFAULT_GOAL := help
.PHONY: help lint fmt test precommit

help:  ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

lint:  ## ruff check + format --check (no modifica)
	ruff check .
	ruff format --check .

fmt:  ## Formatea in-place
	ruff format .
	ruff check --fix .

test:  ## Valida que los DAGs parsean y cumplen estándares
	pytest tests/ -q

precommit:  ## Corre todos los hooks
	pre-commit run --all-files
```

> Verificá: `make lint && make test` en local reproduce los jobs `lint` y `dag-validate` del CI
> (ruff pasa, pytest 5/5: parseo + DagBag no vacío + los 3 estándares parametrizados). Con
> `pre-commit install`, los hooks corren en cada commit.

### 11.3 Workflow de Deploy — `.github/workflows/deploy.yml`

El que corre en el repo. Solo se dispara si cambiaron `dags/` o `spark-apps/emr/` (el código que
despliega); usa las **variables de repo** `AWS_DEPLOY_ROLE_ARN`, `AWS_REGION`, `ARTIFACTS_BUCKET`, y
va detrás de `environment: production` (gate de aprobación).

```yaml
# .github/workflows/deploy.yml
name: Deploy
on:
  push:
    branches: [main]
    paths:
      - "dags/**"
      - "spark-apps/emr/**"

permissions:
  id-token: write   # requerido para OIDC
  contents: read

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production   # gate de aprobación manual (GitHub Environments)
    steps:
      - uses: actions/checkout@v4

      - name: Autenticar en AWS (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}
          aws-region: ${{ vars.AWS_REGION }}

      - name: Sync a S3 (DAGs → deploy/dags/, entrypoints → emr/)
        run: |
          B=${{ vars.ARTIFACTS_BUCKET }}
          aws s3 sync dags/           s3://$B/deploy/dags/ --delete --exclude '__pycache__/*'
          # Entrypoints PySpark → emr/: EMR Serverless los toma directo de S3 en cada StartJobRun (§6.4).
          aws s3 sync spark-apps/emr/ s3://$B/emr/         --delete --exclude '__pycache__/*'

      - name: Resolver instancia
        id: res
        run: |
          I=$(aws ec2 describe-instances \
            --filters "Name=tag:Name,Values=pyspark-stack-node" \
                      "Name=instance-state-name,Values=running" \
            --query 'Reservations[0].Instances[0].InstanceId' --output text)
          echo "instance=$I" >> "$GITHUB_OUTPUT"

      - name: Sync-down + smoke en la EC2 vía SSM
        run: |
          I=${{ steps.res.outputs.instance }}
          if [ "$I" = "None" ] || [ -z "$I" ]; then
            echo "EC2 apagada: el deploy quedó en S3 (deploy/dags/ + emr/); se aplica al encenderla."
            exit 0
          fi
          B=${{ vars.ARTIFACTS_BUCKET }}
          CMD=$(aws ssm send-command --instance-ids "$I" \
            --document-name AWS-RunShellScript --comment "deploy sync-down + smoke" \
            --parameters commands="[\
              \"cd /home/ec2-user/pyspark_stack\",\
              \"aws s3 sync s3://$B/deploy/dags/ dags/ --delete --only-show-errors\",\
              \"docker compose exec -T airflow-dag-processor airflow dags reserialize\",\
              \"echo '===SMOKE==='\",\
              \"docker compose exec -T airflow-scheduler airflow dags list-import-errors\"\
            ]" --query 'Command.CommandId' --output text)
          # `wait` sale con error si el comando falló o expiró; la assertion real es el Status +
          # que list-import-errors venga vacío (si imprime un .py, el deploy rompió un DAG).
          aws ssm wait command-executed --command-id "$CMD" --instance-id "$I" || true
          STATUS=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$I" --query 'Status' --output text)
          OUT=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$I" --query 'StandardOutputContent' --output text)
          echo "$OUT"
          # El grep va acotado a lo que sigue al centinela: si mirara todo $OUT, las líneas
          # "download: ... .py" del s3 sync lo harían dar positivo SIEMPRE y el job saldría en
          # rojo aunque el deploy hubiera funcionado (el workflow se dispara con paths: dags/**).
          if [ "$STATUS" != "Success" ] || echo "$OUT" | sed -n '/===SMOKE===/,$p' | grep -q '\.py'; then
            echo "Deploy/smoke falló (Status=$STATUS o hay import errors)"
            aws ssm get-command-invocation --command-id "$CMD" --instance-id "$I" --query 'StandardErrorContent' --output text
            exit 1
          fi
```

> Tres detalles: (1) el CD solo mueve **código** (DAGs + entrypoints EMR); cambios de
> `monitoring/`, los compose o `requirements.txt` van por el rsync completo de §5.5 + un
> `docker compose ... up -d` en la EC2, no por este workflow. (2) El `entryPoint` de los jobs sale
> directo de `s3://<artifacts>/emr/` — EMR lo lee en cada `StartJobRun` sin depender de la EC2. (3)
> El smoke test corre en la propia EC2 (`airflow dags list-import-errors`): si el deploy rompió un
> DAG, el job de Actions falla en rojo.

### 11.4 Puesta en marcha (una vez)

1. Ajustá `github_repo` en `cicd.tf` a tu `org/repo` (o pasalo con `-var 'github_repo=...'`).
2. `terraform -chdir=infra/prod init -upgrade` (incorpora el nuevo provider `tls`) y luego
   `terraform -chdir=infra/prod apply` → copiá `github_actions_role_arn`.
3. GitHub → Settings → Secrets and variables → Actions → *Variables* → crear **`AWS_DEPLOY_ROLE_ARN`**
   (ese ARN), **`AWS_DBT_CI_ROLE_ARN`** (el output `dbt_ci_role_arn` de §11.1b), **`AWS_REGION`** y
   **`ARTIFACTS_BUCKET`** (`pyspark-stack-artifacts-<account-id>`).
4. GitHub → Settings → Environments → crear **`production`** con *Required reviewers* (el gate de
   aprobación que exige `environment: production` en `deploy.yml`). El job `dbt-slim-ci` de §11.2 **no**
   declara `environment`, a propósito: corre sin aprobación en cada PR (§11.1b explica por qué).
5. El **CD** corre bien apenas la EC2 está **encendida** con el proyecto en
   `/home/ec2-user/pyspark_stack` (el sync-down baja los DAGs y corre el smoke); si está apagada, el
   deploy queda en S3 y se aplica en el próximo encendido. Hacé el primer `git push` a `main` que
   toque `dags/` o `spark-apps/emr/` — o probá el OIDC en frío con un `workflow_dispatch` que solo
   corra `aws sts get-caller-identity`. Los PRs disparan **CI** (ruff + validación de DAGs + gitleaks
   + terraform validate + dbt Slim CI si tocaste `dbt/`).

> Seguridad: el rol de deploy solo lo puede asumir tu repo desde el environment `production`
> (condición `sub = repo:org/repo:environment:production`, §11.1); el rol de `dbt_ci` (§11.1b) es un
> rol **distinto**, sin ese gate pero con permisos acotados solo a la database `_ci`. Ninguno usa
> claves de larga vida, y el deploy usa SSM (no expone SSH en CI). El `terraform apply` queda fuera
> de CI (manual/local) para no dar permisos amplios a Actions.

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
| Spark (jobs) | **EMR Serverless** → métricas CloudWatch + logs a S3 (`emr/logs/`) | — (managed) |
| Logs de todos los contenedores | `Promtail` → `Loki` | 3100 |
| Alertas | `Alertmanager` → email | 9093 |
| Dashboards | `Grafana` | 3000 |

Cobertura de la instrumentación, área por área:

| Área | Estado | Nota |
|---|---|---|
| Host / contenedores | OK | node-exporter + cAdvisor |
| Airflow | OK | StatsD → statsd-exporter |
| Spark (jobs EMR Serverless) | OK (managed) | métricas en CloudWatch + logs a S3 (`emr/logs/`); alerta de fallo vía Airflow/CloudWatch (§12.4) |
| Logs centralizados | OK (agregado) | Loki + Promtail (era el gran faltante) |
| Alertas → notificación | OK | Alertmanager (antes las reglas no notificaban) |
| HDFS / Spark en la EC2 | N/A | ya no corren en la EC2 (Spark → EMR Serverless, HDFS eliminado) |
| UI de Spark del job | Consola EMR | EMR reconstruye la Spark UI de cada corrida terminada desde la consola de EMR Serverless (reemplaza al History Server local) |

### 12.2 Estructura de archivos (`monitoring/`)

Creá esta estructura en la raíz del repo. Cada archivo se detalla en §12.3–§12.7, en el mismo orden
en que aparece acá; el compose de §14.1 los monta por ruta, así que los nombres deben coincidir.

```text
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

Estos servicios se agregan al `docker-compose.prod.yml` y ya incluyen `restart`, límites de memoria y rotación de logs.

> Los fragmentos de compose de §12–§13 se explican **por tema**; el archivo real a copiar es el
> `docker-compose.prod.yml` completo de §14.1 — no los escribas a mano ni los concatenes.

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

Para que Airflow **emita** métricas StatsD, agregá estas 4 variables al `environment:` de
`x-airflow-common` en tu `docker-compose.prod.yml` (el mismo anchor `&airflow-common-env` donde ya
sumaste `DAGS_ARE_PAUSED_AT_CREATION`/`DAG_PROCESSOR_REFRESH_INTERVAL` en §10; los 5 servicios
`airflow-*` las heredan solos, no hace falta tocarlos uno por uno):

```yaml
# docker-compose.prod.yml — dentro de x-airflow-common → environment → &airflow-common-env
AIRFLOW__METRICS__STATSD_ON: "True"
AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
AIRFLOW__METRICS__STATSD_PORT: "9125"
AIRFLOW__METRICS__STATSD_PREFIX: airflow
```

### 12.4 Prometheus, reglas de alerta y Alertmanager

Tres archivos, en este orden: `prometheus.yml` define qué se scrapea y a qué Alertmanager notificar,
`alerts.yml` contiene las reglas de alerta y `alertmanager.yml` define el envío por email.

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
  # Spark ya NO corre en la EC2: no hay spark-master/worker que scrapear. Los jobs corren en EMR
  # Serverless → sus métricas van a CloudWatch y sus logs a S3 (emr/logs/); ver §12.8.
  # HDFS eliminado de prod: sin namenode/datanode que monitorear.
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
        # or absent(...): si la serie desaparece (p. ej. statsd-exporter reiniciado), increase()
        # devuelve vacío —no 0— y sin él la alerta nunca dispararía.
        expr: >-
          increase(airflow_dagrun_duration_success_count{dag_id="customer_etl_emr"}[26h]) == 0
          or absent(airflow_dagrun_duration_success_count{dag_id="customer_etl_emr"})
        for: 10m
        labels: { severity: critical }
        annotations:
          summary: "El ETL diario no completó con éxito (dead-man switch)"
          description: "customer_etl_emr (el DAG que dispara el cron de §7.2) no registró corrida exitosa en 26h (¿EC2 apagada? ¿trigger falló?). Ajustá la ventana a tu schedule real."
      # ── EMR Serverless: un job Spark que FALLA hace fallar la task del EmrServerlessJobSensor/
      # Operator en Airflow → se ve en airflow_ti_failures. Esta alerta lo hace explícito. ──
      - alert: EmrServerlessJobFailed
        expr: increase(airflow_ti_failures[15m]) > 0
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "Job Spark de EMR Serverless falló (task de Airflow en error)"
          description: "Una task falló en los últimos 15m; si es la del EmrServerlessStartJobOperator/Sensor, el job de EMR terminó en FAILED. Revisá los logs en s3://artifacts/emr/logs/ o en la consola de EMR Serverless."
```

> Las métricas `airflow_dagrun_duration_{success,failed}_count` salen del `statsd_mapping.yml` (§12.5);
> verificá los nombres exactos en `localhost:9090/api/v1/targets` la primera vez. `DailyEtlMissing` es la
> regla de mayor valor operativo: detecta que el pipeline dejó de correr, no solo que falló.

> Alternativa nativa para EMR: una **alarma CloudWatch** sobre la métrica de *job runs* en estado
> `FAILED` de la aplicación EMR Serverless (namespace `AWS/EMRServerless`), notificando por SNS.
> Cubre el caso incluso si el fallo no llegara a reflejarse como task fallida en Airflow.

> Alertmanager no expande variables de entorno: la contraseña queda en texto plano, por lo que este
> archivo **no** debe versionarse en git.

```yaml
# monitoring/alertmanager/alertmanager.yml
global:
  resolve_timeout: 5m
  smtp_smarthost: "smtp.gmail.com:587"
  smtp_from: "tu-email@gmail.com"
  smtp_auth_username: "tu-email@gmail.com"
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
      - to: "tu-email@gmail.com"
        send_resolved: true
inhibit_rules:
  - source_matchers: ['severity="critical"']
    target_matchers: ['severity="warning"']
    equal: ["instance"]
```

### 12.5 Métricas de Airflow (StatsD)

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

> **Spark ya no se scrapea con Prometheus.** Antes se montaba un `metrics.properties` con el
> `PrometheusServlet` en los daemons `spark-master`/`spark-worker` de la EC2; con Spark en EMR
> Serverless esos daemons no existen. Las métricas del job (CPU, memoria, shuffle, estados de las
> corridas) las publica **EMR Serverless en CloudWatch** (namespace `AWS/EMRServerless`) y los logs
> del job van a S3 (`emr/logs/`) — sin exporter ni servicio que mantener. La observabilidad de EMR
> se cubre en §12.8.

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
    { "type": "stat", "title": "Airflow (statsd-exporter) arriba", "gridPos": {"h":8,"w":12,"x":12,"y":16},
      "targets": [{ "expr": "up{job=\"airflow\"}" }] }
  ],
  "templating": { "list": [] }
}
```

> Para paneles más completos, importá desde la UI los dashboards de la comunidad: el **1860** (Node
> Exporter Full) y alguno de **Airflow** o **cAdvisor**.

> Métricas de EMR Serverless en Grafana (opcional): las publica CloudWatch, no Prometheus. Agregá
> un datasource **CloudWatch** (`type: cloudwatch`, `defaultRegion: us-east-1`, auth por el rol de
> la EC2) al `datasources.yml` y armá paneles sobre el namespace `AWS/EMRServerless` (job runs por
> estado, vCPU/GB-seg consumidos). Requiere permisos `cloudwatch:GetMetricData`/`ListMetrics` en el
> rol de la EC2 si querés consultarlo desde Grafana en la instancia.

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
# Sin compactor con retention_enabled, retention_period NO borra nada y el disco crece sin
# límite; este bloque es lo que hace efectiva la retención de 7 días de abajo.
compactor:
  working_directory: /loki/compactor
  retention_enabled: true
  delete_request_store: filesystem
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

### 12.8 Acceso, verificación y observabilidad de EMR Serverless

Acceso por **túnel SSH** (las UIs de monitoreo no se exponen; la única puerta pública es la web de
Airflow por 443 a tu IP, §5.6):

```bash
ssh -i ~/.ssh/pyspark_stack -L 3000:localhost:3000 -L 9090:localhost:9090 -L 9093:localhost:9093 -L 3100:localhost:3100 ec2-user@$IP
# Grafana localhost:3000 · Prometheus localhost:9090 · Alertmanager localhost:9093 · Loki localhost:3100
```

Verificar: en `http://localhost:9090/targets` todos deben estar UP (node, cadvisor, airflow —ya no
hay spark-master/worker: Spark corre en EMR Serverless). Levantar (tras generar el `.env` con
`load-secrets.sh`, §13.1 — sin él Grafana usa `${GRAFANA_ADMIN_PASSWORD:?}` y aborta):
`docker compose -f docker-compose.prod.yml up -d`.
Verificá también Grafana → dashboard "pyspark-stack — Overview" con datos y los datasources
Prometheus + Loki en verde.

**Observabilidad de los jobs Spark (EMR Serverless):** el cómputo Spark es *managed*, así que su
telemetría no pasa por Prometheus/Loki de la EC2 sino por AWS:

- **Métricas** → **CloudWatch**, namespace `AWS/EMRServerless` (job runs por estado, vCPU/GB-seg,
  memoria). Opcionalmente se ven en Grafana con un datasource CloudWatch (§12.6).
- **Logs del job** → **S3** en `s3://<artifacts>/emr/logs/` (driver + executors), y también a
  CloudWatch Logs (`/aws/emr-serverless/pyspark-stack`, §6.4). Para tirar de un log:

```bash
ACCT=$(aws sts get-caller-identity --query Account --output text)
aws s3 ls "s3://pyspark-stack-artifacts-$ACCT/emr/logs/" --recursive | tail
aws logs tail /aws/emr-serverless/pyspark-stack --since 1h
```

- **Spark UI** → la consola de **EMR Serverless** reconstruye la UI de Spark de cada corrida
  terminada (reemplaza al History Server local, que ya no aplica).
- **Estado del job desde el DAG** → el `EmrServerlessJobSensor`/Operator falla si el job termina en
  `FAILED`, y eso dispara la alerta `EmrServerlessJobFailed` (§12.4).

---

## 13. Hardening de producción

Ajustes exigibles antes de considerar el stack production-ready. Todos los fragmentos de esta sección son copiables tal cual.

### 13.1 Secretos y parámetros con AWS (Parameter Store + Secrets Manager)

`docker-compose.prod.yml` trae los secretos parametrizados con defaults débiles de dev
(`${POSTGRES_PASSWORD:-airflow}`, JWT `change-me-in-prod`, admin/admin): sin un
`.env` fuerte, en producción quedarían las credenciales por defecto. En vez de mantener ese `.env`
a mano en texto plano, los valores se generan y guardan en AWS SSM Parameter Store (SecureString,
cifrado con KMS); la EC2 los lee con su rol IAM y los materializa en un `.env` efímero (chmod 600)
antes de `docker compose up`. De esta forma ningún secreto queda versionado en git.

> SSM Parameter Store vs Secrets Manager: Parameter Store SecureString es gratis (tier estándar)
> y alcanza para esto. Secrets Manager (~$0.40/secreto/mes) suma rotación automática; si la
> necesitás, cambiá `aws_ssm_parameter` por `aws_secretsmanager_secret`.

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
  length  = 20
  # special = false: el compose interpola la password sin comillas en el `bash -c` de
  # airflow-init; caracteres como ( ) & * romperían el comando en silencio (admin no creado).
  special = false
}
resource "random_password" "grafana" {
  length  = 20
  special = false # se interpola en env del compose; sin especiales no hay sorpresas de quoting
}

locals {
  secrets = {
    postgres_password      = random_password.postgres.result
    airflow_jwt_secret     = random_password.jwt.result
    airflow_admin_password = random_password.admin.result
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

<details>
<summary>🖱️ A mano en la consola AWS — secretos en SSM / Secrets Manager</summary>

1. Generá los valores en tu máquina (`openssl rand -hex 24` para passwords, `-hex 32` para el JWT).
2. **Systems Manager → Parameter Store → Create parameter** ×4, *Type* **SecureString** (KMS key
   default `aws/ssm`), con estos nombres exactos (son los que lee `load-secrets.sh`):
   `/pyspark-stack/postgres_password` · `/pyspark-stack/airflow_jwt_secret` ·
   `/pyspark-stack/airflow_admin_password` · `/pyspark-stack/grafana_admin_password`.
3. El SMTP de Alertmanager va aparte (nunca lo genera Terraform):
   `/pyspark-stack/smtp_password` (SecureString) con el *app password* de Gmail.
4. (Opcional, rotación) **Secrets Manager → Store a new secret** → *Other type of secret* →
   nombre `pyspark-stack/airflow_jwt_secret` → valor → *Next* hasta crear.
5. `load-secrets.sh` (abajo) funciona igual con parámetros creados a mano o por Terraform: solo
   importan los nombres.

</details>

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

Aplicá y comprobá que los parámetros existen antes de seguir:

```bash
terraform -chdir=infra/prod apply
# Debe imprimir los primeros 8 caracteres del password (algo, no un error):
aws ssm get-parameter --name /pyspark-stack/postgres_password --with-decryption \
  --query Parameter.Value --output text | head -c 8
```

**Script que materializa el `.env` desde SSM — `scripts/load-secrets.sh` (corre en la EC2).**
Creá primero la carpeta con `mkdir -p scripts` y después guardá ahí el siguiente contenido:

```bash
#!/usr/bin/env bash
# Genera un .env efímero desde SSM antes de levantar el stack.
set -euo pipefail
PREFIX="/pyspark-stack"   # si cambiaste var.name_prefix, ajustá este prefijo (el script no lee variables de Terraform)
REGION="${AWS_REGION:-us-east-1}"
get() { aws ssm get-parameter --name "$PREFIX/$1" --with-decryption \
          --query Parameter.Value --output text --region "$REGION"; }

# EMR Serverless (NO son secretos): app id + ARN del rol de ejecución, para las Airflow Variables
# ({{ var.value.emr_app_id }} / {{ var.value.emr_job_role_arn }}, §9.0). Se derivan con el rol de
# la EC2 desde la API de AWS (equivalen a los outputs emr_app_id / emr_job_role_arn de §6.4).
ACCT=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
EMR_APP_ID=$(aws emr-serverless list-applications --region "$REGION" \
  --query "applications[?name=='pyspark-stack-spark'].id | [0]" --output text)
EMR_JOB_ROLE_ARN="arn:aws:iam::${ACCT}:role/pyspark-stack-emr-serverless-job"
# Nombres de bucket → Airflow Variables datalake/artifacts que leen los DAGs EMR (§10.2).
DATALAKE_BUCKET="pyspark-stack-datalake-${ACCT}"
ARTIFACTS_BUCKET="pyspark-stack-artifacts-${ACCT}"

cat > .env <<EOF
POSTGRES_USER=airflow
POSTGRES_DB=airflow
POSTGRES_PASSWORD=$(get postgres_password)
AIRFLOW_JWT_SECRET=$(get airflow_jwt_secret)
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$(get airflow_admin_password)
GRAFANA_ADMIN_PASSWORD=$(get grafana_admin_password)
EMR_APP_ID=${EMR_APP_ID}
EMR_JOB_ROLE_ARN=${EMR_JOB_ROLE_ARN}
DATALAKE_BUCKET=${DATALAKE_BUCKET}
ARTIFACTS_BUCKET=${ARTIFACTS_BUCKET}
EOF
chmod 600 .env
echo ".env generado desde SSM (+ EMR app id / job role arn + buckets datalake/artifacts)"
```

Tras crearlo: `chmod +x scripts/load-secrets.sh`.

Uso en la EC2: `./scripts/load-secrets.sh && docker compose -f docker-compose.prod.yml up -d`.

**El compose base ya lee esas variables** (con defaults de dev que el `.env` generado pisa):

```yaml
# docker-compose.yml (fragmentos, tal como está)
  airflow-db:
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}
      - POSTGRES_DB=${POSTGRES_DB:-airflow}

x-airflow-common: &airflow-common
  environment: &airflow-common-env
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@airflow-db:5432/${POSTGRES_DB:-airflow}
    AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW_JWT_SECRET:-change-me-in-prod}'
```

**Valores delicados / claves con rotación → AWS Secrets Manager.** Para lo más sensible (el JWT
secret de Airflow, credenciales de bases externas, API keys de terceros) usá Secrets Manager,
que agrega rotación automática y auditoría. Parameter Store queda para el resto (gratis).

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

Si migrás el JWT a Secrets Manager, quitá `airflow_jwt_secret` del `for_each` de SSM y cambiá
`load-secrets.sh` — si no, el script seguirá leyendo el valor (potencialmente viejo) de SSM.

**Cuándo usar cada uno:**

| Servicio | Usar para | Costo |
|---|---|---|
| **Parameter Store** (SecureString) | passwords operacionales, tokens, config | gratis (tier estándar) |
| **Secrets Manager** | claves delicadas con **rotación**, credenciales de terceros | ~$0.40/secreto/mes |

**Alertmanager (SMTP password):** guardalo en Secrets Manager (o SSM) y renderizá el
`alertmanager.yml` en el arranque con `envsubst` tomando el valor con
`aws secretsmanager get-secret-value ...`, así el password nunca vive en un archivo versionado.

> Airflow también puede leer sus *Connections* y *Variables* directo de Parameter Store con el
> `SystemsManagerParameterStoreBackend` (provider `apache-airflow-providers-amazon`), sin
> guardarlas en la metadata DB: `AIRFLOW__SECRETS__BACKEND` +
> `AIRFLOW__SECRETS__BACKEND_KWARGS` apuntando al prefijo `/pyspark-stack/`.

### 13.2 restart + límites + logging en los servicios del stack

El único servicio core que queda en la EC2 es `airflow-db` (Postgres). A esta altura ya tiene
`restart: unless-stopped`, límite de memoria (512m) y rotación de logs desde el Paso 0 (§5.5) — no
hay nada que agregar acá, esto es solo la recapitulación de por qué. (Los `hdfs-*` y `spark-*` ya
**no existen** en `docker-compose.prod.yml`: Spark corre en EMR Serverless y HDFS se eliminó.)

> **Jupyter no existe en `docker-compose.prod.yml`.** No se usa en prod (§5.5): el ETL corre por
> Airflow (que dispara EMR Serverless) y los `.ipynb` por papermill (headless, sin UI interactiva).
> Para explorar datos a mano, usá el stack local (`docs/01`).

`restart: unless-stopped` es lo que hace que, al prender la EC2 (auto start/stop), el stack
vuelva solo: Docker arranca en boot (`systemctl enable docker` del `user_data`) y reinicia los
contenedores que estaban corriendo. Los límites están calibrados para `t3.large` (8 GB): sin Spark
en la EC2, la RAM la usan Airflow + Postgres + monitoreo, que entran con holgura y dejan margen al SO.

> Los límites de los servicios de monitoreo (Prometheus 1g, etc.) van en sus propios bloques
> (§12.3/§14.1). El resto de la RAM queda para los `airflow-*` (scheduler, apiserver, etc.), que en
> una `t3.large` corren cómodos ahora que ningún job Spark compite por la memoria del host.

### 13.3 Spark ↔ S3 con el rol IAM (s3a) en EMR Serverless

Con Spark en **EMR Serverless** ya no hay caja donde montar un `spark-defaults.conf`: el runtime de
EMR (release `emr-7.5.0`) trae los conectores S3 y, por defecto, resuelve `s3a://` con el **rol de
ejecución del job** (§6.4) — sin keys ni credential provider que configurar a mano. Las credenciales
son las del rol que la EC2 le pasa a EMR con `iam:PassRole`; el rol de la EC2 ya **no** es el que
usa Spark (solo lo usan las tasks Python puro de Airflow para `s3://`).

Si un job necesita config S3 particular (region explícita, committer, etc.), va **por-job** en los
`sparkSubmitParameters` del `StartJobRun` (§9.0), no en un archivo local:

```text
--conf spark.hadoop.fs.s3a.endpoint.region=us-east-1
--conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
```

> El `spark-conf/spark-defaults.conf` con `InstanceProfileCredentialsProvider` **sigue existiendo
> solo para el stack local** (`docs/01`), donde Spark corre en tu máquina con el
> instance profile / tus keys. En prod no se monta en ningún contenedor: la EC2 no corre Spark.

### 13.4 `docker.sock` en Airflow

`docker-compose.prod.yml` (§14.1) **no** monta `/var/run/docker.sock` en los `airflow-*` — a
diferencia del compose de dev local, que sí lo trae (por si algún DAG usa `DockerOperator`). Darles
ese socket a los contenedores equivale a acceso root al host, y ningún DAG de este stack usa
`DockerOperator` (los jobs van a EMR Serverless, no a contenedores locales), así que en prod
directamente se omite. Si en algún momento lo necesitás, no montes el socket crudo: poné un
socket-proxy (`tecnativa/docker-socket-proxy`) read-only delante.

### 13.5 Higiene del repo

El `.gitignore` de la raíz ya cubre todo lo sensible. Estas son las reglas que debe conservar:

```gitignore
# .gitignore (raíz) — fragmentos clave.
# Ojo: gitignore solo admite `#` al PRINCIPIO de la línea. Un comentario al final se interpreta
# como parte del patrón y la regla deja de matchear.

# Secretos locales
.env
*.pem
*.key
monitoring/alertmanager/alertmanager.yml

# Terraform: estado y variables. El lock SÍ se versiona (pinea las versiones de los providers).
**/.terraform/*
*.tfstate
*.tfstate.*
*.tfvars
!*.tfvars.example

# Lambdas empaquetadas
**/lambda/*.zip

# Event logs de Spark (se conserva la config)
spark-events/*
!spark-events/spark-defaults.conf

# Salidas de papermill (§9.3)
notebooks/**/output/
spark-apps/notebook-output/
```

El `.env.example` (sin valores reales) y el `README.md` raíz (que enlaza estas guías y lista los
modos de arranque) ya existen en el repo — mantenelos al día si agregás variables nuevas.

### 13.6 UI de jobs terminados (Spark UI de EMR Serverless)

Ya **no hace falta un Spark History Server en la EC2**: con Spark en EMR Serverless, AWS reconstruye
la **Spark UI de cada corrida terminada** desde la consola de EMR Serverless (application → job run
→ *Spark UI*), sin montar `spark-events` ni correr `Dockerfile.history` en prod. Es clave para
depurar un ETL que ya corrió (DAG de stages, tareas, spill, tiempos) y sale gratis con el servicio.

Para el post-mortem también tenés los logs del job en `s3://<artifacts>/emr/logs/` y en CloudWatch
Logs (`/aws/emr-serverless/pyspark-stack`), más las métricas en CloudWatch (§12.8).

> El History Server local (`Dockerfile.history`, `spark.eventLog.*` en `spark-conf/`) queda
> **solo para el stack local** (`docs/01`), donde Spark corre en tu máquina. En
> prod no se levanta.

### 13.7 Checklist final (production-ready)

- [ ] `.env` generado desde SSM con `load-secrets.sh` (`openssl rand` solo en el camino manual por consola), fuera de git.
- [ ] Grafana sin password default (`GRAFANA_ADMIN_PASSWORD` desde SSM, §13.1).
- [ ] `restart: unless-stopped` + límites de memoria en todos los servicios.
- [ ] Rotación de logs (`max-size`) en todos.
- [ ] EMR Serverless: app creada, rol de ejecución scopeado a los buckets, entrypoints en
      `s3://<artifacts>/emr/`; la EC2 puede `StartJobRun` + `PassRole` (§6.4).
- [ ] `docker.sock` quitado o detrás de proxy.
- [ ] Monitoreo: targets UP, Alertmanager con SMTP real, Loki recibiendo logs; alerta de fallo de
      job EMR activa (§12.4).
- [ ] Backups: snapshot del EBS `/data` (DLM) + versioning de S3.
- [ ] Imágenes pineadas por tag; `terraform apply` solo manual/local.
- [ ] DLQ en las 2 Lambdas + alarma sobre la cola; AWS Budgets, Cost Anomaly Detection y Access
      Analyzer aplicados, con `alert_email` real (§18).

---

## 14. Archivos compose completos

Los dos archivos listos para copiar. El de producción ya incorpora todo lo explicado en las
secciones de monitoreo y hardening: persistencia en `/data`, `restart`, límites, logging, métricas
y logs. **Ya no lleva HDFS ni Spark**: el cómputo Spark corre en EMR Serverless (§6.4), así que la
caja solo tiene Airflow + Postgres + monitoreo. Las variables `${...}` las provee el `.env` que
genera `load-secrets.sh` desde SSM (incluidos `EMR_APP_ID` / `EMR_JOB_ROLE_ARN`, que alimentan las
Airflow Variables de los DAGs).

### 14.1 docker-compose.prod.yml (producción, completo)

> Este archivo no arranca de cero acá: se creó ya en el Paso 0 (§5.5) con Airflow + Postgres, sin
> Spark/HDFS/Jupyter. Lo de abajo es la versión completa, con todo lo que las secciones 9-13 le
> fueron sumando (env de EMR Serverless, monitoreo, hardening). **Reemplazá tu archivo local por
> este entero** — es standalone (un solo `-f`, sin merge), no lo concatenes a mano.

```yaml
# docker-compose.prod.yml — stack de PRODUCCIÓN, standalone (un solo archivo, sin merge, sin
# Spark/HDFS/Jupyter: Spark corre en EMR Serverless, §6.4; Jupyter no se usa en prod, §5.5).
#   ./scripts/load-secrets.sh   # genera .env desde SSM (§13.1)
#   docker compose -f docker-compose.prod.yml up -d --build
x-airflow-common: &airflow-common
  image: pyspark_stack-airflow:3.2.2
  build:
    context: .
    dockerfile: Dockerfile.airflow
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:?definí POSTGRES_PASSWORD (scripts/load-secrets.sh)}@airflow-db:5432/${POSTGRES_DB:-airflow}
    AIRFLOW__CORE__LOAD_EXAMPLES: 'False'
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
    AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW_JWT_SECRET:?definí AIRFLOW_JWT_SECRET (scripts/load-secrets.sh)}'
    AIRFLOW_UID: 50000
    # Métricas StatsD (§12.5):
    AIRFLOW__METRICS__STATSD_ON: "True"
    AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
    AIRFLOW__METRICS__STATSD_PORT: "9125"
    AIRFLOW__METRICS__STATSD_PREFIX: airflow
    # EMR Serverless: expone los outputs de Terraform como Airflow Variables ({{ var.value.emr_app_id }} /
    # {{ var.value.emr_job_role_arn }}, §9.0). Completá con `terraform output` tras el apply de §6.4.
    AIRFLOW_VAR_EMR_APP_ID: "${EMR_APP_ID}"
    AIRFLOW_VAR_EMR_JOB_ROLE_ARN: "${EMR_JOB_ROLE_ARN}"
    # Buckets como Airflow Variables datalake/artifacts: los DAGs EMR reales arman con ellas el
    # entryPoint (s3://<artifacts>/emr/customer_etl.py) y los args ([datalake, "{{ ds }}"]) (§10.2).
    AIRFLOW_VAR_DATALAKE: "${DATALAKE_BUCKET}"
    AIRFLOW_VAR_ARTIFACTS: "${ARTIFACTS_BUCKET}"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "False"
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: "30" # detecta archivos DAG nuevos en ~30s (§10)
  volumes:
    - ./dags:/opt/airflow/dags
  restart: unless-stopped
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }
  networks:
    - hadoopnet

x-mon-logging: &mon-logging
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }

services:
  # ---- Persistencia en /data (EBS) + restart/límites/logging (calibrado a t3.large 8 GB) ----
  # Sin HDFS ni Spark en la caja: Spark corre en EMR Serverless (§6.4). El único servicio con
  # estado local es Postgres; el resto es Airflow + monitoreo.
  airflow-db:
    image: postgres:16
    container_name: airflow-db
    restart: unless-stopped
    <<: *mon-logging
    deploy: { resources: { limits: { memory: 512m } } }
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:?definí POSTGRES_PASSWORD (scripts/load-secrets.sh)}
      - POSTGRES_DB=${POSTGRES_DB:-airflow}
    volumes:
      - /data/postgres:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER:-airflow}"]
      interval: 5s
      timeout: 5s
      retries: 10
    networks:
      - hadoopnet

  airflow-init:
    <<: *airflow-common
    container_name: airflow-init
    restart: "no"
    depends_on:
      airflow-db: { condition: service_healthy }
    command: >
      bash -c "
        airflow db migrate &&
        airflow fab-db migrate &&
        airflow users create --username ${AIRFLOW_ADMIN_USER:-admin} --firstname Admin --lastname User --role Admin --email admin@example.com --password ${AIRFLOW_ADMIN_PASSWORD:?definí AIRFLOW_ADMIN_PASSWORD (scripts/load-secrets.sh)} || true"

  # NOTA: exponer la web por HTTPS/443 edita este bloque directamente (cert + puerto 443 + alias
  # de red + EXECUTION_API_SERVER_URL en https) — ver §5.6. Sin eso, queda como acá: solo por 8082.
  airflow-apiserver:
    <<: *airflow-common
    container_name: airflow-apiserver
    command: api-server
    ports:
      - "8082:8080"
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }

  # El scheduler EJECUTA las tasks (LocalExecutor): dispara EMR Serverless (§9.0) y corre papermill.
  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks   # papermill lee los .ipynb (§9.1)
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

  # ==================== MONITOREO (detalle en §12) ====================
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
      - /var/run/docker.sock:/var/run/docker.sock:ro   # promtail SÍ necesita leer logs de contenedores (§13.4)
    networks: [hadoopnet]

networks:
  hadoopnet:
```


---

## 15. Puesta en producción — runbook final

La secuencia completa que engrana todo lo anterior, de cero a producción. Diez pasos, en orden;
si uno falla, resolvelo antes de seguir.

**Cada bloque dice dónde corre.** `[LOCAL]` es tu máquina, parado en la raíz del repo. `[EC2]` es
dentro de la instancia — cada bloque `[EC2]` arranca con su `ssh`, así que podés copiarlos sueltos
sin arrastrar contexto. Los pasos 1, 2, 7 y 10 son locales; del 3 al 6 y el 8 corren en la EC2; el 9 se mira
desde tu máquina por el túnel.

**Paso 1 — Aplicar la infra completa** `[LOCAL]` *(~5-8 min: incluye la app EMR)*. Antes de correrlo, `secrets.tf` y `emr.tf` (app EMR Serverless +
rol del job + extensión del rol EC2, §6.4) tienen que estar escritos:

```bash
terraform -chdir=infra/prod apply

# Verificar SSM y la app EMR antes de seguir:
aws ssm get-parameter --name /pyspark-stack/postgres_password --with-decryption \
  --query Parameter.Value --output text | head -c 8    # imprime 8 caracteres, no un error
terraform -chdir=infra/prod output emr_app_id          # `output` acepta UN nombre por vez
terraform -chdir=infra/prod output emr_job_role_arn
aws emr-serverless list-applications --query 'applications[?name==`pyspark-stack-spark`].[id,state]'
```

**Paso 2 — Subir el proyecto y publicar los entrypoints** `[LOCAL]`. Usá el `rsync` completo de §5.5, que incluye `scripts/`, `monitoring/` y los compose (`deploy.sh`
**no** los sube):

```bash
IP=$(terraform -chdir=infra/prod output -raw public_ip)
ACCT=$(aws sts get-caller-identity --query Account --output text)

rsync -avz --exclude '.git' --exclude 'infra' --exclude '.env' --exclude '__pycache__' \
  -e "ssh -i ~/.ssh/pyspark_stack" ./ ec2-user@$IP:/home/ec2-user/pyspark_stack/

# EMR Serverless toma los entrypoints de S3, no de la EC2:
aws s3 sync spark-apps/emr/ "s3://pyspark-stack-artifacts-$ACCT/emr/" --exclude '__pycache__/*'
```

**Paso 3 — Crear `alertmanager.yml`** `[EC2]`, con el app password real de Gmail (§12.4). Atención a un detalle: el `rsync` del paso 2 copia tu árbol de trabajo y **no respeta `.gitignore`**, así que
si ya lo creaste local, el archivo —con el password— ya viajó. Verificá antes de reescribirlo:

```bash
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'cd pyspark_stack && ls -l monitoring/alertmanager/alertmanager.yml'
```

Si no existe, crealo con el contenido de §12.4. **No lo dejes sin crear**: al montar un bind mount
sobre una ruta inexistente Docker crea un *directorio*, y Alertmanager arranca en crash-loop. El
`config --quiet` del paso 5 no lo detecta.

**Paso 4 — Generar el `.env` desde SSM y verificarlo** `[EC2]`:

```bash
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP 'cd pyspark_stack && \
  ./scripts/load-secrets.sh && \
  wc -l .env && ls -l .env && \
  grep -E "EMR_APP_ID|EMR_JOB_ROLE_ARN|DATALAKE_BUCKET|ARTIFACTS_BUCKET" .env'
```

El archivo debe tener **11 líneas** (7 base + los 4 derivados), permisos `-rw-------` y los cuatro
derivados **con valor**. Dos trampas: si alguno dice `None`, la app EMR no existe o está en otra región —el
script no falla, escribe `None` y el error aparece recién en el primer `StartJobRun`—; y si exponés la
web por HTTPS (§5.6) las líneas son 12, porque se suma `AIRFLOW_DOMAIN`. Esa línea se agrega a mano
y `load-secrets.sh` **reescribe el `.env` entero** en cada corrida, así que hay que volver a
agregarla después de cada ejecución del script.

**Paso 5 — Validar el merge de los compose antes de levantar** `[EC2]`:

```bash
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP 'cd pyspark_stack && \
  docker compose -f docker-compose.prod.yml config --quiet'
```

Sin salida = OK.

**Paso 6 — Levantar el stack completo** `[EC2]` *(la primera vez tarda varios minutos: compila)*:

```bash
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP 'cd pyspark_stack && \
  docker compose -f docker-compose.prod.yml up -d --build'
```

Si en cambio corrieras `docker compose -f docker-compose.yml up -d` (el compose de **dev local**,
por error), levantarías Spark standalone + HDFS en la EC2 y Postgres arrancaría contra un volumen
Docker nuevo en vez de `/data/postgres` (EBS): usá siempre `docker-compose.prod.yml`, nunca el de dev.

**Paso 6b — (opcional) Exponer la web por HTTPS.** Solo si definiste `airflow_domain`: seguí §5.6
—emitir el cert, agregar `AIRFLOW_DOMAIN` al `.env`, aplicar el delta TLS del compose—. Sin esto,
Airflow queda accesible solo por el túnel 8082. La §17 asume esta web.

**Paso 7 — Re-correr los smoke tests de §8.1, capas 2 a 5** `[LOCAL]`. Ahora sí pasa la capa 5:
el monitoreo está arriba. Tené presente que esas capas corren **desde tu máquina** (usan `nc`, `$S` y la CLI
con tus credenciales), no dentro de la EC2.

**Paso 8 — Probar una alerta real de punta a punta** `[EC2]`:

```bash
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP 'docker stop node-exporter'   # ~3 min → email TargetDown
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP 'docker start node-exporter'  # y después el "resolved"
```

**Paso 9 — Confirmar el pipeline en modo producción** `[LOCAL]`, por el túnel. El cron `daily-etl`
(§7.2) dispara solo dentro de la ventana de encendido; el DAG lanza el job en EMR Serverless y
espera su fin. En Prometheus (`localhost:9090`) consultá `airflow_dagrun_duration_success_count > 0`; esa es la
métrica sobre la que se arma el dead-man switch `DailyEtlMissing` (§12.4). La corrida y sus logs los ves en la consola
de EMR Serverless y en `s3://<artifacts>/emr/logs/`.

**Paso 10 — La prueba final: el ciclo de ahorro completo** `[LOCAL]`:

```bash
aws ec2 stop-instances --instance-ids $(terraform -chdir=infra/prod output -raw instance_id)
aws lambda invoke --function-name pyspark-stack-startstop \
  --cli-binary-format raw-in-base64-out --payload '{"action":"start"}' /dev/stdout
```

Confirmá que **todo vuelve solo** tras el arranque: `docker compose ps` con todos Up, los targets
de Prometheus UP, y el DAG siguiente corriendo por su schedule. Si `/data` no montó, acá se nota
—Postgres y Prometheus quedan en crash-loop— y el diagnóstico está en §5.3.

Con esto el stack queda en producción: infra reproducible, secretos gestionados, deploy por
push, monitoreo con alertas y costo optimizado.

---

## 16. Athena — capa de consumo SQL/BI (opcional)

**Opcional y marginal en costo** — no cambia los totales canónicos (~$35/mes con start/stop,
~$83/mes 24/7 de §2). Athena consulta el data lake **con SQL puro, sin prender Spark ni un cluster**:
paga solo por dato escaneado y escala a cero. Sirve para tres cosas concretas a esta escala:

- Consultar `s3://<datalake>/analytics/` (y `curated/`) con SQL ad-hoc, sin levantar un job.
- **BI**: enchufar QuickSight / Grafana / Metabase directo al lake, sin ETL extra a una base.
- **Asserts de calidad** dentro de un DAG (un `SELECT count(*)` post-ETL barato, §16.4).

### 16.1 Tablas Iceberg: ACID, time travel y `MERGE` desde SQL, sin crawler

Las tablas de `curated/`/`analytics/` son **Apache Iceberg** (arquitectura: `docs/03` §7), no
Parquet suelto con partition projection. La diferencia práctica: Iceberg guarda su propia
metadata (manifests + snapshots) en S3 y la registra en el **mismo Glue Data Catalog** que ya usa
este workgroup — así que una tabla que el job Spark crea/escribe (§6.4/§10.2) aparece automáticamente
acá, consultable **y escribible** por SQL, sin crawler y sin declarar particiones a mano.

**Si el job Spark ya creó la tabla** (caso normal — ver el `CREATE TABLE ... USING iceberg` de
§10.2), no hay que declarar nada en Athena: aparece sola en `pyspark_stack_analytics.ventas` apenas
Spark hace el primer `INSERT`/`MERGE`. **Si preferís crearla primero desde Athena** (por ejemplo para
prototipar sin correr un job EMR todavía), el DDL es:

```sql
CREATE TABLE pyspark_stack_analytics.ventas (
  pais  string,
  monto double,
  dt    string
)
PARTITIONED BY (dt)
LOCATION 's3://pyspark-stack-datalake-<acct>/curated/ventas/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');
```

> `PARTITIONED BY (dt)` acá es partición **oculta** de Iceberg (*hidden partitioning*), no una regla
> de projection: Iceberg reescribe el filtro `WHERE dt = '...'` contra sus manifests solo, sin que
> vos tengas que mantener rangos de fecha ni `storage.location.template` a mano — a diferencia de la
> partition projection de Hive/Parquet plano, que si tenías antes.

**Lo que gana esta tabla frente al Parquet suelto anterior** — todo disponible desde Athena, no solo
desde Spark:

```sql
-- Time travel: la tabla como estaba hace 3 versiones (auditoría, "qué pasó con esta fila ayer")
SELECT * FROM pyspark_stack_analytics.ventas FOR VERSION AS OF 3 WHERE pais = 'PE';

-- Historial de snapshots (para elegir la versión de arriba)
SELECT * FROM "pyspark_stack_analytics"."ventas$history";

-- Upsert incremental desde SQL — reemplaza el `overwrite` completo del Parquet suelto
MERGE INTO pyspark_stack_analytics.ventas t
USING (VALUES ('PE', 120.50, '2026-07-16')) AS s(pais, monto, dt)
ON t.pais = s.pais AND t.dt = s.dt
WHEN MATCHED THEN UPDATE SET monto = s.monto
WHEN NOT MATCHED THEN INSERT (pais, monto, dt) VALUES (s.pais, s.monto, s.dt);
```

> `MERGE`/`UPDATE`/`DELETE` desde Athena requieren **Athena engine version 3** (el default desde
> 2023) y que el workgroup lo tenga configurado así — el Terraform de §16.2 ya lo fija. Es
> justamente lo que Delta Lake **no** puede hacer vía Athena (solo lectura) y fue el motivo de elegir
> Iceberg en vez de Delta para este stack.

### 16.1b Mantenimiento: compactación y expiración de snapshots (auditoría §2.1)

**El problema que resuelve:** cada `MERGE`/`INSERT` agrega un snapshot y, casi siempre, varios
archivos de datos chicos. Sin mantenimiento periódico, después de meses de corridas 3x/semana
tenés cientos de manifests y archivos pequeños — el *planning time* de cada query en Athena/Spark
crece (hay que leer más metadata antes de leer un solo dato), y los snapshots viejos nunca se
liberan. A este volumen no es un problema de costo grande, es un problema de **latencia que se
degrada sola, sin que nadie lo note hasta que ya molesta**.

**La solución, sin levantar Spark aparte:** desde Athena engine v3, `OPTIMIZE`/`VACUUM` operan
directo sobre las tablas Iceberg:

```sql
-- Compacta archivos chicos en archivos más grandes (mejora el tiempo de lectura)
OPTIMIZE pyspark_stack_analytics.ventas REWRITE DATA USING BIN_PACK;

-- Libera snapshots y archivos de datos ya no referenciados, más viejos que la retención por defecto
VACUUM pyspark_stack_analytics.ventas;
```

> Verificá la sintaxis exacta y la disponibilidad contra la versión de Athena engine que tengas
> activa — el soporte de mantenimiento de Iceberg se fue agregando de forma incremental. Si tu
> engine no lo tiene, el equivalente corre desde Spark con los *stored procedures* de Iceberg:
> `CALL glue_catalog.system.rewrite_data_files('pyspark_stack_analytics.ventas')`,
> `expire_snapshots(...)` y `remove_orphan_files(...)` — mismo catálogo `glue_catalog` de §6.4.

**Task de Airflow, semanal** (housekeeping, no parte del pipeline crítico — no hace falta correrlo
en cada ETL):

```python
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

with DAG(
    "iceberg_maintenance",
    schedule="0 17 * * FRI",   # viernes 17:00 UTC — dentro de la ventana de encendido L-V (§5.4)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    # Una task por tabla Iceberg que tengas — este ejemplo cubre solo `ventas`.
    compactar = AthenaOperator(
        task_id="compactar_ventas",
        query="OPTIMIZE pyspark_stack_analytics.ventas REWRITE DATA USING BIN_PACK",
        database="pyspark_stack_analytics",
        output_location="s3://{{ var.value.artifacts }}/athena-results/",
        workgroup="pyspark-stack-analytics",
    )
    vacuum = AthenaOperator(
        task_id="vacuum_ventas",
        query="VACUUM pyspark_stack_analytics.ventas",
        database="pyspark_stack_analytics",
        output_location="s3://{{ var.value.artifacts }}/athena-results/",
        workgroup="pyspark-stack-analytics",
    )
    compactar >> vacuum   # VACUUM después de compactar: no tiene sentido liberar antes de reescribir
```

> **Por qué viernes y no domingo.** El auto start/stop (§5.4) solo enciende la EC2 L-V. Si programás
> el mantenimiento en fin de semana, la EC2 va a estar apagada y el disparo se pierde en silencio
> (mismo caso ya cubierto por `DailyEtlMissing`, pero para un DAG que nadie mira todos los días).
> Metelo un viernes, dentro de la ventana ya encendida, y te ahorrás la duda.

**Verificación:**

```bash
# Antes/después: cantidad de archivos de datos (menos archivos, más grandes = compactación funcionó)
aws s3 ls "s3://pyspark-stack-datalake-<acct>/curated/ventas/data/" --recursive --summarize | tail -3
```

```sql
-- Snapshots vivos: debería bajar después del VACUUM, sin perder el rango de time-travel que necesitás
SELECT count(*) FROM "pyspark_stack_analytics"."ventas$snapshots";
```

### 16.2 Terraform mínimo — workgroup + resultados

Se reusa el bucket de **artifacts** con el prefijo `athena-results/` (podés usar uno nuevo si
preferís aislar). `enforce_workgroup_configuration=true` obliga a que toda consulta use este bucket
cifrado; los resultados son descartables → expiran a los 7 días.

```hcl
# infra/prod/athena.tf
resource "aws_athena_workgroup" "analytics" {
  name = "${var.name_prefix}-analytics"
  configuration {
    enforce_workgroup_configuration    = true   # obliga a usar ESTA config (bucket + cifrado)
    publish_cloudwatch_metrics_enabled = true
    # Engine v3 (default desde 2023, pero explícito acá): sin esto MERGE/UPDATE/DELETE sobre
    # tablas Iceberg (§16.1) fallan con "not supported" en workgroups viejos migrados de v2.
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    # Guardrail de costo (auditoría §2.2): a $5/TB, un SELECT * sin filtro de partición sobre una
    # tabla que creció puede ser $5-20 en una sola query. 5 GB de techo por query es generoso a
    # este volumen (2-5 GB/día TOTAL) — cualquier query legítima entra cómoda; una mal escrita corta
    # antes de convertirse en sorpresa de factura. Subilo si el volumen real crece.
    bytes_scanned_cutoff_per_query = 5368709120   # 5 GiB
    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}

# Los resultados de consultas son descartables → expiran a los 7 días (mismo bucket artifacts).
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "athena-results-expire"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
}

# Database AISLADA para dbt Slim CI (§11.2): las tablas que un PR materializa al testear
# `state:modified+` NO tocan pyspark_stack_analytics (producción) — quedan acá, en su propio
# prefijo S3, y se auto-expiran: un PR no limpia después de sí mismo, así que lo hace el lifecycle.
resource "aws_glue_catalog_database" "analytics_ci" {
  name = "${replace(var.name_prefix, "-", "_")}_analytics_ci"
}
resource "aws_s3_bucket_lifecycle_configuration" "dbt_ci" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "dbt-ci-expire"
    status = "Enabled"
    filter { prefix = "dbt-ci/" }
    expiration { days = 3 }   # sobra: un PR viejo que nadie mergeó no necesita sus tablas de prueba
  }
}
```

> `aws_glue_catalog_database.analytics` **no** se declara acá: ya se creó en §6.4 junto con el rol de
> ejecución EMR, porque las tablas Iceberg lo necesitan exista o no exista esta sección de Athena
> (es el catálogo que usa Spark, no algo exclusivo de Athena). Si saltaste directo a §16 sin pasar
> por §6.4, volvé — ese recurso es un prerrequisito.

<details>
<summary>🖱️ A mano en la consola AWS — workgroup de Athena + base Glue + lifecycle</summary>

1. **Athena → Administration → Workgroups → Create workgroup**: nombre
   `pyspark-stack-analytics` · *Query result location*
   `s3://pyspark-stack-artifacts-<account-id>/athena-results/` · tildá **Override client-side
   settings** (es el equivalente de `enforce_workgroup_configuration = true`: obliga a toda
   consulta a usar este bucket y este cifrado) · *Encrypt query results* **SSE-S3**.
2. **AWS Glue → Data catalog → Databases → Add database**: nombre `pyspark_stack_analytics`.
   Glue no admite guiones: el `-` del prefijo va como `_`.
3. **S3 → bucket `artifacts` → Management → Create lifecycle rule**: nombre `athena-results`,
   *Prefix* `athena-results/`, acción **Expire current versions** a los **7 días**. Los resultados
   de consulta son descartables; sin esta regla se acumulan y pagás storage indefinidamente.
4. Verificá corriendo el DDL de §16.1 desde **Athena → Query editor**, con el workgroup del paso 1
   seleccionado arriba a la derecha (si dejás `primary`, la consulta escribe en otro bucket).

</details>

### 16.3 IAM — permitir que un DAG consulte (rol de la EC2)

Para que un `AthenaOperator` (en un DAG, corriendo bajo el rol de la EC2) ejecute queries:

```hcl
# infra/prod/iam.tf  (Athena para el rol de la EC2)
data "aws_iam_policy_document" "ec2_athena" {
  statement {
    sid     = "AthenaQuery"
    actions = ["athena:StartQueryExecution", "athena:GetQueryExecution",
               "athena:GetQueryResults", "athena:StopQueryExecution"]
    resources = ["arn:aws:athena:${local.region}:${local.account_id}:workgroup/${var.name_prefix}-analytics"]
  }
  statement {   # el catálogo Glue no admite ARN fino para estas lecturas
    sid       = "GlueCatalogRead"
    actions   = ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"]
    resources = ["*"]
  }
  statement {   # leer los datos del lake que Athena escanea
    sid       = "AthenaDataRead"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.datalake.arn, "${aws_s3_bucket.datalake.arn}/*"]
  }
  statement {   # escribir/leer los resultados de la consulta (objetos)
    sid       = "AthenaResultsObjects"
    actions   = ["s3:GetObject", "s3:PutObject",
                 "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.artifacts.arn}/athena-results/*"]
  }
  statement {
    # Athena valida el bucket de resultados a NIVEL BUCKET, no solo el prefijo: estas tres
    # acciones no aceptan un ARN con /athena-results/*. Sin ellas la query falla con
    # "Unable to verify/create output bucket". Mismo set que la policy gestionada
    # AmazonAthenaFullAccess, menos CreateBucket (el bucket ya lo crea Terraform).
    sid       = "AthenaResultsBucket"
    actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.artifacts.arn]
  }
}
resource "aws_iam_role_policy" "ec2_athena" {
  name   = "ec2-athena"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_athena.json
}
```

### 16.4 Uso en un DAG — assert de calidad post-ETL

Después del job EMR que escribe `analytics/ventas/`, una task barata confirma que hay datos del día
(el provider `apache-airflow-providers-amazon` trae el operator):

```python
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

assert_calidad = AthenaOperator(
    task_id="assert_calidad",
    query="SELECT count(*) AS filas FROM ventas WHERE dt = '{{ ds }}'",
    database="pyspark_stack_analytics",
    output_location="s3://{{ var.value.artifacts }}/athena-results/",
    workgroup="pyspark-stack-analytics",
)
# encadenala tras el job EMR:  ... >> assert_calidad
```

### 16.5 Costo y cuándo (no) usarla

Athena cobra **~$5 por TB escaneado**, con un **mínimo de 10 MB por consulta**. A esta escala
(~2 GB/día, Parquet particionado que recorta lo escaneado) el gasto es **~$0/mes** — prácticamente
ruido frente a la EC2 y EMR. Por eso queda **fuera de los totales canónicos** de §2: es una capa
opcional que agregás si la necesitás.

| Cuándo SÍ | Cuándo NO |
|---|---|
| Consumo **SQL/BI** ad-hoc del lake sin prender Spark | Si el único consumidor del `analytics/` es el **próximo job Spark** (leelo con `s3a://` directo — Athena no aporta) |
| Dashboards (QuickSight/Grafana/Metabase) sobre `curated/`/`analytics/` | Transformaciones pesadas (joins/`groupBy` grandes) → eso es **EMR Serverless**, no Athena |
| Asserts de calidad baratos dentro de un DAG | Consultas que escanean el lake entero sin partición (costo y latencia se disparan) |

> Parquet + partition projection es lo que la hace barata: columnar (lee solo las columnas del
> `SELECT`) y particionada (escanea solo los `dt` del `WHERE`). Sobre CSV sin particionar, Athena
> escanea todo y el costo/latencia suben.

---

## 17. Airflow, 3 sabores: ejemplo + monitoreo de cada uno

Airflow es **solo el orquestador**; cada task elige su motor. Acá están los **tres** que usa este
stack, cada uno con: **(a)** el DAG mínimo, **(b)** qué infra necesita y **(c)** cómo lo monitoreás.
La regla para elegir (detalle en §9.0):

| Dato / trabajo | Sabor | Dónde corre | Detalle |
|---|---|---|---|
| Chico (<~1 GB), API, mover/validar archivos | **Python puro** | En la **EC2** (proceso del scheduler) | §17.1 |
| Mediano/grande, joins/`groupBy`, muchos archivos | **PySpark** | **EMR Serverless** (escala a cero) | §17.2 |
| Consulta SQL / BI / assert de calidad | **Athena** | **Serverless AWS** (paga por dato escaneado) | §17.3 |

**Lo común a los tres (dónde mirar primero):** la **web de Airflow** (§5.6, ahora por HTTPS). En
`Grid`/`Graph` ves cada run verde/rojo, reintentos y duración; en `Logs` el stdout de la task. Esa es
la vista de *orquestación*. Lo que cambia entre sabores es **dónde vive la telemetría del cómputo**:
en la EC2 (Prometheus/Loki) para Python puro, o en **AWS (CloudWatch)** para EMR y Athena, porque su
cómputo es *managed* y no pasa por la EC2.

> Los DAGs de abajo son **patrones** ilustrativos (no los copies tal cual a `dags/`: referencian
> buckets/rutas que tenés que reemplazar). Las Airflow Variables `datalake`, `artifacts`, `emr_app_id`
> y `emr_job_role_arn` salen de los `terraform output` cargados como env (§14.1).

### 17.1 Python puro (en la EC2)

**(a) DAG** — `PythonOperator`/TaskFlow; pandas lee de S3 y escribe curated, sin Spark
(`requirements.txt`: `pandas`, `s3fs`, `pyarrow`):

```python
# dags/small_etl_dag.py — el caso "no necesito Spark"
from datetime import datetime
import pandas as pd
from airflow.sdk import DAG, task, Variable      # Airflow 3: DAG y TaskFlow @task en airflow.sdk

with DAG("small_etl", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False,
         tags=["python"]) as dag:
    @task
    def transform(ds=None):                       # Airflow inyecta ds del context
        base = f"s3://{Variable.get('datalake')}"
        df = pd.read_csv(f"{base}/raw/ventas.csv") # s3fs + rol IAM de la EC2 (§6.2), sin keys
        out = df[df["monto"] > 0].groupby("pais")["monto"].sum().reset_index()
        out.to_parquet(f"{base}/curated/ventas_por_pais/{ds}.parquet")
    transform()
```

**(b) Infra:** ninguna extra. Corre **dentro de la EC2** en el proceso del scheduler (LocalExecutor).
Solo necesita el rol IAM de la EC2 con acceso al bucket (`s3:GetObject/PutObject`, ya en §6.2). No
prende nada; ideal para pasos livianos (no arranques Spark para 50 MB).

**(c) Monitoreo/observabilidad — todo local (EC2):**
- **Orquestación** → web de Airflow: `Grid`/`Logs` de `small_etl` (el `print`/excepción de pandas
  aparece ahí mismo).
- **Recursos** → **Prometheus + Grafana** de la EC2 (§12): como el cómputo es la propia EC2, la CPU/RAM
  del run se ve en `node-exporter`/`cAdvisor`. Si un `read_csv` se come la RAM, salta
  `HostLowMemory` (§12.4) y lo ves en `cAdvisor` en el dashboard "Overview".
- **Logs** → **Loki + Promtail** (§12.7): el stdout del contenedor `airflow-scheduler` queda indexado;
  buscás por `{container="airflow-scheduler"}` en Grafana.
- **Métricas de DAG** → StatsD → Prometheus (§12.5): `airflow_dagrun_duration_success_count` alimenta
  el dead-man switch si el DAG deja de correr.

> Punto clave: en Python puro **toda** la telemetría vive en la EC2 (Airflow + Prometheus + Loki).
> No hay que ir a la consola de AWS a buscar nada.

### 17.2 PySpark en EMR Serverless

**(a) DAG** — dispara el job con `EmrServerlessStartJobOperator` y espera su fin (provider
`apache-airflow-providers-amazon`, §9.1). El código PySpark se sube a `s3://<artifacts>/emr/`
(§6.4/§11.3):

```python
# dags/emr_etl_dag.py — el caso "sí necesito Spark"
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

with DAG("emr_etl", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False,
         tags=["spark", "emr"]) as dag:
    run = EmrServerlessStartJobOperator(
        task_id="ventas_spark",
        application_id="{{ var.value.emr_app_id }}",           # output emr_app_id (§6.4)
        execution_role_arn="{{ var.value.emr_job_role_arn }}",  # output emr_job_role_arn (§6.4)
        job_driver={"sparkSubmit": {
            "entryPoint": "s3://{{ var.value.artifacts }}/emr/ventas.py",
            "entryPointArguments": ["{{ var.value.datalake }}", "{{ ds }}"],  # bucket pelado (convención §9.0/§10.2)
            "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g",
        }},
        configuration_overrides={"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": "s3://{{ var.value.artifacts }}/emr/logs/"},
        }},
        wait_for_completion=True,   # la task no termina hasta que el job EMR termina (falla si FAILED)
    )
```

**(b) Infra:** la app de **EMR Serverless** + su rol de ejecución (§6.4), y la EC2 con permiso para
`emr-serverless:StartJobRun` + `iam:PassRole` del rol de ejecución. **La EC2 no corre Spark**: solo
dispara y espera. El cómputo lo aprovisiona EMR por-job y **escala a cero** al terminar (pagás solo
mientras corre).

**(c) Monitoreo/observabilidad — split EC2 (orquestación) + AWS (cómputo):**
- **Orquestación** → web de Airflow: la task `ventas_spark` queda "running" mientras el job vive; se
  pone roja si el job termina `FAILED` (y dispara la alerta `EmrServerlessJobFailed`, §12.4).
- **Métricas del job** → **CloudWatch**, namespace `AWS/EMRServerless` (runs por estado, vCPU/GB-seg,
  memoria). Opcional: verlas en Grafana con un datasource CloudWatch (§12.6).
- **Logs del job (driver+executors)** → **S3** `s3://<artifacts>/emr/logs/` y **CloudWatch Logs**
  (`/aws/emr-serverless/pyspark-stack`, §6.4):
  ```bash
  aws logs tail /aws/emr-serverless/pyspark-stack --since 1h
  ```
- **Spark UI** → la **consola de EMR Serverless** reconstruye la Spark UI de cada corrida terminada
  (reemplaza al History Server local, que ya no aplica).

> Punto clave: la telemetría de *cómputo* **no** está en la EC2 — vive en **AWS (CloudWatch + consola
> EMR + S3)**, porque el Spark es managed. En la EC2 solo ves el estado *orquestado* del job. Detalle
> completo en §12.8.

### 17.3 SQL con Athena

**(a) DAG** — `AthenaOperator` (mismo provider Amazon). Típico: un **assert de calidad** barato tras
el job EMR que escribió `analytics/ventas/` (§16.4):

```python
# fragmento de dags/emr_etl_dag.py — encadenado tras el job EMR
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

assert_calidad = AthenaOperator(
    task_id="assert_calidad",
    query="SELECT count(*) AS filas FROM ventas WHERE dt = '{{ ds }}'",
    database="pyspark_stack_analytics",
    output_location="s3://{{ var.value.artifacts }}/athena-results/",
    workgroup="pyspark-stack-analytics",
)
run >> assert_calidad     # corre después del job EMR de §17.2
```

**(b) Infra:** el **workgroup** de Athena + la base en el Glue Data Catalog + la tabla con *partition
projection* (§16.1/§16.2), y la EC2 con permiso `athena:StartQueryExecution`/`GetQueryResults` +
lectura del lake + escritura de resultados (§16.3). **No prende ningún cluster**: Athena escanea S3 y
paga por dato leído (Parquet particionado ⇒ ~$0/mes a esta escala).

**(c) Monitoreo/observabilidad — split EC2 (orquestación) + AWS (motor SQL):**
- **Orquestación** → web de Airflow: `assert_calidad` roja = la query falló o devolvió un resultado que vos definiste como condición de fallo (p. ej. `0 filas`); el error de Athena aparece en `Logs` de la task.
- **Métricas del motor** → **CloudWatch** (el workgroup tiene `publish_cloudwatch_metrics_enabled`,
  §16.2): datos escaneados, tiempo y estado por query. De ahí sacás si una query escanea de más.
- **Historial y plan** → **consola de Athena** → *Query history*: cada ejecución con su
  `DataScannedInBytes`, duración y estado. La evidencia (resultado) queda en
  `s3://<artifacts>/athena-results/` (expira a 7 días, §16.2):
  ```bash
  aws athena list-query-executions --work-group pyspark-stack-analytics --max-results 5
  ```

> Punto clave: como en EMR, el *motor* es managed → su telemetría vive en **AWS (CloudWatch + consola
> Athena)**; Airflow solo te dice si el assert pasó. El costo se controla mirando `DataScannedInBytes`
> (bajalo con Parquet + particiones, §16.5).

**Resumen — dónde miro cada cosa:**

| Sabor | Orquestación | Recursos / métricas del cómputo | Logs | Costo se ve en |
|---|---|---|---|---|
| Python puro | Web Airflow | **Prometheus/Grafana** (EC2) | **Loki** (EC2) | — (parte de la EC2) |
| EMR Serverless | Web Airflow | **CloudWatch** `AWS/EMRServerless` + consola EMR | S3 `emr/logs/` + CloudWatch Logs | CloudWatch (vCPU/GB-seg) |
| Athena | Web Airflow | **CloudWatch** (workgroup) | consola Athena (*Query history*) | `DataScannedInBytes` |

---

## 18. Gobierno, costo y resiliencia (extras)

Cuatro piezas AWS-nativas, independientes entre sí y de todo lo anterior — no tocan Airflow, EMR
Serverless ni el compose. Las tres primeras son gratis o casi gratis; la cuarta (DLQ) es la más
urgente: es un hallazgo que `checkov` ya reporta en el CI (§11.2) desde el día uno y quedaba
diferido a propósito hasta acá.

Todas notifican al mismo lugar: `var.alert_email` (§5.1). Ponele un valor real en
`terraform.tfvars` antes de aplicar esta sección:

```hcl
# infra/prod/terraform.tfvars — sumá esta línea a las que ya tenías
alert_email = "vos@tu-email.com"
```

### 18.1 DLQ para las Lambdas `trigger-airflow` y `startstop`

**El problema concreto:** si `trigger-airflow` falla a mitad de un `SendCommand` (throttling de SSM,
la EC2 apagándose justo en ese instante, un error transitorio de IAM), Lambda reintenta un par de
veces y después **descarta el evento en silencio**. Si ese evento era un `ObjectCreated` de `raw/`
(§7.3), el archivo que lo disparó nunca se reprocesa y nadie se entera — la alerta `DailyEtlMissing`
(§12.4) recién avisa al día siguiente, y solo cubre el cron, no el event-driven.

**La solución:** una cola SQS como *Dead Letter Queue* en cada Lambda (`dead_letter_config`): los
eventos que agotan sus reintentos van ahí en vez de perderse, y una alarma de CloudWatch te avisa
apenas hay **algo** en la cola (no debería haber nunca nada, así que 1 mensaje ya es señal).

```hcl
# infra/prod/governance.tf — DLQ compartida (cola distinta para cada Lambda; alarma sobre ambas)
resource "aws_sqs_queue" "trigger_airflow_dlq" {
  name                      = "${var.name_prefix}-trigger-airflow-dlq"
  message_retention_seconds = 1209600 # 14 días — tiempo de sobra para investigar y reprocesar a mano
}
resource "aws_sqs_queue" "startstop_dlq" {
  name                      = "${var.name_prefix}-startstop-dlq"
  message_retention_seconds = 1209600
}

# Cada Lambda necesita permiso para escribir en SU PROPIA DLQ (no lo cubre la policy de invocación).
data "aws_iam_policy_document" "trigger_airflow_dlq" {
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.trigger_airflow_dlq.arn]
  }
}
resource "aws_iam_role_policy" "trigger_airflow_dlq" {
  name   = "trigger-airflow-dlq"
  role   = aws_iam_role.trigger_airflow.id   # rol ya creado en §7.1
  policy = data.aws_iam_policy_document.trigger_airflow_dlq.json
}

data "aws_iam_policy_document" "startstop_dlq" {
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.startstop_dlq.arn]
  }
}
resource "aws_iam_role_policy" "startstop_dlq" {
  name   = "startstop-dlq"
  role   = aws_iam_role.lambda.id            # rol ya creado en §5.4
  policy = data.aws_iam_policy_document.startstop_dlq.json
}

# Enganchá la DLQ a cada función (agregá este bloque a los `aws_lambda_function` ya existentes,
# §7.1 y §5.4 — no dupliques el resource, solo sumale `dead_letter_config`):
#   resource "aws_lambda_function" "trigger_airflow" { ... dead_letter_config { target_arn = aws_sqs_queue.trigger_airflow_dlq.arn } }
#   resource "aws_lambda_function" "startstop"       { ... dead_letter_config { target_arn = aws_sqs_queue.startstop_dlq.arn } }

# Tema SNS compartido por toda esta sección (Budgets y Cost Anomaly Detection notifican directo por
# email, sin SNS — pero las alarmas de CloudWatch sí necesitan un ARN de acción, de ahí este topic).
resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
}
resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
  # AWS manda un mail de confirmación al aplicar: hay que click "Confirm subscription" una vez,
  # si no la suscripción queda en pending y nunca llegan alertas.
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  for_each            = { trigger = aws_sqs_queue.trigger_airflow_dlq, startstop = aws_sqs_queue.startstop_dlq }
  alarm_name          = "${var.name_prefix}-dlq-${each.key}-not-empty"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = each.value.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
}
```

> **Reprocesar un mensaje de la DLQ:** `aws sqs receive-message --queue-url <url>` te da el `event`
> original (para `trigger-airflow`, el payload S3 `ObjectCreated`); reenvialo a mano invocando la
> Lambda (`aws lambda invoke --function-name ... --payload file://evento.json`) y después
> `aws sqs delete-message` para sacarlo de la cola. No hay redrive automático — a este volumen
> (~13 corridas/mes) no se justifica un pipeline de reproceso; es una cola chica que revisás cuando
> suena la alarma.

<details>
<summary>🖱️ A mano en la consola AWS — DLQ + alarma</summary>

1. **SQS → Create queue** ×2, *Standard*, nombres `pyspark-stack-trigger-airflow-dlq` y
   `pyspark-stack-startstop-dlq`, *Retention period* 14 días.
2. **Lambda → `pyspark-stack-trigger-airflow` → Configuration → Asynchronous invocation → Edit** →
   *Dead-letter queue* → la cola `trigger-airflow-dlq`. Repetí para `pyspark-stack-startstop` con su
   propia cola.
3. En el rol de ejecución de cada Lambda (IAM → Roles), **Add permissions → Create inline policy**
   → `sqs:SendMessage` sobre el ARN de su cola.
4. **SNS → Create topic** (*Standard*, nombre `pyspark-stack-alerts`) → **Create subscription** →
   *Protocol* Email → tu dirección → confirmá el mail que te llega.
5. **CloudWatch → Alarms → Create alarm** ×2, métrica `AWS/SQS → ApproximateNumberOfMessagesVisible`
   filtrada por cada `QueueName`, condición `> 0`, acción: notificar al topic SNS de arriba.

</details>

### 18.2 AWS Budgets

Presupuesto mensual con dos umbrales de aviso — **gratis** (los primeros presupuestos no tienen
costo). Referencia: el total canónico de §2 es ~$35/mes (start/stop) / ~$83/mes (24/7); poné el
`limit_amount` con margen sobre el que corresponda a tu modo de operación.

```hcl
# infra/prod/governance.tf
resource "aws_budgets_budget" "monthly" {
  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = "50"     # ajustá al total real de §2 + margen
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "FORECASTED"   # avisa ANTES de llegar, según la proyección de AWS
    subscriber_email_addresses = [var.alert_email]
  }
}
```

<details>
<summary>🖱️ A mano en la consola AWS — Budgets</summary>

**Billing and Cost Management → Budgets → Create budget** → *Customize (advanced)* → *Cost budget* →
monto mensual (§2 + margen) → dos *Alert thresholds*: 80% actual y 100% forecasted → tu email en
ambas → **Create budget**.

</details>

### 18.3 Cost Anomaly Detection

Detecta picos de gasto que no siguen el patrón histórico (p. ej. un job de EMR Serverless que quedó
pegado escalando workers, o un bug que dispara el DAG en loop) — **gratis**, sin infraestructura que
mantener, corre sobre el mismo Cost Explorer que ya usás para ver el total mensual.

```hcl
# infra/prod/governance.tf
resource "aws_ce_anomaly_monitor" "service" {
  name              = "${var.name_prefix}-service-monitor"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"   # anomalías por servicio (EMR Serverless, EC2, S3...) — el nivel útil acá
}

resource "aws_ce_anomaly_subscription" "alerts" {
  name      = "${var.name_prefix}-anomaly-alerts"
  frequency = "DAILY"   # resumen diario, no un mail por cada anomalía chica
  monitor_arn_list = [aws_ce_anomaly_monitor.service.arn]
  subscriber {
    type    = "EMAIL"
    address = var.alert_email
  }
  threshold_expression {
    dimension {
      key           = "ANOMALY_TOTAL_IMPACT_ABSOLUTE"
      values        = ["5"]   # solo avisa si la anomalía supera ~$5 — a esta escala, ruido debajo de eso
      match_options = ["GREATER_THAN_OR_EQUAL"]
    }
  }
}
```

<details>
<summary>🖱️ A mano en la consola AWS — Cost Anomaly Detection</summary>

**Billing and Cost Management → Cost Anomaly Detection → Create monitor** → *AWS services* (nivel
servicio) → **Create alert subscription** → frecuencia *Daily summary*, umbral $5, tu email →
**Save**.

</details>

### 18.4 IAM Access Analyzer

Analiza las policies IAM de la cuenta y avisa si algún recurso (bucket S3, rol, cola SQS...) es
accesible desde **fuera** de la cuenta — gratis, a nivel cuenta, sin mantenimiento. A esta escala
(una sola cuenta, sin cross-account) no debería reportar nada; el valor es la **detección temprana**
si algún día agregás un rol con `Principal` mal acotado.

```hcl
# infra/prod/governance.tf
resource "aws_accessanalyzer_analyzer" "account" {
  analyzer_name = "${var.name_prefix}-analyzer"
  type          = "ACCOUNT"
}
```

<details>
<summary>🖱️ A mano en la consola AWS — Access Analyzer</summary>

**IAM → Access Analyzer → Create analyzer** → *Zone of trust* Current account → nombre
`pyspark-stack-analyzer` → **Create analyzer**. Los hallazgos aparecen en la misma pantalla; no
generan email por sí solos (opcional: EventBridge rule sobre
`aws.access-analyzer` + `Access Analyzer Finding` → el mismo SNS topic de §18.1 si querés que te
avisen sin entrar a la consola).

</details>

### 18.5 Verificación

```bash
# DLQ: confirmá que ambas Lambdas tienen dead-letter configurado
aws lambda get-function --function-name pyspark-stack-trigger-airflow --query 'Configuration.DeadLetterConfig'
aws lambda get-function --function-name pyspark-stack-startstop --query 'Configuration.DeadLetterConfig'

# Suscripción SNS confirmada (no "PendingConfirmation")
aws sns list-subscriptions-by-topic --topic-arn "$(terraform -chdir=infra/prod output -raw alerts_topic_arn 2>/dev/null || echo)" \
  --query 'Subscriptions[].SubscriptionArn'

# Budget existe
aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)" \
  --query 'Budgets[].BudgetName'

# Cost Anomaly: monitor activo
aws ce get-anomaly-monitors --query 'AnomalyMonitors[].MonitorName'

# Access Analyzer: estado ACTIVE
aws accessanalyzer list-analyzers --query 'analyzers[].{name:name,status:status}'
```

Agregá `output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }` a `infra/prod/outputs.tf`
si querés el comando de arriba tal cual; si no, sacá el ARN de `terraform state show
aws_sns_topic.alerts` o de la consola.

---

## 19. Transformaciones SQL con dbt

dbt reemplaza el SQL que hoy viviría hardcodeado dentro de un DAG (como el `assert_calidad` de
§16.4) por **modelos versionados en Git**, con tests de datos integrados y documentación
autogenerada. **Airflow sigue siendo el único orquestador**: dbt no reemplaza nada de lo que ya
tenés, es una task más — `BashOperator` corriendo `dbt run`, igual que hoy corrés papermill (§9) o
disparás EMR Serverless (§9.0).

### 19.1 Dos targets, uno por tipo de transformación

Mismo criterio que la tabla "¿PySpark o Python puro?" de §9.0: la mayoría de las transformaciones
`curated → analytics` son SQL liviano sobre las tablas Iceberg (§16.1) — para eso, Athena directo,
sin prender ningún cómputo. Solo si el modelo necesita reprocesar volumen grande o lógica que no
entra en SQL, vale la pena Spark.

| Target | Motor | Cuándo | Cómo corre |
|---|---|---|---|
| `athena` | Athena (SQL sobre Iceberg, §16.1) | La mayoría de los modelos: agregaciones, joins livianos, lo que hoy sería un `INSERT`/`MERGE` chico | El adapter `dbt-athena-community` se conecta directo vía boto3 — dbt corre **en la EC2**, sin levantar cómputo |
| `spark` | EMR Serverless | Solo si el modelo reprocesa volumen grande que Athena no maneja cómodo | dbt corre **embebido en el propio job de EMR Serverless** (ver nota abajo) — no es una conexión a un cluster persistente |

> **Por qué el target `spark` no es un dbt-spark "típico".** El adapter `dbt-spark` normalmente se
> conecta a un **Spark ya corriendo** (Thrift server, Databricks, o una sesión local) — necesita un
> endpoint vivo al que conectarse por JDBC/ODBC. **EMR Serverless no es eso**: cada `StartJobRun` es
> un envío de job efímero (como `spark-submit`), no un cluster que quede escuchando queries. Para que
> dbt use EMR Serverless, dbt tiene que **correr adentro** del propio job (método `session` del
> adapter: crea la `SparkSession` en el mismo proceso), empaquetado como el entrypoint que hoy
> apunta a un script `.py` normal (§6.4/§9.0). Es más piezas moviéndose que el target `athena` — usalo
> solo si de verdad necesitás que un modelo dbt reprocese a escala Spark; si no, un job Spark
> disparado directo con `EmrServerlessStartJobOperator` (§9.0) sigue siendo el camino simple para
> transformaciones pesadas, con o sin dbt en el medio.

### 19.2 Instalación

```text
# requirements.txt — agregá junto a los providers de §9.1 (pineá contra el constraints de
# Airflow 3.2.2/py3.12 igual que el resto; verificá compatibilidad exacta antes de fijar versión)
dbt-core==1.9.*
dbt-athena-community==1.9.*
dbt-spark[session]==1.9.*   # solo si vas a usar el target `spark` (§19.1) — trae pyspark como dependencia
```

### 19.3 Estructura del proyecto y `profiles.yml`

```text
dbt/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── athena/
    │   └── ventas_por_pais.sql
    └── spark/
        └── ventas_reproceso.sql
```

```yaml
# dbt/dbt_project.yml
name: pyspark_stack_dbt
version: "1.0.0"
profile: pyspark_stack

model-paths: ["models"]

models:
  pyspark_stack_dbt:
    athena:
      +schema: analytics
      +materialized: table
    spark:
      +schema: analytics
      +materialized: table
```

```yaml
# dbt/profiles.yml — las credenciales salen del rol IAM (EC2 en prod, el rol OIDC dbt_ci en CI,
# §11.1) — sin keys en ningún target.
pyspark_stack:
  target: athena   # default en prod; el DAG lo pisa con --target según el modelo (§19.4)
  outputs:
    athena:
      type: athena
      s3_staging_dir: "s3://{{ env_var('ARTIFACTS_BUCKET') }}/athena-results/"
      region_name: us-east-1
      database: pyspark_stack_analytics     # el Glue database de §6.4/§16.2
      work_group: pyspark-stack-analytics   # el workgroup engine v3 de §16.2
      num_retries: 3
    spark:
      type: spark
      method: session       # sin endpoint: crea la SparkSession en el mismo proceso (nota de §19.1)
      schema: pyspark_stack_analytics
      host: localhost        # requerido por el adapter aunque `session` no lo use para conectar
    # Target de CI (§11.2, Slim CI): mismo motor que `athena`, pero aislado en su propio Glue
    # database y su propio prefijo S3 — un PR nunca escribe sobre las tablas de producción.
    ci:
      type: athena
      s3_staging_dir: "s3://{{ env_var('ARTIFACTS_BUCKET') }}/athena-results/"
      s3_data_dir: "s3://{{ env_var('ARTIFACTS_BUCKET') }}/dbt-ci/"   # dónde caen los DATOS (no solo resultados de query)
      s3_data_naming: schema_table
      region_name: us-east-1
      database: pyspark_stack_analytics_ci
      work_group: pyspark-stack-analytics
      num_retries: 3
```

### 19.4 Modelo de ejemplo (target `athena`) y task de Airflow

```sql
-- dbt/models/athena/ventas_por_pais.sql
{{ config(materialized='table') }}

SELECT
  pais,
  sum(monto) AS monto_total,
  count(*)   AS cantidad_ventas
FROM {{ source('curated', 'ventas') }}
WHERE dt = '{{ var("ds") }}'
GROUP BY pais
```

```python
# fragmento de un DAG — dispara dbt como cualquier otra task, tras el job EMR que puebla curated/
from airflow.providers.standard.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id="dbt_run_ventas",
    bash_command=(
        "cd /opt/dbt && "
        "dbt run --target athena --profiles-dir . --select ventas_por_pais --vars '{\"ds\": \"{{ ds }}\"}' && "
        # Publica el manifest.json de ESTA corrida real como el nuevo baseline de Slim CI (§11.2):
        # es la única ejecución de dbt contra producción —Airflow siempre orquesta, CI nunca corre
        # dbt contra datos reales— así que es la fuente de verdad correcta para el `--state` de los PRs.
        "aws s3 cp target/manifest.json s3://{{ var.value.artifacts }}/dbt-state/manifest.json"
    ),
    env={"ARTIFACTS_BUCKET": "{{ var.value.artifacts }}"},
)
run_emr >> dbt_run   # corre después del job EMR que escribe curated/ventas (§9.0)
```

Montá `./dbt:/opt/dbt` en `airflow-scheduler` (mismo criterio que `./notebooks` para papermill,
§9.1) — agregalo al `docker-compose.prod.yml`:

```yaml
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks
      - ./dbt:/opt/dbt
```

### 19.5 Docs de dbt (opcional)

`dbt docs generate` produce un sitio estático navegable (linaje de modelos, columnas, tests). Como
task adicional del DAG, subilo a S3 y serví el HTML desde ahí (o abrilo local por túnel):

```bash
dbt docs generate --target athena
aws s3 sync target/ "s3://<artifacts>/dbt-docs/" --exclude '*' --include '*.html' --include '*.json' --include '*.js'
```

---

## 20. Calidad de datos con Great Expectations

Un gate más estricto que el `SELECT count(*)` de §16.4: valida schema, nulls y rangos de `curated/`
**antes** de promoverlo a `analytics/` (o antes de que dbt corra sus modelos sobre esa tabla, §19).
Igual que dbt, corre **vía SQL sobre Athena** — no levanta Spark ni pandas cargando el dataset
entero a memoria: las expectations se traducen a queries que Athena pushea contra la tabla Iceberg.
Es la misma filosofía de "liviano → Athena, pesado → Spark" de §9.0/§19.1.

### 20.1 Instalación

```text
# requirements.txt
great-expectations==1.3.*
pyathena==3.*   # motor SQLAlchemy que GE usa para conectarse a Athena
```

### 20.2 Estructura del proyecto

```text
great_expectations/
├── great_expectations.yml
├── expectations/
│   └── curated_ventas.json
└── checkpoints/
    └── curated_ventas_checkpoint.yml
```

```yaml
# great_expectations/great_expectations.yml (fragmento — datasource)
datasources:
  athena_curated:
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      # mismo workgroup/database que dbt (§19.3) y el resto de Athena (§16.2)
      connection_string: >-
        awsathena+rest://@athena.us-east-1.amazonaws.com:443/pyspark_stack_analytics
        ?s3_staging_dir=s3://<artifacts>/athena-results/&work_group=pyspark-stack-analytics
    data_connectors:
      default_runtime:
        class_name: RuntimeDataConnector
        batch_identifiers: ["dt"]
```

> Sin access keys: el rol IAM de la EC2 (los mismos permisos `ec2_athena` de §16.3) alcanza —
> `pyathena` firma con las credenciales del entorno (instance profile), igual que boto3.

### 20.3 Expectation suite de ejemplo

`great_expectations/expectations/curated_ventas.json` (fragmento — JSON no admite comentarios, así
que la ruta va en el texto, no en el bloque):

```json
{
  "expectation_suite_name": "curated_ventas",
  "expectations": [
    { "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": { "min_value": 1 } },
    { "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "pais" } },
    { "expectation_type": "expect_column_values_to_be_between",
      "kwargs": { "column": "monto", "min_value": 0, "max_value": 1000000 } }
  ]
}
```

```yaml
# great_expectations/checkpoints/curated_ventas_checkpoint.yml
name: curated_ventas_checkpoint
validations:
  - batch_request:
      datasource_name: athena_curated
      data_connector_name: default_runtime
      data_asset_name: ventas
      runtime_parameters: { query: "SELECT * FROM ventas WHERE dt = '{ds}'" }
    expectation_suite_name: curated_ventas
action_list:
  - name: store_validation_result
    action: { class_name: StoreValidationResultAction }
```

### 20.4 Task de Airflow

```python
# fragmento de un DAG — corre entre el job EMR (escribe curated/) y dbt/la promoción a analytics/
from airflow.providers.standard.operators.bash import BashOperator

validar_calidad = BashOperator(
    task_id="validar_calidad_ventas",
    bash_command=(
        "cd /opt/great_expectations && "
        "great_expectations checkpoint run curated_ventas_checkpoint "
        "--checkpoint-config '{\"validations\": [{\"batch_request\": {\"runtime_parameters\": "
        "{\"query\": \"SELECT * FROM ventas WHERE dt = '\\''{{ ds }}'\\''\"}}}]}'"
    ),
)
run_emr >> validar_calidad >> dbt_run   # solo promueve/transforma si la validación pasa
```

`great_expectations checkpoint run` sale con código de error si alguna expectation falla — la task
queda roja en Airflow, con el mismo mecanismo de cualquier otra falla de DAG (retries, `email_on_failure`
si lo configuraste, y la métrica de task failed que ya scrapea StatsD → Prometheus, §12.5). No es un
canal de alertas nuevo.

Montá `./great_expectations:/opt/great_expectations` en `airflow-scheduler`, junto a `./dbt` (§19.4):

```yaml
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks
      - ./dbt:/opt/dbt
      - ./great_expectations:/opt/great_expectations
```

### 20.5 Reporte HTML (Data Docs, opcional)

Igual que `dbt docs` (§19.5), Great Expectations genera un sitio estático navegable con el resultado
de cada corrida — subilo al mismo bucket de artifacts:

```bash
great_expectations --v3-api docs build --no-view
aws s3 sync great_expectations/uncommitted/data_docs/local_site/ "s3://<artifacts>/ge-docs/"
```

---

## 21. Lineage de datos con OpenLineage

**Qué resuelve:** saber "esta tabla de `analytics/` salió de qué archivo de `raw/`, pasando por qué
job y qué modelo dbt" sin leer el código de cada DAG — útil cuando algo en un reporte de BI se ve
raro y hay que rastrear hacia atrás qué lo produjo.

**Diseño elegido (sin servicio nuevo):** en vez de desplegar Marquez (API + UI + su propio Postgres
— otro contenedor con estado en una `t3.large` ya ajustada de recursos), los eventos de lineage de
Airflow y dbt se escriben como JSON Lines a S3 y se consultan con Athena, mismo patrón que el resto
de la plataforma (§16). Marquez queda como upgrade opcional (§21.5) si en algún momento querés una
UI de grafo navegable o lineage columna-a-columna dentro de los jobs Spark.

> El ecosistema OpenLineage cambia rápido entre versiones mayores (nombres de config, formato de
> transporte). Lo de abajo es la **forma** de la solución, verificada contra la documentación
> oficial al momento de escribir esto — antes de fijar versiones en `requirements.txt`, confirmá
> las claves exactas contra el changelog de la versión que instalás.

### 21.1 Qué cubre y qué no

| Capa | Cubierta | Cómo |
|---|---|---|
| Airflow (DAG → task → dataset) | Sí | `apache-airflow-providers-openlineage`, emite automático por task |
| dbt (modelo → modelo, columna → columna) | Sí | `openlineage-dbt` envuelve `dbt run`/`build`, usa el `manifest.json` que dbt ya genera (§19.5) |
| Spark en EMR Serverless (columna → columna dentro del job) | **No, por defecto** | Necesita el listener `openlineage-spark` + un endpoint HTTP receptor (Marquez). Cada `StartJobRun` es efímero: no hay archivo compartido a dónde escribir. Ver §21.5 |

Con Airflow + dbt ya tenés lineage **a nivel de tabla/dataset** de punta a punta
(`raw/archivo.csv → curated.ventas → analytics.ventas_por_pais`) — lo que en la práctica resuelve la
mayoría de los "¿qué tocó esto?". El lineage columna-a-columna *dentro* de un job Spark es la pieza
que falta sin Marquez.

### 21.2 Instalación

```text
# requirements.txt
apache-airflow-providers-openlineage==1.*   # verificá la versión exacta compatible con Airflow 3.2.2
openlineage-dbt==1.*                        # ídem, contra la versión de dbt-core fijada en §19.2
```

### 21.3 Config: eventos a un archivo local, sincronizados a S3

```yaml
# docker-compose.prod.yml — dentro de x-airflow-common → environment → &airflow-common-env
AIRFLOW__OPENLINEAGE__TRANSPORT: '{"type": "file", "log_file_path": "/opt/airflow/logs/openlineage/events.log"}'
AIRFLOW__OPENLINEAGE__NAMESPACE: "pyspark-stack-prod"
```

Montá un path persistente para ese archivo (si no, se pierde en cada restart del contenedor):

```yaml
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks
      - ./dbt:/opt/dbt
      - ./great_expectations:/opt/great_expectations
      - /data/openlineage:/opt/airflow/logs/openlineage   # persistente en el EBS /data (§5.3)
```

dbt usa el mismo transporte vía las variables de entorno que lee `openlineage-dbt` (no dbt en sí):

```bash
export OPENLINEAGE_URL=file:///opt/airflow/logs/openlineage/events.log   # verificar sintaxis exacta de la versión instalada
export OPENLINEAGE_NAMESPACE=pyspark-stack-prod
openlineage-dbt run --target athena --profiles-dir . --select ventas_por_pais
```

Task de Airflow que sincroniza el archivo a S3, particionado por fecha (para que Athena no escanee
todo el historial en cada consulta):

```python
from airflow.providers.standard.operators.bash import BashOperator

sync_lineage = BashOperator(
    task_id="sync_lineage_s3",
    bash_command=(
        "aws s3 cp /opt/airflow/logs/openlineage/events.log "
        "s3://{{ var.value.artifacts }}/lineage/dt={{ ds }}/events.jsonl"
    ),
    trigger_rule="all_done",   # corre aunque el DAG haya fallado — el lineage de un fallo también importa
)
```

### 21.4 Consultar el lineage desde Athena

Tabla externa sobre el JSON Lines (acá SÍ hace falta declarar el esquema a mano, a diferencia de
las tablas Iceberg de §16.1: el evento OpenLineage es JSON anidado, no tabular):

```sql
CREATE EXTERNAL TABLE pyspark_stack_analytics.lineage_events (
  eventType string,
  eventTime string,
  job       struct<namespace: string, name: string>,
  inputs    array<struct<namespace: string, name: string>>,
  outputs   array<struct<namespace: string, name: string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
PARTITIONED BY (dt string)
LOCATION 's3://pyspark-stack-artifacts-<acct>/lineage/'
TBLPROPERTIES ('projection.enabled' = 'true', 'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd', 'projection.dt.range' = '2026-01-01,NOW',
  'storage.location.template' = 's3://pyspark-stack-artifacts-<acct>/lineage/dt=${dt}');

-- "¿qué produjo esta tabla, y de qué la armó?"
SELECT job.name AS productor, o.name AS salida, i.name AS entrada, eventTime
FROM pyspark_stack_analytics.lineage_events
CROSS JOIN UNNEST(outputs) AS t(o)
CROSS JOIN UNNEST(inputs)  AS t(i)
WHERE o.name LIKE '%ventas_por_pais%'
ORDER BY eventTime DESC LIMIT 10;
```

> Nota: esta tabla usa **partition projection**, la misma técnica de §16.1 antes de migrar a
> Iceberg — son mecanismos independientes, cada tabla elige el que le sirve; los eventos de lineage
> son *write-once, append-only* y no necesitan `MERGE`, así que projection alcanza y sobra acá.

### 21.5 Upgrade opcional: Marquez (UI navegable + lineage de Spark)

Si más adelante necesitás lineage columna-a-columna dentro de los jobs Spark, o preferís un grafo
clickeable en vez de SQL: Marquez es el backend de referencia de OpenLineage (API + UI + su propio
Postgres). Súmalo como servicios nuevos en `docker-compose.prod.yml` (impacto real en RAM: +1-2 GB
en una `t3.large` de 8 GB — confirmá que sobra margen antes de meterlo, §13.2) y cambiá el
transporte de `file` a `http` apuntando a su endpoint — mismos providers de arriba, solo cambia el
destino. Para EMR Serverless, sumá al `sparkSubmitParameters` (§6.4):

```text
--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
--conf spark.openlineage.transport.type=http
--conf spark.openlineage.transport.url=http://<marquez-host>:5000
--conf spark.openlineage.namespace=pyspark-stack-prod
```

> Marquez necesita ser alcanzable desde EMR Serverless, que corre **fuera** de tu VPC por defecto
> (§6.4) — exponer ese endpoint más allá del túnel SSH actual, o meter EMR Serverless dentro de la
> VPC (`network_configuration`, ya comentado como opcional en §6.4), no es gratis en complejidad.
> Evaluá si el lineage columna-a-columna de Spark lo justifica antes de sumarlo.
