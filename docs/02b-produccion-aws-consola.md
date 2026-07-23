# Guía experta — Producción en AWS **100% por la consola** (Airflow en EC2 + EMR Serverless)

> Misma arquitectura y mismo objetivo que [`docs/02-produccion-aws.md`](02-produccion-aws.md)
> —una EC2 **chica** (`t3.large`) que corre solo el *orquestador* (Airflow + Postgres + monitoreo)
> en Docker, con **Spark fuera de la caja** en **EMR Serverless** (pago por uso, escala a cero), S3
> como data lake, Lambda + EventBridge para disparar los DAGs y el auto start/stop, monitoreo
> completo y CI/CD— pero construida **enteramente a mano en la consola web de AWS**. Cero Terraform:
> cada recurso se crea clic a clic, y todo lo que se pega (políticas IAM en JSON, código de las
> Lambdas, `user_data`, secretos, configs) va **copy-paste** en esta misma guía.

> **¿Consola o Terraform?** La consola sirve para **entender** qué crea cada bloque, para un
> despliegue **puntual** o para **aprender** AWS tocando cada servicio. Pero tiene costos reales que
> conviene saber antes de empezar:
> - **No hay estado ni reproducibilidad.** No existe `terraform plan`/`apply`/`destroy`. Si querés
>   recrear todo en otra cuenta/región, repetís cada clic a mano.
> - **Teardown manual.** Para no dejar cargos colgando hay que borrar cada recurso a mano en orden
>   inverso (hay un checklist al final, §15.3). No hay un botón "borrar todo".
> - **Drift silencioso.** Un cambio a mano no queda versionado; a los 3 meses nadie sabe por qué el
>   SG tiene esa regla. Terraform es la fuente de verdad; la consola no deja rastro.
> - **No mezcles las dos vías** para el mismo recurso. Si algo lo creaste acá a mano y después
>   querés pasarlo a Terraform, hay que `terraform import`; si no, el `apply` duplica o choca por
>   nombre.
>
> Con eso claro: esta guía es el camino manual completo, de cero a producción.

Índice:
1. [Panorama y orden de creación](#1-panorama-y-orden-de-creación)
2. [Costo](#2-costo)
3. [Prerrequisitos](#3-prerrequisitos)
4. [Núcleo: EC2 con Docker](#4-núcleo-ec2-con-docker)
   - 4.1 [Security group (SSH + web de Airflow a tu IP)](#41-security-group)
   - 4.2 [Key pair + rol IAM de la EC2](#42-key-pair--rol-iam-de-la-ec2)
   - 4.3 [EC2 + EBS + user_data + Elastic IP](#43-ec2--ebs--user_data--elastic-ip)
   - 4.4 [Automatización: Lambda start/stop + EventBridge Scheduler](#44-automatización-lambda-startstop--eventbridge-scheduler)
   - 4.5 [Desplegar, subir código y túnel SSH](#45-desplegar-subir-código-y-túnel-ssh)
   - 4.6 [Exponer la web de Airflow (HTTPS nativo, solo tu IP)](#46-exponer-la-web-de-airflow-https-nativo-solo-tu-ip)
5. [Data lake en S3 + cómputo Spark](#5-data-lake-en-s3--cómputo-spark)
   - 5.1 [Buckets S3 (data lake + artifacts)](#51-buckets-s3)
   - 5.2 [IAM: permitir S3 a la EC2 (sin keys)](#52-iam-permitir-s3-a-la-ec2)
   - 5.3 [Backups: snapshots EBS automáticos (DLM)](#53-backups-snapshots-ebs-automáticos-dlm)
   - 5.4 [Cómputo Spark: EMR Serverless (app + roles + submit)](#54-cómputo-spark-emr-serverless)
   - 5.5 [S3 VPC Gateway Endpoint](#55-s3-vpc-gateway-endpoint)
6. [Orquestación: Lambda trigger-airflow + EventBridge + evento S3](#6-orquestación-lambda-trigger-airflow)
   - 6.1 [Lambda que dispara los DAGs vía SSM (retry + contrato de datos)](#61-lambda-que-dispara-los-dags-vía-ssm-con-retry-si-la-ec2-está-apagada--contrato-de-datos)
   - 6.2 [Disparo por cron (EventBridge Scheduler)](#62-disparo-por-cron)
   - 6.3 [Disparo por evento (archivo nuevo en S3, vía SQS)](#63-disparo-por-evento-archivo-nuevo-en-s3-vía-sqs)
7. [Secretos y parámetros (SSM Parameter Store / Secrets Manager)](#7-secretos-y-parámetros)
8. [CI/CD con GitHub Actions (OIDC, sin claves)](#8-cicd-con-github-actions-oidc-sin-claves)
9. [Monitoreo (Prometheus + Grafana + Alertmanager + Loki)](#9-monitoreo)
10. [Athena — capa de consumo SQL/BI (opcional)](#10-athena--capa-de-consumo-sqlbi-opcional)
11. [Archivos de repo (compose, DAGs, scripts, monitoreo)](#11-archivos-de-repo)
12. [Los DAGs de producción (EMR Serverless)](#12-los-dags-de-producción-emr-serverless)
13. [Operación, seguridad y ahorro](#13-operación-seguridad-y-ahorro)
14. [Airflow, 3 sabores: Python puro · EMR Serverless · Athena](#14-airflow-3-sabores)
15. [Runbook final + smoke tests + teardown](#15-runbook-final)
16. [Gobierno, costo y resiliencia (extras)](#16-gobierno-costo-y-resiliencia-extras)
17. [Lineage de datos con OpenLineage](#17-lineage-de-datos-con-openlineage)

---

## 1. Panorama y orden de creación

La arquitectura es **idéntica** a la de la guía 02 (el detalle conceptual y los diagramas están en
[`docs/03-arquitectura.md`](03-arquitectura.md)). Una EC2 `t3.large` corre solo el orquestador en
Docker; AWS *serverless* lo rodea para el cómputo Spark (EMR Serverless), storage durable (S3),
disparo de DAGs (Lambda + EventBridge) y ahorro (auto start/stop).

```
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

Regla mental: almacenar es barato y constante; computar es lo que cuesta, y solo cuando corrés. Por
eso Spark vive en EMR Serverless (escala a cero), la EC2 se apaga fuera de horario (auto start/stop)
y el data lake vive en S3.

### El orden importa (dependencias)

En Terraform el grafo de dependencias lo resuelve el `apply`; **a mano tenés que respetar el orden**,
porque muchos recursos referencian a otros por ARN/ID que todavía no existirían. Este es el camino
que sigue la guía, y por qué:

| # | Creás | Necesita que ya exista |
|---|---|---|
| 1 | Security group (§4.1) | — |
| 2 | Key pair + rol IAM de la EC2 (§4.2) | — |
| 3 | EC2 + EBS + EIP (§4.3) | SG, key pair, rol EC2 |
| 4 | Lambda startstop + schedules (§4.4) | EC2 (para el ARN de la instancia en la policy SSM) |
| 5 | Buckets S3 (§5.1) | — |
| 6 | Política S3 al rol EC2 (§5.2) | buckets, rol EC2 |
| 7 | Backups DLM (§5.3) | volumen `/data` etiquetado |
| 8 | EMR Serverless app + rol del job + permisos EC2 (§5.4) | buckets, rol EC2, Lambda startstop |
| 9 | S3 VPC endpoint (§5.5) | — |
| 10 | Lambda trigger-airflow + schedules + evento S3 (§6) | EC2, bucket datalake |
| 11 | Secretos en SSM + permisos EC2 (§7) | rol EC2 |
| 12 | OIDC + rol de GitHub Actions (§8) | bucket artifacts |
| 13 | (En la EC2) subir repo, generar `.env`, `docker compose up` (§15) | todo lo anterior |

> Los nombres son fijos en toda la guía: región **`us-east-1`**, prefijo **`pyspark-stack`**,
> `<acct>` = tu **Account ID** (arriba a la derecha en la consola, o *IAM → Dashboard*). Donde veas
> `<acct>` reemplazalo por ese número de 12 dígitos.

---

## 2. Costo

Idéntico a la guía 02 (la vía de creación no cambia el precio). Precios aproximados us-east-1,
estimados en julio 2026
(on-demand), sujetos a cambio — validá en [calculator.aws](https://calculator.aws). Escenario real:
~2 GB/día, 3 corridas/semana (≈13/mes) de Spark en EMR Serverless, ~50 GB en el data lake.

| Ítem | US$/mes (auto start/stop 8h×22d) |
|---|---|
| EC2 `t3.large` (Airflow + Postgres + monitoreo) | ~12 |
| EMR Serverless (pago por uso, ~13 corridas/mes) | ~9 |
| EBS gp3 (root 40 + data 30) + snapshots DLM | ~9 |
| S3 data lake (~50 GB) + requests | ~1.5 |
| IPv4 pública (EIP; AWS la cobra desde feb-2024, asociada o no) | ~3.6 |
| Lambda + EventBridge + SSM | ~0 (free tier) |
| **Total** | **~35/mes** |

Variante **24/7** (EC2 encendida siempre): ~**$83/mes**. A tu volumen exacto EMR Serverless ronda
~$5 → real ~**$31** (start/stop) / ~**$79** (24/7). El monitoreo corre dentro de la misma EC2 (costo
$0 adicional). La comparación self-managed vs managed (EMR Serverless, Glue, EMR on EC2, MWAA) está
en la guía 02 §2 — no cambia.

---

## 3. Prerrequisitos

- **Cuenta AWS** con un usuario/rol que tenga permisos de administración (o al menos EC2, S3, IAM,
  Lambda, EMR Serverless, EventBridge, SSM, CloudWatch, Route 53). Para la consola alcanza con
  loguearte; el **AWS CLI** local es **opcional** pero muy útil para las verificaciones (`aws ...`)
  y para subir código a la EC2.
- **Par de claves SSH** para entrar a la EC2. Generalo en tu máquina (una vez):

  ```bash
  ssh-keygen -t ed25519 -f ~/.ssh/pyspark_stack -C "pyspark_stack"
  cat ~/.ssh/pyspark_stack.pub    # este contenido lo pegás al importar el key pair (§4.2)
  ```

- **Tu IP pública /32** (única fuente de SSH y de la web de Airflow):

  ```bash
  curl -s https://checkip.amazonaws.com    # p.ej. 203.0.113.7  →  usás 203.0.113.7/32
  ```

  > Si tu IP es dinámica (cambia sola), vas a tener que **editar la regla del SG** cuando cambie
  > (§4.1). Con IP fija, se setea una vez.

- El **repositorio del proyecto** clonado en tu máquina (trae los `dags/`, `spark-apps/`,
  `docker-compose.yml`, etc.). Los archivos nuevos que pide esta guía (Lambdas, monitoreo, compose
  de prod, scripts) los vas creando con los bloques copy-paste de acá.

Estructura de archivos que vas a crear en el repo (nada de `infra/*.tf`: no hay Terraform):

```
lambdas/
├── startstop.py            # código que pegás en la Lambda startstop (§4.4)
└── trigger_airflow.py      # código que pegás en la Lambda trigger-airflow (§6.1)
scripts/
├── load-secrets.sh         # materializa .env desde SSM en la EC2 (§7)
└── deploy.sh               # deploy rápido dev (§11)
monitoring/                 # configs de Prometheus/Grafana/Alertmanager/Loki (§9/§11)
docker-compose.prod.yml     # stack de producción, standalone (§11)
dags/                       # + los DAGs de producción EMR (§12)
.github/workflows/          # ci.yml + deploy.yml (§8)
```

---

## 4. Núcleo: EC2 con Docker

La EC2 corre un `docker-compose.prod.yml` propio, standalone (§11) — no el mismo de local: solo el
orquestador (Airflow + Postgres + monitoreo), sin Spark ni HDFS. Acceso por **túnel SSH** para todo,
más una excepción explícita: la web de Airflow se publica por **HTTPS (443) restringida a tu IP**
(§4.6). Grafana/Prometheus/Loki siguen **solo por túnel**.

### 4.1 Security group

Consola: **VPC → Security groups → Create security group**.

1. **Basic details**: *Security group name* `pyspark-stack-sg` · *Description* `SSH + web Airflow a mi
   IP`. *VPC*: la **default**.
   > Si cambiás ese texto: AWS solo acepta `a-zA-Z0-9` y `. _-:/()#,@[]+=&;{}!$*` en las descripciones
   > de SG (las de las reglas incluidas). Un acento o una comilla simple y la creación falla con
   > `InvalidParameterValue`. Escribí "tunel", no "túnel".
2. **Inbound rules → Add rule**:
   - Regla 1: *Type* **SSH** (TCP 22) · *Source* **My IP** (te autocompleta tu `/32`).
   - Regla 2 (solo si vas a exponer la web, §4.6): *Type* **HTTPS** (TCP 443) · *Source* **My IP**.
3. **Outbound rules**: dejá la default (**All traffic** a `0.0.0.0/0`).
4. **Create security group**.

> **Verificá que NO haya inbound para 8082/9090/3000/9093/3100** (ni ningún otro puerto de UI):
> esas van solo por túnel SSH. La única UI publicable es Airflow por 443 (§4.6); la Spark UI vive en
> la consola de EMR Serverless, no en la EC2.

> **Si tu IP de cliente cambia** (IP dinámica): la EC2 ya tiene Elastic IP (§4.3), así que el
> *servidor* no cambia entre stop/start — lo que se desactualiza es **tu** `/32` como *Source* de las
> reglas 22/443 (una Elastic IP no arregla esto: es tu IP de casa/oficina, no la de la EC2). En vez de
> editarlo a mano cada vez (**VPC → Security groups → `pyspark-stack-sg` → Inbound rules → Edit**),
> corré este script desde tu máquina cuando cambie —o ponelo en un cron local (`*/15 * * * *`)—:
> actualiza el `/32` de las reglas 22 y 443 **sin tocar sus IDs** (usa `modify-security-group-rules`,
> idempotente), y salta el 443 si no lo expusiste.
>
> ```bash
> #!/usr/bin/env bash
> # scripts/update-sg-ip.sh — pone tu IP de cliente actual en las reglas 22 y 443 del SG.
> set -euo pipefail
> REGION="${AWS_REGION:-us-east-1}"
> SG_NAME="pyspark-stack-sg"
> MYIP="$(curl -s https://checkip.amazonaws.com)/32"
> echo "IP actual: $MYIP"
> SG_ID=$(aws ec2 describe-security-groups --region "$REGION" \
>   --filters "Name=group-name,Values=$SG_NAME" \
>   --query 'SecurityGroups[0].GroupId' --output text)
> for PORT in 22 443; do
>   RULE_ID=$(aws ec2 describe-security-group-rules --region "$REGION" \
>     --filters "Name=group-id,Values=$SG_ID" \
>     --query "SecurityGroupRules[?FromPort==\`$PORT\` && IsEgress==\`false\` && IpProtocol=='tcp'].SecurityGroupRuleId | [0]" \
>     --output text)
>   [ "$RULE_ID" = "None" ] || [ -z "$RULE_ID" ] && { echo "puerto $PORT: sin regla, salto"; continue; }
>   aws ec2 modify-security-group-rules --region "$REGION" --group-id "$SG_ID" \
>     --security-group-rules "SecurityGroupRuleId=$RULE_ID,SecurityGroupRule={IpProtocol=tcp,FromPort=$PORT,ToPort=$PORT,CidrIpv4=$MYIP,Description=auto-mi-ip}"
>   echo "puerto $PORT: regla $RULE_ID -> $MYIP"
> done
> ```
>
> Necesita en tu usuario/rol local los permisos `ec2:DescribeSecurityGroups`,
> `ec2:DescribeSecurityGroupRules` y `ec2:ModifySecurityGroupRules`.

### 4.2 Key pair + rol IAM de la EC2

**Key pair** — Consola: **EC2 → Key pairs → Actions → Import key pair**.

- *Name* `pyspark-stack-key`.
- *Public key contents*: pegá el contenido de `~/.ssh/pyspark_stack.pub` (el del prerrequisito).
- **Import key pair**.

**Rol IAM de la EC2** (para entrar por SSM sin abrir puertos, y luego para S3/EMR/secretos) —
Consola: **IAM → Roles → Create role**.

1. *Trusted entity type*: **AWS service** · *Use case*: **EC2** → **Next**.
2. *Add permissions*: buscá y marcá **`AmazonSSMManagedInstanceCore`** (habilita el agente SSM, que
   usa toda la §6). → **Next**.
3. *Role name* `pyspark-stack-ec2-role` → **Create role**.

> Al **asignarle este rol a la EC2** en el wizard de lanzamiento (§4.3), la consola crea sola el
> *instance profile* homónimo. No hay que crearlo aparte.
>
> A este rol le vas a **ir agregando** políticas inline a lo largo de la guía: S3 (§5.2), EMR
> Serverless + invocar la Lambda startstop (§5.4), leer secretos de SSM (§7), Route 53 para el cert
> (§4.6) y Athena (§10). Todas van a **IAM → Roles → `pyspark-stack-ec2-role` → Add permissions →
> Create inline policy → pestaña JSON**.

### 4.3 EC2 + EBS + user_data + Elastic IP

**Lanzar la instancia** — Consola: **EC2 → Instances → Launch instances**.

1. **Name and tags**: *Name* `pyspark-stack-node`. Clic en **Add additional tags → Add tag**:
   `AutoStartStop` = `true` (la Lambda de §4.4 filtra por este tag).
2. **Application and OS Images (AMI)**: **Amazon Linux 2023** (x86_64). Evitá las variantes
   *minimal*/*ECS* (no traen el agente SSM que usa la §6).
3. **Instance type**: **t3.large** (2 vCPU / 8 GB — solo orquesta; Spark corre en EMR Serverless).
4. **Key pair (login)**: `pyspark-stack-key`.
5. **Network settings → Edit**:
   - *VPC*: la default · *Subnet*: cualquiera (recordá la AZ, la vas a necesitar para el volumen).
   - *Firewall (security groups)*: **Select existing security group** → `pyspark-stack-sg`.
6. **Configure storage**:
   - Root: **40 GiB**, *Volume type* **gp3**, *Encrypted* **Yes**.
   - **Add new volume**: **30 GiB**, **gp3**, *Encrypted* **Yes**, *Device name* `/dev/xvdf`.
     (gp3 crece online sin downtime; empezá chico y crecé cuando la alerta `HostDiskAlmostFull` avise.)
7. **Advanced details**:
   - **IAM instance profile** → `pyspark-stack-ec2-role`.
   - **Metadata version** → **V2 only (token required)** (IMDSv2 obligatorio).
   - **Metadata response hop limit** → **2** (sin esto los contenedores no alcanzan el IMDS y
     `s3a://` con rol IAM falla por credenciales).
   - **User data** → pegá el script de abajo tal cual.
8. **Launch instance**.

**`user_data`** (instala Docker + prepara el disco de datos `/data`) — pegalo en *User data*:

```bash
#!/bin/bash
set -euxo pipefail
dnf update -y && dnf install -y docker git && systemctl enable --now docker

# Versión PINEADA (mismo criterio que las imágenes por @sha256): un boot de hoy y uno de dentro
# de 6 meses instalan lo mismo. Actualizala a propósito, no dejes que "latest" decida por vos.
COMPOSE_VERSION=v5.3.1
DOCKER_CONFIG=/usr/local/lib/docker
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -fSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
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
  # documenta), y esta EC2 para y arranca todos los días. Si el nombre baila, `nofail` hace
  # exactamente lo que promete —no falla, no monta— y los servicios que escriben en /data quedan
  # en crash-loop por permisos sobre un /data vacío del disco root.
  echo "UUID=$(blkid -s UUID -o value "$DATA_DEV") /data xfs defaults,nofail 0 2" >> /etc/fstab
  chown -R ec2-user:ec2-user /data
  # Bind mounts del compose de prod. Prometheus (65534), Grafana (472) y Loki (10001) corren
  # sin privilegios: sin este chown quedan en crash-loop por "permission denied".
  mkdir -p /data/postgres /data/prometheus /data/grafana /data/loki
  chown 65534:65534 /data/prometheus
  chown 472:472     /data/grafana
  chown 10001:10001 /data/loki
fi
echo 'vm.max_map_count=262144' > /etc/sysctl.d/99-pyspark.conf && sysctl --system
```

**Etiquetar el volumen de datos** (el wizard no lo etiqueta, y sin el tag el DLM de §5.3 no respalda
nada) — Consola: **EC2 → Volumes** → seleccioná el volumen de **30 GiB** de tu instancia → pestaña
**Tags → Manage tags → Add tag**: `Name` = `pyspark-stack-data`.

**Elastic IP** (sin ella, cada stop/start del ahorro cambiaría la IP pública y romperían los túneles)
— Consola: **EC2 → Elastic IPs → Allocate Elastic IP address** → **Allocate**. Luego, con la EIP
seleccionada: **Actions → Associate Elastic IP address** → *Instance* `pyspark-stack-node` →
**Associate**. Anotá la IP: es tu `IP` para todos los `ssh`/`rsync` de la guía.

> Verificá (CLI, opcional): la instancia queda `running` con IMDSv2 `required` y el agente SSM
> `Online` a los pocos minutos:
> ```bash
> aws ec2 describe-instances --filters Name=tag:Name,Values=pyspark-stack-node \
>   --query 'Reservations[].Instances[].{estado:State.Name,imdsv2:MetadataOptions.HttpTokens}'
> ```

### 4.4 Automatización: Lambda startstop + EventBridge Scheduler

En vez de apagar la EC2 a mano, una Lambda la prende/apaga y EventBridge Scheduler la dispara por
cron. La Lambda no apaga a ciegas: antes consulta **si hay DAG runs activos en Airflow** (guardia
anti-corte) y, si los hay, no apaga — así el apagado es *job-aware* (§12).

**Paso 1 — Crear la función.** Consola: **Lambda → Create function**.

- *Author from scratch* · *Function name* `pyspark-stack-startstop` · *Runtime* **Python 3.12** ·
  *Architecture* x86_64 → **Create function**.
- En el editor de código, reemplazá `lambda_function.py` por el código de abajo (guardalo también en
  el repo como `lambdas/startstop.py`). Luego **Deploy**.

```python
# lambdas/startstop.py
import os
import time
import boto3

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")

def _dags_activos(instance_id):
    """Cuenta los DAG runs en estado 'running' DENTRO de la EC2, vía SSM SendCommand.
    Guardia anti-corte: si hay alguno, NO apagamos. Ante cualquier duda (comando fallido,
    salida no numérica) es conservador y devuelve >0 → no apagar."""
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
    event = {"action": "start"} | {"action": "stop"}. El stop es JOB-AWARE."""
    action   = event.get("action", "stop")
    tag_key  = os.environ.get("TAG_KEY", "AutoStartStop")
    tag_val  = os.environ.get("TAG_VALUE", "true")

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
        activos = _dags_activos(ids[0])       # un solo nodo pyspark-stack-node
        if activos > 0:
            return {"msg": f"{activos} DAG run(s) activos, no apago", "instances": ids}
        ec2.stop_instances(InstanceIds=ids)

    return {"action": action, "instances": ids}
```

**Paso 2 — Handler, timeout y variables.**

- *Runtime settings → Edit* → **Handler** = `lambda_function.handler` (el código define `def
  handler`, no el `lambda_handler` que asume la consola).
- *Configuration → General configuration → Edit* → **Timeout** = **2 min** (el guard job-aware espera
  al SSM SendCommand).
- *Configuration → Environment variables → Edit* → agregá `TAG_KEY=AutoStartStop` y
  `TAG_VALUE=true`.

**Paso 3 — Permisos (IAM inline policy en el rol de ejecución de la Lambda).** *Configuration →
Permissions* → clic en el **Role name** (te lleva a IAM) → **Add permissions → Create inline policy →
JSON** → pegá (reemplazá `<acct>` y el `i-xxxx` de tu instancia):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "DescribeInstances", "Effect": "Allow",
      "Action": ["ec2:DescribeInstances"], "Resource": "*" },
    { "Sid": "StartStopTagged", "Effect": "Allow",
      "Action": ["ec2:StartInstances", "ec2:StopInstances"], "Resource": "*",
      "Condition": { "StringEquals": { "aws:ResourceTag/AutoStartStop": "true" } } },
    { "Sid": "SsmSend", "Effect": "Allow",
      "Action": ["ssm:SendCommand"],
      "Resource": [
        "arn:aws:ec2:us-east-1:<acct>:instance/i-xxxxxxxxxxxxxxxxx",
        "arn:aws:ssm:us-east-1::document/AWS-RunShellScript"
      ] },
    { "Sid": "SsmGet", "Effect": "Allow",
      "Action": ["ssm:GetCommandInvocation"], "Resource": "*" }
  ]
}
```

*Policy name* `startstop-policy` → **Create policy**. (El permiso de escribir logs a CloudWatch ya lo
trae el *basic execution role* que la consola creó con la función.)

**Paso 3b — Log retention (auditoría §1.2).** **CloudWatch → Log groups** → buscá
`/aws/lambda/pyspark-stack-startstop` (Lambda lo crea solo en la primera invocación) →
**Actions → Edit retention setting** → **14 days** (por defecto es *Never expire*).

**Paso 4 — Los dos schedules (cron).** Consola: **Amazon EventBridge → Scheduler → Schedules →
Create schedule** (×2). Para cada uno:

- *Schedule name* — `pyspark-stack-start` / `pyspark-stack-stop`.
- *Schedule pattern*: **Recurring schedule** · *Cron-based* · *Flexible time window* **Off** ·
  *Timezone* **UTC**.
  - start: `cron(0 11 ? * MON-FRI *)`  (08:00 ART)
  - stop:  `cron(0 22 ? * MON-FRI *)`  (19:00 ART)
- *Target*: **AWS Lambda → Invoke** → *Function* `pyspark-stack-startstop` → *Payload*:
  - start: `{"action": "start"}`
  - stop:  `{"action": "stop"}`
- *Permissions*: **Create a new role for this schedule** (la consola crea sola el rol que invoca la
  Lambda) → **Create schedule**.

> Los crons quedan activos ya: esa misma noche la EC2 se apaga, y como `docker-compose.prod.yml`
> (§4.5) ya trae `restart: unless-stopped` en todos sus servicios desde el arranque mínimo, todo el
> stack vuelve solo al prender.

**Por qué apagar/prender no degrada nada** (igual que en la guía 02): `t3.large` burstable es lo
correcto (la caja ya no corre Spark, solo orquesta, carga a ráfagas — el perfil de los CPU credits de
`t3`); EBS `gp3` rinde constante antes y después del ciclo; los datos persisten en EBS al *stop*; y el
stack vuelve solo porque Docker arranca en boot y `restart: unless-stopped` (ya en el archivo desde
§4.5) relevanta los contenedores. Lo único más lento es la primera corrida de Spark tras idle (~1-2
min): es el *cold start* de EMR Serverless, no una degradación sostenida.

> Verificá (CLI): `aws lambda invoke --function-name pyspark-stack-startstop
> --cli-binary-format raw-in-base64-out --payload '{"action":"stop"}' /dev/stdout` debe **listar tu
> instancia**, no `{"msg":"no instances tagged"}` (si eso, revisá el tag `AutoStartStop`). Con el
> guard job-aware, si hay DAGs corriendo devuelve `{"msg":"N DAG run(s) activos, no apago"}` — es lo
> esperado; probá el stop sin DAGs en vuelo.

### 4.5 Desplegar, subir código y túnel SSH

Con la EC2 arriba, subís el proyecto y levantás el stack. Estos pasos corren en **tu máquina**
(usando el CLI/ssh); `IP` es la Elastic IP de §4.3.

**Antes de subir nada, creá `docker-compose.prod.yml` en la raíz de tu repo LOCAL** (todavía no
existe). A diferencia de `docker-compose.yml` (el del dev local), **este no es un override que se
fusiona**: es un archivo standalone y autosuficiente, sin Spark, sin HDFS y sin Jupyter (acá no se
usa: el ETL corre por Airflow + EMR Serverless + papermill headless, sin UI interactiva; para
explorar datos a mano usá el Jupyter del stack local, `docs/01`). Es obligatorio, no opcional: si le
hicieras `docker compose up` pelado a `docker-compose.yml` (el de dev), levantarías Spark standalone
y HDFS en esta EC2 orquestadora — justo lo que evita EMR Serverless (§1, guía 02 §6.4).

Esta es la versión **mínima** (Airflow + Postgres); §7 (secretos) y §9 (monitoreo) van a pedirte que
**reemplaces todo el archivo** por una versión más completa (la final está en §11.1) — no lo vayas
parcheando a mano, cada sección te da el archivo entero de nuevo:

```yaml
# docker-compose.prod.yml — stack de PRODUCCIÓN, standalone (un solo archivo, sin merge).
# Arranque mínimo: Airflow + Postgres. Sin Spark/HDFS (van a EMR Serverless, §5.4) y sin Jupyter
# (no se usa en prod). §7/§9 amplían este mismo archivo — versión final en §11.1.
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
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
    AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW_JWT_SECRET:-change-me-in-prod}'
    AIRFLOW_UID: 50000
  volumes:
    - ./dags:/opt/airflow/dags
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
    deploy: { resources: { limits: { memory: 512m } } }
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}
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

```bash
IP=<tu-elastic-ip>

# Esperar a que la instancia pase los status checks (primer boot + user_data tardan unos minutos)
aws ec2 wait instance-status-ok --instance-ids <i-xxxxxxxx>

# Subir el proyecto (docker-compose.prod.yml creado arriba incluido).
# --exclude '.env': el .env local (dev) no debe pisar el de prod,
# que lo genera load-secrets.sh en la EC2 desde SSM (§7).
rsync -avz --exclude '.git' --exclude '.env' --exclude '__pycache__' \
  -e "ssh -i ~/.ssh/pyspark_stack" ./ ec2-user@$IP:/home/ec2-user/pyspark_stack/

# Confirmar que el user_data terminó: Docker Compose instalado y /data montado.
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'cloud-init status --wait && docker compose version && df -h /data | tail -1'

# Levantar el stack base, sin Spark/HDFS (el completo con monitoreo/secretos es el runbook de §15)
ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
  'cd pyspark_stack && docker compose -f docker-compose.prod.yml up -d --build'

# Túnel a las UIs
ssh -i ~/.ssh/pyspark_stack -L 8082:localhost:8082 ec2-user@$IP
```

UIs (con el túnel abierto): Airflow `localhost:8082` — o, si exponés la web por HTTPS (§4.6), directo
en `https://airflow.midominio.com` sin túnel. Spark ya no corre en la EC2 (los jobs van a EMR
Serverless — su UI y logs se ven desde la consola de EMR / CloudWatch / S3, §9). No hay Jupyter en
prod: la exploración interactiva queda para el stack local (`docs/01`).

> Esto es el núcleo, no el final: la infra se arma incrementalmente. Seguí con S3 (§5), orquestación
> (§6), secretos (§7) y monitoreo (§9); cada una te da una versión más completa de
> `docker-compose.prod.yml` para reemplazar el creado arriba (Spark/HDFS/Jupyter nunca estuvieron en
> el archivo). El arranque **real** de producción es el runbook de §15:
> `./scripts/load-secrets.sh && docker compose -f docker-compose.prod.yml up -d`.

> **Sin CLI en tu máquina** podés hacer el rsync igual (solo necesitás `ssh`/`rsync`, que no son AWS
> CLI). El `aws ec2 wait` reemplazalo mirando en la consola **EC2 → Instances** que *Status check*
> diga **2/2 checks passed**.

### 4.6 Exponer la web de Airflow (HTTPS nativo, solo tu IP)

Opcional pero recomendado para *seguir los DAGs* desde el navegador sin túnel. Publica **solo la web
de Airflow** por **HTTPS (443) restringida a tu IP**; el resto sigue por túnel. Requiere un **dominio
propio con hosted zone en Route 53**.

Cuatro piezas:

1. **DNS** — un `A record` `airflow.midominio.com → EIP` de la EC2.
2. **Cert** — Let's Encrypt por **DNS-01** con `certbot/dns-route53`: usa el **rol de la EC2** para
   crear el TXT del reto en Route 53. No abre el puerto 80.
3. **TLS nativo** — el `api-server` de Airflow sirve HTTPS él mismo (`AIRFLOW__API__SSL_CERT/KEY`).
4. **SG** — 443 abierto solo a tu IP (la regla 2 de §4.1).

> **El gotcha (documentado oficialmente).** En Airflow 3 el `api-server` sirve en el **mismo puerto
> 8080** la UI, la API REST **y** la *Task Execution API* que el scheduler usa internamente. Al
> activar TLS, ese tráfico interno también pasa a HTTPS; los contenedores se hablan por el hostname
> `airflow-apiserver`, que **no** está en el cert → la verificación TLS fallaría y las tasks dejarían
> de correr. **La solución para un cert Let's Encrypt** es darle al contenedor un **alias de red = el
> FQDN del cert** y apuntar `EXECUTION_API_SERVER_URL` a ese FQDN. Así el hostname interno pasa a ser
> `airflow.midominio.com` (que sí está en el cert) y la verificación pasa contra las CAs públicas.

**Paso 1 — A record.** Consola: **Route 53 → Hosted zones → `midominio.com` → Create record**.

- *Record name* `airflow` · *Record type* **A** · *Value* la **Elastic IP** de la EC2 · *TTL* **300**
  → **Create records**.

**Paso 2 — Permiso Route 53 al rol de la EC2** (para que certbot resuelva el reto DNS-01). Necesitás
el **Hosted Zone ID** (Route 53 → Hosted zones → tu zona → columna *Hosted zone ID*, algo como
`Z0123...`). Consola: **IAM → Roles → `pyspark-stack-ec2-role` → Add permissions → Create inline
policy → JSON**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "Route53ChangeRecordsInZone", "Effect": "Allow",
      "Action": ["route53:ChangeResourceRecordSets"],
      "Resource": ["arn:aws:route53:::hostedzone/Z0123456789ABCDEFGHIJ"] },
    { "Sid": "Route53ReadForDns01", "Effect": "Allow",
      "Action": ["route53:GetChange", "route53:ListHostedZones", "route53:ListResourceRecordSets"],
      "Resource": ["*"] }
  ]
}
```

*Policy name* `ec2-route53-certbot` → **Create policy**.

**Paso 3 — Emitir el cert (una vez, en la EC2).** Usa el rol de la EC2 vía IMDS (sin keys) y no abre
el puerto 80:

```bash
DOMAIN="airflow.midominio.com"
EMAIL="tu@email.com"
dig +short "$DOMAIN"     # debe devolver la EIP (el A record ya está)

ssh -i ~/.ssh/pyspark_stack ec2-user@$IP "
  sudo docker run --rm -v /data/certs:/etc/letsencrypt certbot/dns-route53 certonly \
    --dns-route53 -d '$DOMAIN' -m '$EMAIL' --agree-tos -n &&
  sudo chmod -R g+rX /data/certs   # el api-server corre con gid 0 (grupo root): así lee el privkey
"
```

El cert queda en `/data/certs/live/$DOMAIN/{fullchain.pem,privkey.pem}` (en el EBS, sobrevive al
stop/start).

**Paso 4 — Editá el `airflow-apiserver` de tu `docker-compose.prod.yml` directamente** (es un solo
archivo, no hay nada que fusionar). El FQDN viaja como `AIRFLOW_DOMAIN` (no es secreto). Agregalo al
`.env` en la EC2 (`echo "AIRFLOW_DOMAIN=airflow.midominio.com" >> .env`) y reemplazá el bloque
`airflow-apiserver` por este — el `<<: *airflow-common-env` es el anchor **anidado** que ya tenés
dentro de `x-airflow-common` (§11.1): permite sumar las 3 claves de TLS sin repetir el resto del
environment a mano:

```yaml
services:
  airflow-apiserver:
    <<: *airflow-common
    container_name: airflow-apiserver
    command: api-server
    environment:
      <<: *airflow-common-env
      AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "https://${AIRFLOW_DOMAIN}:8080/execution/"
      AIRFLOW__API__SSL_CERT: /opt/airflow/certs/fullchain.pem
      AIRFLOW__API__SSL_KEY:  /opt/airflow/certs/privkey.pem
      AIRFLOW__API__BASE_URL: "https://${AIRFLOW_DOMAIN}"
    ports:
      - "8082:8080"                                      # túnel local (seguís pudiendo usarlo)
      - "443:8080"                                        # HTTPS público; el SG lo limita a tu IP
    volumes:
      - ./dags:/opt/airflow/dags                          # el `<<:` no mergea volumes, hay que repetirlo
      - /data/certs/live/${AIRFLOW_DOMAIN}:/opt/airflow/certs:ro
    networks:
      hadoopnet:
        aliases: ["${AIRFLOW_DOMAIN}"]                   # adentro, el cert matchea este nombre
    depends_on:
      airflow-db: { condition: service_healthy }
      airflow-init: { condition: service_completed_successfully }
```

**Paso 5 — Renovación automática (una vez, en la EC2).** `certbot renew` es no-op si faltan >30 días:

```bash
echo '0 3 * * 1 root docker run --rm -v /data/certs:/etc/letsencrypt certbot/dns-route53 renew --quiet && chmod -R g+rX /data/certs && docker restart airflow-apiserver' \
  | sudo tee /etc/cron.d/airflow-cert-renew
```

Entrás a `https://airflow.midominio.com` con el usuario **admin** y la password que generaste en SSM
(§7). Desde otra IP debe cortar (el SG solo deja 443 a tu `/32`). La restricción por IP es
defensa-en-profundidad **sobre** el login de Airflow.

> Alternativa Caddy (reverse-proxy con auto-cert): evita el gotcha del alias, pero su emisión
> automática necesita el **puerto 80 abierto al mundo** (o compilar Caddy con el módulo DNS-01). Con
> el SG cerrado a tu IP, el TLS nativo de arriba es más directo. El detalle está en la guía 02 §5.6.

---

## 5. Data lake en S3 + cómputo Spark

Sin HDFS, **todo el dato vive en S3**: data lake durable (`raw/ → curated/ → analytics/`). Los jobs
Spark de **EMR Serverless** lo leen/escriben con `s3a://` usando **su propio rol de ejecución**
(§5.4); las tasks Python puro de Airflow usan `s3://` con el **rol IAM de la EC2** — en ambos casos
sin access keys.

### 5.1 Buckets S3

Consola: **S3 → Create bucket** (×2). Región **us-east-1**. Los nombres de S3 son globales:

- `pyspark-stack-datalake-<acct>`
- `pyspark-stack-artifacts-<acct>`  (scripts + logs EMR + `deploy/`)

En **ambos**, durante la creación:

1. **Block Public Access**: dejá las **4 casillas activadas** (default).
2. **Bucket Versioning**: **Enable**.
3. **Default encryption**: **SSE-S3 (AES256)** (default).
4. **Create bucket**.

**Política solo-TLS** (en cada bucket) — *Permissions → Bucket policy → Edit* → pegá (ajustando el
nombre del bucket en los dos ARN):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
    "Resource": [
      "arn:aws:s3:::pyspark-stack-datalake-<acct>",
      "arn:aws:s3:::pyspark-stack-datalake-<acct>/*"
    ],
    "Condition": { "Bool": { "aws:SecureTransport": "false" } }
  }]
}
```

**Lifecycle (solo `datalake`)** — *Management → Create lifecycle rule*:

- *Rule name* `tiering` · *Rule scope*: **Apply to all objects in the bucket** (aceptá el aviso).
- *Lifecycle rule actions*: **Move current versions of objects between storage classes**:
  - **Standard-IA** a los **30** días.
  - **Glacier Instant Retrieval** a los **90** días.
- **Create rule**.

> (Opcional) *Create folder* para `raw/`, `curated/`, `analytics/` — también aparecen solos con la
> primera escritura.

> Verificá (CLI): `aws s3 ls | grep pyspark-stack` → los 2 buckets.

### 5.2 IAM: permitir S3 a la EC2

Para que las tasks Python puro de Airflow (pandas/`s3fs`) lean/escriban S3 con el instance profile,
sin keys. Consola: **IAM → Roles → `pyspark-stack-ec2-role` → Add permissions → Create inline policy →
JSON**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "S3ReadWrite", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::pyspark-stack-datalake-<acct>/*",
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>/*"
      ] },
    { "Sid": "S3List", "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::pyspark-stack-datalake-<acct>",
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>"
      ] }
  ]
}
```

*Policy name* `ec2-s3a` → **Create policy**. No hay que tocar la EC2: el rol ya está asociado y los
contenedores toman las credenciales al instante.

> Verificá (desde la EC2, para probar el instance profile y no tus keys locales):
> ```bash
> ssh -i ~/.ssh/pyspark_stack ec2-user@$IP \
>   'aws s3 cp /etc/hostname s3://pyspark-stack-datalake-<acct>/raw/smoke-iam.txt'
> ```

### 5.3 Backups: snapshots EBS automáticos (DLM)

`/data` (EBS gp3) guarda Postgres + datos de monitoreo: el estado que **no** vive en S3. Data
Lifecycle Manager toma snapshots automáticos y retiene los últimos N — cero código.

Consola: **EC2 → Elastic Block Store → Lifecycle Manager → Create lifecycle policy**.

1. *Policy type*: **EBS snapshot policy** → **Next**.
2. *Target resources*: **Volume** · *Target resource tags*: `Name` = `pyspark-stack-data`.
3. *IAM role*: **Default role** (la consola usa el service role de DLM).
4. *Schedule*: nombre `diario-7d` · *Frequency* **Daily**, cada **24 hours** a las **05:00 UTC** ·
   *Retention type* **Count** = **7** · *Copy tags from source*: **Enable**.
5. *Policy status*: **Enable policy** → **Create policy**.

> Restore: creás un volumen desde el snapshot y lo montás en `/data`. S3 ya está versionado, así que
> el data lake tiene su propia protección.

> Verificá (CLI): `aws dlm get-lifecycle-policies --query 'Policies[].State'` → `["ENABLED"]`.

### 5.4 Cómputo Spark: EMR Serverless

Spark salió de la EC2. Los jobs corren en **EMR Serverless**: arranca solo al recibir un job, escala
a cero tras 15 min idle y paga solo mientras computa. Airflow dispara cada job con
`EmrServerlessStartJobOperator` (§12) — nunca corre `spark-submit` local.

**Paso 1 — Log group para los logs del job.** Consola: **CloudWatch → Log groups → Create log group**:
*Name* `/aws/emr-serverless/pyspark-stack` · *Retention* **30 days** → **Create**.

**Paso 2 — Rol de ejecución del job (least-privilege).** EMR Serverless asume **este** rol para
correr el Spark; solo toca los dos buckets y escribe sus logs.

Consola: **IAM → Roles → Create role** → *Trusted entity type* **Custom trust policy** → pegá:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "emr-serverless.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

→ **Next** → (sin managed policies) **Next** → *Role name* `pyspark-stack-emr-serverless-job` →
**Create role**. Luego, en ese rol: **Add permissions → Create inline policy → JSON**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "S3ReadWriteData", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::pyspark-stack-datalake-<acct>/*",
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>/*"
      ] },
    { "Sid": "S3List", "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::pyspark-stack-datalake-<acct>",
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>"
      ] },
    { "Sid": "CloudWatchLogs", "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": ["arn:aws:logs:us-east-1:<acct>:log-group:/aws/emr-serverless/*"] },
    { "Sid": "GlueCatalogIceberg", "Effect": "Allow",
      "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"],
      "Resource": [
        "arn:aws:glue:us-east-1:<acct>:catalog",
        "arn:aws:glue:us-east-1:<acct>:database/pyspark_stack_analytics",
        "arn:aws:glue:us-east-1:<acct>:table/pyspark_stack_analytics/*"
      ] }
  ]
}
```

*Policy name* `emr-serverless-job` → **Create policy**. El `GlueCatalogIceberg` es el único permiso
de Glue que necesita este rol: las tablas `curated/`/`analytics/` son **Iceberg** (§16.1), y el job
Spark tiene que poder registrar/actualizar su metadata en el catálogo cada vez que escribe.

**Paso 2b — Base de datos en el Glue Data Catalog.** Es el catálogo lógico donde Iceberg registra las
tablas — hace falta exista o no la sección de Athena (§10), porque lo usa el job Spark, no solo
Athena. Consola: **Glue → Data Catalog → Databases → Add database** → nombre
`pyspark_stack_analytics` → **Create database**.

**Paso 3 — La aplicación EMR Serverless.** Consola: **EMR → EMR Serverless → Get started / Create and
launch application** (o **Applications → Create application**).

- *Name* `pyspark-stack-spark` · *Type* **Spark** · *Release version* **emr-7.5.0**.
- *Application setup options*: **Use custom settings**:
  - **Pre-initialized capacity**: dejala en **0** (para escalar a cero de verdad).
  - **Application behavior**: **Auto-start** *On*; **Auto-stop** *On*, *idle timeout* **15 minutes**.
  - **Maximum capacity**: **16 vCPU / 64 GB** (techo de gasto).
  - **Network connections**: dejala **sin VPC** (los jobs solo tocan S3). Agregá VPC solo si el job
    accede a recursos privados de tu red (RDS privada, etc.).
- **Create application**. Anotá el **Application ID** (`00xxxxxxxxxxxxxx`): lo usan los DAGs.

**Paso 4 — Extender el rol de la EC2** para que Airflow **envíe/pollee** jobs y **pase** el rol de
ejecución a EMR. El `iam:PassRole` con `iam:PassedToService` es la barrera. Consola: **IAM → Roles →
`pyspark-stack-ec2-role` → Add permissions → Create inline policy → JSON** (reemplazá el
`<emr-app-id>`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "EmrServerlessSubmit", "Effect": "Allow",
      "Action": [
        "emr-serverless:StartJobRun", "emr-serverless:GetJobRun",
        "emr-serverless:StartApplication", "emr-serverless:GetApplication"
      ],
      "Resource": [
        "arn:aws:emr-serverless:us-east-1:<acct>:/applications/<emr-app-id>",
        "arn:aws:emr-serverless:us-east-1:<acct>:/applications/<emr-app-id>/jobruns/*"
      ] },
    { "Sid": "PassEmrJobRole", "Effect": "Allow",
      "Action": ["iam:PassRole"],
      "Resource": "arn:aws:iam::<acct>:role/pyspark-stack-emr-serverless-job",
      "Condition": { "StringEquals": { "iam:PassedToService": "emr-serverless.amazonaws.com" } } }
  ]
}
```

*Policy name* `ec2-emr-serverless` → **Create policy**.

**Paso 5 — Permiso para que el DAG apague la EC2 al terminar** (task `trigger_stop`, §12). Otra
inline policy en el mismo rol EC2:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "InvokeStartStopLambda", "Effect": "Allow",
    "Action": ["lambda:InvokeFunction"],
    "Resource": "arn:aws:lambda:us-east-1:<acct>:function:pyspark-stack-startstop"
  }]
}
```

*Policy name* `ec2-invoke-startstop` → **Create policy**.

**Paso 6 — Subir los entrypoints PySpark a S3.** Los entrypoints reales viven en el repo bajo
`spark-apps/emr/` (`customer_etl.py`, `wordcount.py` — su código completo está en §11) y en S3 bajo
`s3://<artifacts>/emr/`:

```bash
ACCT=$(aws sts get-caller-identity --query Account --output text)
aws s3 sync spark-apps/emr/ "s3://pyspark-stack-artifacts-$ACCT/emr/"
```

> Sin CLI: **S3 → bucket artifacts → Create folder `emr/` → Upload** y subís los `.py` a mano.

**Probar un job a mano (opcional).** Así lo arma el operator de Airflow; equivalente CLI:

```bash
aws emr-serverless start-job-run \
  --application-id "<emr-app-id>" \
  --execution-role-arn "arn:aws:iam::<acct>:role/pyspark-stack-emr-serverless-job" \
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

La config de Spark va **por-job** (en `sparkSubmitParameters`), no en un `spark-defaults.conf`: en EMR
Serverless no hay caja donde montarlo. EMR escribe los logs a S3 (`emr/logs/`) y a CloudWatch, y
expone la Spark UI de cada corrida desde la consola de EMR.

> Verificá (CLI):
> `aws emr-serverless list-applications --query 'applications[?name==\`pyspark-stack-spark\`].[id,state]'`

### 5.5 S3 VPC Gateway Endpoint

Para que el tráfico **EC2↔S3** no salga a internet (menor superficie, y **gratis** — el
gateway endpoint de S3 no cobra por hora ni por GB). Consola: **VPC → Endpoints → Create endpoint**.

> **No cubre a EMR Serverless.** Un gateway endpoint inyecta una ruta en la route table de tu VPC,
> así que solo afecta tráfico que sale de ENIs de esa VPC. La app EMR se crea sin configuración de
> red, o sea que corre en la red administrada de AWS, fuera de tu VPC: no hay ENI tuya y el endpoint
> no le aplica. Solo aplicaría si le configuraras subnets.

1. *Name* `pyspark-stack-s3-endpoint`.
2. *Service category*: **AWS services** → buscá `com.amazonaws.us-east-1.s3` con *Type* **Gateway**
   (no Interface).
3. *VPC*: la **default**.
4. *Route tables*: marcá **todas** las de la VPC default (así el tráfico a S3 se enruta por el
   endpoint).
5. *Policy*: **Full access** (los buckets ya están cerrados con sus bucket policies) → **Create
   endpoint**.

> Verificá (CLI): `aws ec2 describe-vpc-endpoints --query
> 'VpcEndpoints[?ServiceName==\`com.amazonaws.us-east-1.s3\`].[VpcEndpointId,State]'`

---

## 6. Orquestación: Lambda trigger-airflow

Airflow corre dentro de la EC2. Para dispararlo desde AWS (por cron o cuando llega un archivo a S3)
se usa una **Lambda que ejecuta `airflow dags trigger` vía SSM `SendCommand`** — sin abrir puertos ni
depender de la web.

### 6.1 Lambda que dispara los DAGs vía SSM (con retry si la EC2 está apagada + contrato de datos)

**Paso 1 — Crear la función.** Consola: **Lambda → Create function** → *Function name*
`pyspark-stack-trigger-airflow` · *Runtime* **Python 3.12** → **Create function**. Pegá el código
(guardalo en el repo como `lambdas/trigger_airflow.py`) y **Deploy**. Dos mejoras sobre la versión
mínima, idénticas a la guía 02 §7.1: **(a)** si la EC2 está apagada, la Lambda la prende y deja que
el reintento (SQS para eventos S3, retry async de Lambda para el cron) la reintente sola en unos
minutos, en vez de fallar en silencio; **(b)** un **contrato de datos** liviano (stdlib, sin Lambda
Layers) rechaza archivos con columnas faltantes antes de gastar en cómputo de EMR:

```python
# lambdas/trigger_airflow.py
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

# Contrato mínimo por archivo: columnas/keys requeridas. La validación de contenido de verdad es
# Great Expectations (§9 de la guía 02 §20); esto es un gate barato, solo mira el header/las keys.
CONTRACTS = {
    "orders.csv":    {"order_id", "customer_id", "product_id", "quantity", "order_date"},
    "customers.csv": {"customer_id", "customer_name", "city", "state", "signup_date"},
    "products.json": {"product_id", "category", "unit_price"},
}


class ContractViolation(Exception):
    pass


def _peek_columns(bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-2047")["Body"].read()
    head = body.decode("utf-8", errors="replace")
    if key.endswith(".csv"):
        return set(next(csv.reader([head.splitlines()[0]])))
    if key.endswith(".json"):
        try:
            data = json.loads(head)
        except json.JSONDecodeError:
            return None  # muestra cortada a mitad de objeto: no bloqueamos por un falso positivo
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
    state = ec2.describe_instances(InstanceIds=[instance_id]) \
               ["Reservations"][0]["Instances"][0]["State"]["Name"]
    if state == "stopped":
        ec2.start_instances(InstanceIds=[instance_id])
        return False
    if state != "running":
        return False
    infos = ssm.describe_instance_information(
        Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
    )["InstanceInformationList"]
    return bool(infos) and infos[0]["PingStatus"] == "Online"


def _disparar_dag(dag, conf, run_id=None):
    trigger = f"airflow dags trigger {dag}"
    if run_id:
        # Determinístico por archivo (auditoría §1.3 de la guía 02): un reintento del MISMO objeto
        # produce el MISMO run_id — si el trigger anterior ya tuvo éxito (SendCommand es
        # fire-and-forget, no lo confirmamos), este segundo intento falla en vez de crear un
        # segundo dagrun duplicado para el mismo archivo.
        trigger += f" --run-id '{run_id}'"
    if conf:
        trigger += f" --conf '{json.dumps(conf)}'"
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
        Comment=f"trigger airflow dag {dag}",
        Parameters={"commands": [f"docker exec airflow-scheduler {trigger}"]},
    )
    return resp["Command"]["CommandId"]


def handler(event, context):
    """Cron (async directo): {"dag": "customer_etl_emr"}.
    Evento S3 vía la cola SQS primaria (§6.3): {"Records": [{"body": "<S3 event JSON>"}]}."""
    bucket = key = run_id = None
    if "Records" in event and event["Records"] and "body" in event["Records"][0]:
        rec = json.loads(event["Records"][0]["body"])["Records"][0]["s3"]
        key = urllib.parse.unquote_plus(rec["object"]["key"])
        bucket = rec["bucket"]["name"]
        dag, conf = DEFAULT_DAG, {"bucket": bucket, "key": key}
        run_id = "s3-" + hashlib.sha256(f"{bucket}/{key}".encode()).hexdigest()[:16]
    else:
        dag, conf = event.get("dag", DEFAULT_DAG), {}

    try:
        if bucket and key:
            _validar_contrato(bucket, key)
    except ContractViolation as e:
        print(f"RECHAZADO por contrato de datos: {e}")
        return {"status": "rejected", "reason": str(e)}

    if not _ec2_lista(INSTANCE_ID):
        raise RuntimeError(f"EC2 {INSTANCE_ID} no está lista todavía; reintentar")

    return {"dag": dag, "conf": conf, "commandId": _disparar_dag(dag, conf, run_id)}
```

**Paso 2 — Handler y variables.** *Runtime settings → Edit* → **Handler** = `lambda_function.handler`.
*Configuration → General → Edit* → **Timeout** = **1 min**. *Environment variables*:
`INSTANCE_ID=<i-xxxxxxxx>` (tu instancia) y `DEFAULT_DAG=customer_etl_emr` (el DAG de
producción EMR, §12 — no el flujo dev local).

**Paso 3 — Permisos (inline policy en el rol de ejecución de la Lambda).** *Configuration →
Permissions* → clic en el role → **Add permissions → Create inline policy → JSON**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "SsmSend", "Effect": "Allow",
      "Action": ["ssm:SendCommand"],
      "Resource": [
        "arn:aws:ec2:us-east-1:<acct>:instance/i-xxxxxxxxxxxxxxxxx",
        "arn:aws:ssm:us-east-1::document/AWS-RunShellScript"
      ] },
    { "Sid": "SsmGet", "Effect": "Allow",
      "Action": ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ssm:DescribeInstanceInformation"],
      "Resource": "*" },
    { "Sid": "DescribeEc2", "Effect": "Allow",
      "Action": ["ec2:DescribeInstances"], "Resource": "*" },
    { "Sid": "StartEc2IfStopped", "Effect": "Allow",
      "Action": ["ec2:StartInstances"],
      "Resource": "arn:aws:ec2:us-east-1:<acct>:instance/i-xxxxxxxxxxxxxxxxx" },
    { "Sid": "ContractPeek", "Effect": "Allow",
      "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::pyspark-stack-datalake-<acct>/raw/*" },
    { "Sid": "ConsumeTriggerQueue", "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
      "Resource": "arn:aws:sqs:us-east-1:<acct>:pyspark-stack-trigger-events" }
  ]
}
```

*Policy name* `trigger-airflow` → **Create policy**. Nota: `StartInstances` va con ARN específico,
no `"*"` — a diferencia de `DescribeInstances`, sí admite scoping por recurso (auditoría §1.1).

**Paso 3b — Log retention + límite de concurrencia (auditoría §1.2/§3.1).** *Configuration → General
configuration → Edit*:

- **Reserved concurrency** → **Reserve 2** (si suben muchos archivos a la vez a `raw/`, como máximo
  2 invocaciones corren en paralelo — el resto queda esperando en la cola SQS de §6.3, no se pierde
  ni se lanza como una avalancha de 50 jobs de EMR a la vez).

Y en **CloudWatch → Log groups**, buscá `/aws/lambda/pyspark-stack-trigger-airflow`
(Lambda lo crea solo en la primera invocación) → **Actions → Edit retention setting** → **14 days**
(por defecto queda **Never expire**, que acumula logs para siempre sin necesidad).

**Paso 4 — Dead-letter queue de la propia Lambda** (para el camino cron, invocación async): *Configuration
→ Asynchronous invocation → Edit* → *Dead-letter queue* → `pyspark-stack-trigger-airflow-dlq` (la de §16.1).
Para el camino de eventos S3, el redrive lo maneja la **cola** de §6.3, no esto — mismo DLQ final, dos
mecanismos según el transporte.

> **Por qué no un `time.sleep()` esperando a que la EC2 arranque.** Boot + agente SSM online tarda
> ~2-5 min. Bloquear la Lambda ese tiempo cuesta y arriesga el timeout. Devolver "todavía no" y
> dejar que el transporte reintente es gratis: SQS ya tiene *visibility timeout* + redrive, y el
> cron ya tiene el retry async de Lambda.

> Verificá (CLI): el agente SSM `Online` es prerrequisito de toda la §6.
> ```bash
> aws ssm describe-instance-information --query "InstanceInformationList[?InstanceId=='<i-xxxx>'].PingStatus"  # ["Online"]
> aws lambda invoke --function-name pyspark-stack-trigger-airflow \
>   --cli-binary-format raw-in-base64-out --payload '{"dag":"customer_etl_emr"}' /dev/stdout
> ```
> O en la consola: **Lambda → `pyspark-stack-trigger-airflow` → Test** con evento
> `{"dag": "customer_etl_emr"}`.

### 6.2 Disparo por cron

Consola: **EventBridge → Scheduler → Create schedule**.

- *Name* `pyspark-stack-daily-etl`.
- *Recurring* · *Cron-based* → `cron(0 12 ? * MON-FRI *)` (12:00 UTC, dentro de la ventana de
  encendido del auto start/stop, §4.4) · *Flexible time window* **Off** · *Timezone* **UTC**.
- *Target*: **AWS Lambda → Invoke** → `pyspark-stack-trigger-airflow` · *Payload*
  `{"dag": "customer_etl_emr"}` (el DAG de producción, §12).
- *Permissions*: **Create a new role for this schedule** → **Create schedule**.

> Verificá (CLI): `aws scheduler list-schedules --query 'Schedules[].Name'` → aparece
> `pyspark-stack-daily-etl`.

### 6.3 Disparo por evento (archivo nuevo en S3, vía SQS)

Cuando llega un archivo a `raw/`, S3 **no** invoca la Lambda directo: escribe un mensaje en una
cola **SQS primaria**, y la Lambda la consume. Esa vuelta por SQS es lo que le da a §6.1 su
reintento gratis cuando la EC2 está apagada: si el handler levanta una excepción, el mensaje no se
borra de la cola y vuelve a estar visible pasado el *visibility timeout* — se reprocesa solo, sin
que nadie haga nada.

**Paso 1 — Crear la cola.** Consola: **SQS → Create queue** → *Standard* → nombre
`pyspark-stack-trigger-events` → *Visibility timeout* **360 seconds** (6x el timeout de la Lambda, y
suficiente para cubrir un boot completo de la EC2, ~2-5 min) → *Dead-letter queue*: **Enabled**,
cola `pyspark-stack-trigger-airflow-dlq` (la de §16.1), *Maximum receives* **5** → **Create queue**.

**Paso 2 — Access policy de la cola** (para que S3 pueda escribirle) → en la cola → **Access policy**
→ pegá:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowS3Send", "Effect": "Allow",
    "Principal": { "Service": "s3.amazonaws.com" },
    "Action": "sqs:SendMessage",
    "Resource": "arn:aws:sqs:us-east-1:<acct>:pyspark-stack-trigger-events",
    "Condition": { "ArnEquals": { "aws:SourceArn": "arn:aws:s3:::pyspark-stack-datalake-<acct>" } }
  }]
}
```

**Paso 3 — Notificación S3 → SQS.** Consola: **S3 → bucket `pyspark-stack-datalake-<acct>` →
Properties → Event notifications → Create event notification** → *Event name* `on-upload-raw` ·
*Prefix* `raw/` · *Event types* **All object create events** · *Destination*: **SQS queue** →
`pyspark-stack-trigger-events` → **Save changes**.

**Paso 4 — La Lambda consume la cola.** Consola: **Lambda → `pyspark-stack-trigger-airflow` →
Configuration → Triggers → Add trigger** → **SQS** → `pyspark-stack-trigger-events` → *Batch size*
**1** (un archivo = una invocación: así uno rechazado o lento no bloquea a los demás) → **Add**.

> Los dos disparadores (cron y evento S3) apuntan al DAG de producción `customer_etl_emr` (§12) —
> no al `customer_etl_dag` dev-local, que usa el Spark/HDFS deshabilitado en prod. Tal como viene,
> `customer_etl_emr` tampoco lee `dag_run.conf` (procesa por `{{ ds }}`): por evento S3 corre pero
> ignora el archivo puntual. Para el camino event-driven real, hacé que lea
> `{{ dag_run.conf['bucket'] }}` / `{{ dag_run.conf['key'] }}` y los pase como
> `entryPointArguments` del `EmrServerlessStartJobOperator` (patrón de §12/§14).

> Verificá el retry: apagá la EC2 a mano, subí un archivo a `raw/`, y mirá **SQS → la cola →
> Monitoring** — el mensaje queda "in flight" (procesándose o esperando el próximo intento) hasta
> que la EC2 esté arriba y el DAG se dispare solo.

---

## 7. Secretos y parámetros

`docker-compose.prod.yml` trae los secretos con defaults débiles de dev (`${POSTGRES_PASSWORD:-airflow}`,
JWT `change-me-in-prod`, admin/admin). En producción se generan valores fuertes, se
guardan en **SSM Parameter Store** (SecureString, cifrado con KMS), y la EC2 los lee con su rol IAM y
los materializa en un `.env` efímero (chmod 600) antes de `docker compose up`. Cero secretos en git.

**Paso 1 — Generar los valores** (en tu máquina):

```bash
openssl rand -hex 24    # postgres_password
openssl rand -hex 32    # airflow_jwt_secret
openssl rand -hex 20    # airflow_admin_password
openssl rand -hex 20    # grafana_admin_password
```

> Todos sin caracteres especiales (hex): el compose los interpola sin comillas en el `bash -c` de
> `airflow-init`; caracteres como `( ) & *` romperían el comando en silencio.

**Paso 2 — Guardarlos en Parameter Store.** Consola: **Systems Manager → Parameter Store → Create
parameter** (×4). En cada uno: *Type* **SecureString** (KMS key **`alias/aws/ssm`** default),
*Value* el valor generado. **Nombres exactos** (son los que lee `load-secrets.sh`):

- `/pyspark-stack/postgres_password`
- `/pyspark-stack/airflow_jwt_secret`
- `/pyspark-stack/airflow_admin_password`
- `/pyspark-stack/grafana_admin_password`

El **SMTP de Alertmanager** va aparte (nunca en git): creá también
`/pyspark-stack/smtp_password` (SecureString) con el *app password* de Gmail
(https://myaccount.google.com/apppasswords).

**Paso 3 — Permitir a la EC2 leer los parámetros.** Consola: **IAM → Roles → `pyspark-stack-ec2-role`
→ Add permissions → Create inline policy → JSON**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "SsmReadParams", "Effect": "Allow",
      "Action": ["ssm:GetParameter", "ssm:GetParametersByPath"],
      "Resource": "arn:aws:ssm:us-east-1:<acct>:parameter/pyspark-stack/*" },
    { "Sid": "KmsDecrypt", "Effect": "Allow",
      "Action": ["kms:Decrypt"], "Resource": "*" }
  ]
}
```

*Policy name* `ec2-secrets` → **Create policy**.

**Paso 4 — Script que materializa el `.env` desde SSM** — `scripts/load-secrets.sh` (corre en la
EC2):

```bash
#!/usr/bin/env bash
# Genera un .env efímero desde SSM antes de levantar el stack.
set -euo pipefail
PREFIX="/pyspark-stack"
REGION="${AWS_REGION:-us-east-1}"
get() { aws ssm get-parameter --name "$PREFIX/$1" --with-decryption \
          --query Parameter.Value --output text --region "$REGION"; }

# EMR Serverless (NO son secretos): app id + ARN del rol de ejecución, para las Airflow Variables.
ACCT=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
EMR_APP_ID=$(aws emr-serverless list-applications --region "$REGION" \
  --query "applications[?name=='pyspark-stack-spark'].id | [0]" --output text)
EMR_JOB_ROLE_ARN="arn:aws:iam::${ACCT}:role/pyspark-stack-emr-serverless-job"
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

Uso en la EC2: `chmod +x scripts/load-secrets.sh && ./scripts/load-secrets.sh && docker compose -f
docker-compose.prod.yml up -d`.

> **Secrets Manager (opcional, para lo delicado con rotación).** Para el JWT secret o credenciales de
> terceros, **Systems Manager → ... → Secrets Manager → Store a new secret → Other type of secret** →
> nombre `pyspark-stack/airflow_jwt_secret`. Suma rotación automática (~$0.40/secreto/mes). En ese
> caso agregá al rol EC2 `secretsmanager:GetSecretValue` sobre
> `arn:aws:secretsmanager:us-east-1:<acct>:secret:pyspark-stack/*` y leelo en `load-secrets.sh` con
> `aws secretsmanager get-secret-value --secret-id pyspark-stack/airflow_jwt_secret ...`. Parameter
> Store SecureString (gratis) alcanza para el resto.

> Verificá (CLI): `aws ssm get-parameter --name /pyspark-stack/postgres_password --with-decryption
> --query Parameter.Value --output text | head -c 8` → imprime 8 caracteres, no un error.

---

## 8. CI/CD con GitHub Actions (OIDC, sin claves)

Dos workflows en `.github/workflows/`: `ci.yml` (valida en cada PR/push) y `deploy.yml` (despliega al
mergear a `main`). GitHub Actions asume un rol IAM vía **OIDC** — sin access keys en el repo. Los
archivos de workflow son idénticos a la guía 02 §11.2/§11.3 (reproducidos en §8.3); lo que cambia acá
es que el **OIDC provider y el rol se crean a mano en la consola**.

### 8.1 OIDC provider + rol (consola)

**Paso 1 — Identity provider.** Consola: **IAM → Identity providers → Add provider**.

- *Provider type* **OpenID Connect**.
- *Provider URL* `https://token.actions.githubusercontent.com` → **Get thumbprint**.
- *Audience* `sts.amazonaws.com` → **Add provider**.

**Paso 2 — Rol.** Consola: **IAM → Roles → Create role**.

- *Trusted entity type* **Web identity**.
- *Identity provider* → el que acabás de crear · *Audience* `sts.amazonaws.com`.
- *GitHub organization* = tu org/usuario · *GitHub repository* = `pyspark_stack` · *GitHub branch* =
  `main` (equivale a la condición `sub = repo:org/repo:ref:refs/heads/main`). → **Next**.
- (sin managed policies) → *Role name* `pyspark-stack-github-actions` → **Create role**.

**Paso 3 — Permisos del rol.** En el rol → **Add permissions → Create inline policy → JSON**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "S3ListArtifacts", "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::pyspark-stack-artifacts-<acct>" },
    { "Sid": "S3DeployObjects", "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject", "s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>/deploy/*",
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>/emr/*"
      ] },
    { "Sid": "SsmDeploy", "Effect": "Allow",
      "Action": ["ssm:SendCommand"],
      "Resource": [
        "arn:aws:ec2:us-east-1:<acct>:instance/i-xxxxxxxxxxxxxxxxx",
        "arn:aws:ssm:us-east-1::document/AWS-RunShellScript"
      ] },
    { "Sid": "SsmResult", "Effect": "Allow",
      "Action": ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations"], "Resource": "*" }
  ]
}
```

*Policy name* `deploy-policy` → **Create policy**. Copiá el **Role ARN** (arriba en la página del
rol).

> El CD solo mueve **código** (DAGs + entrypoints EMR); no necesita `terraform plan`, así que no lleva
> permisos de tfstate ni `ReadOnlyAccess` (a diferencia del rol de la guía 02, que los tenía como
> opcionales para correr `plan` en CI).

### 8.1b Un segundo rol OIDC, solo para dbt Slim CI

El rol de §8.1 está atado al environment `production` con *Required reviewers* — a propósito, es el
gate de aprobación del deploy. dbt Slim CI (§8.3) necesita correr **automático en cada PR**, sin que
nadie apruebe nada: si reusara ese rol, o le sacás el gate (y el deploy deja de estar protegido) o
cada PR queda esperando aprobación manual. Por eso es un rol aparte, sin ese gate, pero con permisos
acotados **solo** a la database `_ci` (guía 02 §16.2) — nunca puede tocar la de producción.

**Paso 1 — Rol.** Consola: **IAM → Roles → Create role** → *Web identity* → mismo provider de §8.1 →
audience `sts.amazonaws.com`. **No completes** *GitHub branch* en el asistente (dejalo vacío): a
diferencia del rol de deploy, este debe poder asumirse desde **cualquier rama/PR**, no solo `main`.
Nombre `pyspark-stack-dbt-ci` → **Create role**.

**Paso 2 — Permisos.** Inline policy JSON — least-privilege a la database `_ci`, `dbt-ci/` y
`dbt-state/manifest.json` del bucket de artifacts, y lectura de `raw/`/`curated/` del datalake:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "ReadSources", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::pyspark-stack-datalake-<acct>", "arn:aws:s3:::pyspark-stack-datalake-<acct>/*"] },
    { "Sid": "WriteCiOutputs", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>/dbt-ci/*",
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>/athena-results/*",
        "arn:aws:s3:::pyspark-stack-artifacts-<acct>/dbt-state/manifest.json"
      ] },
    { "Sid": "AthenaResultsBucket", "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
      "Resource": "arn:aws:s3:::pyspark-stack-artifacts-<acct>" },
    { "Sid": "AthenaQuery", "Effect": "Allow",
      "Action": ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"],
      "Resource": "arn:aws:athena:us-east-1:<acct>:workgroup/pyspark-stack-analytics" },
    { "Sid": "GlueCiOnly", "Effect": "Allow",
      "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions"],
      "Resource": [
        "arn:aws:glue:us-east-1:<acct>:catalog",
        "arn:aws:glue:us-east-1:<acct>:database/pyspark_stack_analytics_ci",
        "arn:aws:glue:us-east-1:<acct>:table/pyspark_stack_analytics_ci/*"
      ] }
  ]
}
```

*Policy name* `dbt-ci-policy` → **Create policy**. Copiá el **Role ARN**.

**Paso 3 — Database y lifecycle de CI.** **Glue → Data Catalog → Databases → Add database** →
`pyspark_stack_analytics_ci` → **Create**. **S3 → bucket artifacts → Management → Create lifecycle
rule** → *Name* `dbt-ci-expire` · *Prefix* `dbt-ci/` · *Expire current versions* a los **3 días**
→ **Create rule** (las tablas que un PR materializa al testear no se limpian solas si no).

### 8.2 Variables y entorno en GitHub

- **Settings → Secrets and variables → Actions → Variables → New repository variable** (×4, son
  **variables**, no secrets):
  - `AWS_DEPLOY_ROLE_ARN` = el Role ARN de §8.1.
  - `AWS_DBT_CI_ROLE_ARN` = el Role ARN de §8.1b.
  - `AWS_REGION` = `us-east-1`.
  - `ARTIFACTS_BUCKET` = `pyspark-stack-artifacts-<acct>`.
- **Settings → Environments → New environment** → `production` → agregá **Required reviewers** (el
  gate de aprobación manual que exige `environment: production` en `deploy.yml`). El job
  `dbt-slim-ci` de §8.3 **no** declara `environment` — corre sin aprobación en cada PR (§8.1b).

### 8.3 Los workflows

`.github/workflows/ci.yml` y `.github/workflows/deploy.yml` son **idénticos** a los de la guía 02
§11.2 y §11.3 (junto con `tests/conftest.py`, `tests/test_dag_integrity.py`, `ruff.toml`,
`.pre-commit-config.yaml` y `Makefile`). Reproducidos acá para que la guía sea autocontenida:

**`.github/workflows/ci.yml`** — 4 jobs: lint (ruff), validación de DAGs (pytest sobre el
`DagBag`), security (gitleaks; el step de checkov de la guía 02 no aplica acá — no hay Terraform
que escanear), y **dbt Slim CI** — el único con credenciales AWS (rol `dbt_ci` de §8.1b, no el de
deploy):

```yaml
name: CI
on:
  pull_request:
  push:
    branches-ignore: [main]
permissions:
  contents: read
concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true
jobs:
  lint:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12", cache: pip }
      - run: pip install ruff==0.14.3
      - run: ruff check .
      - run: ruff format --check .
  dag-validate:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12", cache: pip }
      - name: Instalar Airflow 3.2.2 + providers (con constraints)
        env:
          CONSTRAINTS: "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt"
        run: |
          python -m pip install --upgrade pip
          pip install "apache-airflow==3.2.2" --constraint "${CONSTRAINTS}"
          pip install "apache-airflow-providers-amazon==9.29.0" \
                      "apache-airflow-providers-apache-spark==6.0.2" pytest
      - run: pytest tests/ -q
  security:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - uses: gitleaks/gitleaks-action@v2
        env: { GITHUB_TOKEN: "${{ secrets.GITHUB_TOKEN }}" }
  dbt-slim-ci:
    name: dbt Slim CI (state:modified+)
    runs-on: ubuntu-latest
    timeout-minutes: 15
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
        with: { sparse-checkout: dbt }
      - uses: actions/setup-python@v5
        with: { python-version: "3.12", cache: pip }
      - name: Autenticar en AWS (OIDC, rol dbt_ci — §8.1b)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_DBT_CI_ROLE_ARN }}
          aws-region: ${{ vars.AWS_REGION }}
      - run: pip install "dbt-core==1.9.*" "dbt-athena-community==1.9.*"
      - name: Bajar el manifest de producción (baseline de state:modified+)
        run: |
          mkdir -p state
          aws s3 cp "s3://${{ vars.ARTIFACTS_BUCKET }}/dbt-state/manifest.json" state/manifest.json \
            || echo "sin baseline todavía — corre todos los modelos esta vez"
      - name: dbt build --select state:modified+
        working-directory: dbt
        env: { ARTIFACTS_BUCKET: "${{ vars.ARTIFACTS_BUCKET }}" }
        run: |
          if [ -s ../state/manifest.json ]; then
            dbt build --target ci --profiles-dir . --select state:modified+ --state ../state
          else
            dbt build --target ci --profiles-dir .
          fi
```

**`.github/workflows/deploy.yml`** — OIDC → `aws s3 sync` de `dags/` a `deploy/dags/` y de
`spark-apps/emr/` a `emr/`, luego SSM sync-down + smoke en la EC2:

```yaml
name: Deploy
on:
  push:
    branches: [main]
    paths: ["dags/**", "spark-apps/emr/**"]
permissions:
  id-token: write   # requerido para OIDC
  contents: read
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production   # gate de aprobación manual
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
            echo "EC2 apagada: el deploy quedó en S3; se aplica al encenderla."; exit 0
          fi
          B=${{ vars.ARTIFACTS_BUCKET }}
          CMD=$(aws ssm send-command --instance-ids "$I" \
            --document-name AWS-RunShellScript --comment "deploy sync-down + smoke" \
            --parameters commands="[\
              \"cd /home/ec2-user/pyspark_stack\",\
              \"aws s3 sync s3://$B/deploy/dags/ dags/ --delete\",\
              \"docker compose exec -T airflow-dag-processor airflow dags reserialize\",\
              \"docker compose exec -T airflow-scheduler airflow dags list-import-errors\"\
            ]" --query 'Command.CommandId' --output text)
          aws ssm wait command-executed --command-id "$CMD" --instance-id "$I" || true
          STATUS=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$I" --query 'Status' --output text)
          OUT=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$I" --query 'StandardOutputContent' --output text)
          echo "$OUT"
          if [ "$STATUS" != "Success" ] || echo "$OUT" | grep -q '\.py'; then
            echo "Deploy/smoke falló (Status=$STATUS o hay import errors)"; exit 1
          fi
```

Los tests (`tests/conftest.py`, `tests/test_dag_integrity.py`) y el tooling (`ruff.toml`,
`.pre-commit-config.yaml`, `Makefile`) son los de la guía 02 §11.2 — copialos tal cual al repo.

**Puesta en marcha:** hacé el primer `git push` a `main` que toque `dags/` o `spark-apps/emr/` (con la
EC2 encendida el sync-down baja los DAGs y corre el smoke; apagada, queda en S3 y se aplica al próximo
encendido). Los PRs disparan **CI**.

---

## 9. Monitoreo

Observabilidad completa corriendo **dentro de la EC2** junto al `docker-compose`: métricas + alertas
+ logs centralizados (Prometheus + Grafana + Alertmanager + Loki). **No hay recursos de consola AWS
que crear acá**: es todo archivos de configuración en el repo + contenedores en `docker-compose.prod.yml`.
Los archivos son idénticos a la guía 02 §12 y §14.1; reproducidos en §11 (compose) y abajo (configs).

Qué se monitorea:

| Señal | Exporter / fuente | Puerto interno |
|---|---|---|
| Host (CPU, RAM, disco, red) | `node-exporter` | 9100 |
| Contenedores (uso por servicio) | `cAdvisor` | 8080 |
| Airflow (DAGs, tasks, duraciones) | Airflow StatsD → `statsd-exporter` | 9102 |
| Spark (jobs) | **EMR Serverless** → CloudWatch + logs a S3 (`emr/logs/`) | — (managed) |
| Logs de todos los contenedores | `Promtail` → `Loki` | 3100 |
| Alertas | `Alertmanager` → email | 9093 |
| Dashboards | `Grafana` | 3000 |

Estructura `monitoring/`:

```
monitoring/
├── prometheus/{prometheus.yml, alerts.yml}
├── alertmanager/alertmanager.yml
├── statsd/statsd_mapping.yml
├── loki/loki-config.yml
├── promtail/promtail-config.yml
└── grafana/
    ├── provisioning/{datasources/datasources.yml, dashboards/dashboards.yml}
    └── dashboards/overview.json
```

Los contenidos de estos archivos son exactamente los de la guía 02 §12.4–§12.7. Los dos más
importantes de tener a mano:

**`monitoring/prometheus/alerts.yml`** (extracto — las alertas de negocio y EMR):

```yaml
groups:
  - name: pyspark-stack
    rules:
      - alert: TargetDown
        expr: up == 0
        for: 2m
        labels: { severity: critical }
        annotations: { summary: "Target {{ $labels.job }} caído" }
      - alert: HostDiskAlmostFull
        expr: (node_filesystem_avail_bytes{mountpoint="/data"} / node_filesystem_size_bytes{mountpoint="/data"}) * 100 < 10
        for: 5m
        labels: { severity: critical }
        annotations: { summary: "Disco /data casi lleno" }
      - alert: DailyEtlMissing   # dead-man switch: el ETL diario dejó de correr en silencio
        expr: >-
          increase(airflow_dagrun_duration_success_count{dag_id="customer_etl_emr"}[26h]) == 0
          or absent(airflow_dagrun_duration_success_count{dag_id="customer_etl_emr"})
        for: 10m
        labels: { severity: critical }
        annotations: { summary: "El ETL diario no completó con éxito (dead-man switch)" }
      - alert: EmrServerlessJobFailed
        expr: increase(airflow_ti_failures[15m]) > 0
        for: 1m
        labels: { severity: critical }
        annotations: { summary: "Job Spark de EMR Serverless falló (task de Airflow en error)" }
```

**`monitoring/alertmanager/alertmanager.yml`** — el SMTP password va **literal** (Alertmanager no
expande env vars), por eso este archivo **no** va a git (`.gitignore`). Crealo en la EC2 con el app
password de Gmail (o renderizalo con `envsubst` tomando `/pyspark-stack/smtp_password` de SSM):

```yaml
global:
  resolve_timeout: 5m
  smtp_smarthost: "smtp.gmail.com:587"
  smtp_from: "tu-email@gmail.com"
  smtp_auth_username: "tu-email@gmail.com"
  smtp_auth_password: "APP_PASSWORD_DE_GMAIL"
  smtp_require_tls: true
route:
  receiver: email
  group_by: ["alertname"]
  routes:
    - matchers: ['severity="critical"']
      receiver: email
      repeat_interval: 1h
receivers:
  - name: email
    email_configs:
      - to: "tu-email@gmail.com"
        send_resolved: true
```

El resto (`prometheus.yml`, `statsd_mapping.yml`, `loki-config.yml`, `promtail-config.yml`, los
provisioning de Grafana y `overview.json`) copialos tal cual de la guía 02 §12.4–§12.7.

**Acceso (por túnel SSH):**

```bash
ssh -i ~/.ssh/pyspark_stack -L 3000:localhost:3000 -L 9090:localhost:9090 -L 9093:localhost:9093 -L 3100:localhost:3100 ec2-user@$IP
# Grafana localhost:3000 · Prometheus localhost:9090 · Alertmanager localhost:9093 · Loki localhost:3100
```

**Observabilidad de los jobs Spark (EMR Serverless)** — es *managed*, así que su telemetría vive en
AWS, no en Prometheus/Loki:
- **Métricas** → CloudWatch, namespace `AWS/EMRServerless`.
- **Logs del job** → S3 `s3://<artifacts>/emr/logs/` y CloudWatch Logs
  (`/aws/emr-serverless/pyspark-stack`).
- **Spark UI** → la consola de EMR Serverless reconstruye la UI de cada corrida terminada.

> Opcional: una **alarma CloudWatch** sobre la métrica de *job runs* en estado `FAILED` de la app EMR
> (namespace `AWS/EMRServerless`), notificando por **SNS** → email. Consola: **CloudWatch → Alarms →
> Create alarm**. Cubre el caso incluso si el fallo no llegara a reflejarse como task fallida en
> Airflow.

---

## 10. Athena — capa de consumo SQL/BI (opcional)

Athena consulta el data lake **con SQL puro, sin prender Spark ni un cluster**: paga solo por dato
escaneado (~$5/TB, mínimo 10 MB/query) y escala a cero. A esta escala el gasto es **~$0/mes**. Sirve
para SQL ad-hoc sobre `analytics/`/`curated/`, BI (QuickSight/Grafana/Metabase), y asserts de calidad
dentro de un DAG.

> La base de datos del Glue Data Catalog (`pyspark_stack_analytics`) ya se creó en §5.4 — la usa
> Iceberg desde el job Spark exista o no esta sección de Athena. Acá no hay que repetirla.

**Paso 1 — Workgroup de Athena.** Consola: **Athena → Administration → Workgroups → Create
workgroup**.

- *Name* `pyspark-stack-analytics`.
- *Query result configuration → Location of query result*:
  `s3://pyspark-stack-artifacts-<acct>/athena-results/`.
- *Encrypt query results*: **SSE-S3**.
- *Override client-side settings*: **On** (equivale a `enforce_workgroup_configuration=true`).
- *Athena engine version*: **Athena engine version 3** — sin esto, `MERGE`/`UPDATE`/`DELETE` sobre
  tablas Iceberg (Paso 3) fallan en workgroups viejos migrados de v2.
- *Publish query metrics to CloudWatch*: **On**.
- **Data usage controls → Track query limit per query** → **5,000 MB** (auditoría §2.2: sin esto,
  un `SELECT *` sin filtro de partición sobre una tabla que creció puede ser $5-20 en una sola
  query, a $5/TB — 5 GB es generoso a este volumen total de 2-5 GB/**día**) → **Create workgroup**.

**Paso 2 — Expiración de resultados** (descartables). Consola: **S3 → bucket artifacts → Management →
Create lifecycle rule** → *Name* `athena-results-expire` · *Prefix* `athena-results/` · *Expire
current versions* a los **7 días** → **Create rule**.

**Paso 3 — Tabla Iceberg (ACID, time travel, `MERGE`, sin crawler).** Si el job Spark de §12 **ya
escribió** la tabla (caso normal), no hay que declarar nada acá: aparece sola en
`pyspark_stack_analytics.ventas` apenas Spark hace el primer `INSERT`/`MERGE` — Iceberg la registra
en el mismo Glue Data Catalog. Si preferís crearla primero desde Athena (prototipar sin correr un
job EMR todavía), consola: **Athena → Query editor** (workgroup `pyspark-stack-analytics`) →
ejecutá una vez:

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

`PARTITIONED BY (dt)` acá es partición **oculta** de Iceberg, no partition projection: Iceberg
resuelve `WHERE dt = '...'` contra sus manifests solo, sin rangos de fecha que mantener a mano. Con
la tabla creada, ya podés hacer desde el mismo Query editor:

```sql
-- Time travel: la tabla como estaba hace 3 versiones
SELECT * FROM pyspark_stack_analytics.ventas FOR VERSION AS OF 3 WHERE pais = 'PE';

-- Upsert incremental — reemplaza el overwrite completo del Parquet suelto anterior
MERGE INTO pyspark_stack_analytics.ventas t
USING (VALUES ('PE', 120.50, '2026-07-16')) AS s(pais, monto, dt)
ON t.pais = s.pais AND t.dt = s.dt
WHEN MATCHED THEN UPDATE SET monto = s.monto
WHEN NOT MATCHED THEN INSERT (pais, monto, dt) VALUES (s.pais, s.monto, s.dt);
```

**Paso 3b — Mantenimiento: compactación y expiración de snapshots (auditoría §2.1).** Sin esto,
cada `MERGE` deja archivos chicos y un snapshot nuevo; después de meses de corridas 3x/semana el
*planning time* de las queries se degrada solo, sin que nadie lo note hasta que ya molesta. Desde
el mismo Query editor, o como task semanal de un DAG (`AthenaOperator`, guía 02 §16.1b):

```sql
-- Compacta archivos chicos en archivos más grandes
OPTIMIZE pyspark_stack_analytics.ventas REWRITE DATA USING BIN_PACK;

-- Libera snapshots/archivos ya no referenciados, más viejos que la retención por defecto
VACUUM pyspark_stack_analytics.ventas;
```

> Verificá la sintaxis exacta contra tu versión de Athena engine — el soporte de mantenimiento de
> Iceberg se fue agregando de forma incremental. El detalle completo (DAG semanal, por qué viernes
> y no domingo por el auto start/stop) está en la [guía 02 §16.1b](02-produccion-aws.md#161b-mantenimiento-compactación-y-expiración-de-snapshots-auditoría-21).

**Paso 4 — Permitir que un DAG consulte (rol de la EC2).** Consola: **IAM → Roles →
`pyspark-stack-ec2-role` → Add permissions → Create inline policy → JSON**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "AthenaQuery", "Effect": "Allow",
      "Action": ["athena:StartQueryExecution", "athena:GetQueryExecution",
                 "athena:GetQueryResults", "athena:StopQueryExecution"],
      "Resource": "arn:aws:athena:us-east-1:<acct>:workgroup/pyspark-stack-analytics" },
    { "Sid": "GlueCatalogRead", "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"], "Resource": "*" },
    { "Sid": "AthenaDataRead", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::pyspark-stack-datalake-<acct>",
        "arn:aws:s3:::pyspark-stack-datalake-<acct>/*"
      ] },
    { "Sid": "AthenaResults", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::pyspark-stack-artifacts-<acct>/athena-results/*" }
  ]
}
```

*Policy name* `ec2-athena` → **Create policy**. El uso en un DAG (`AthenaOperator` como assert de
calidad post-ETL) está en §14.3.

---

## 11. Archivos de repo

Todo lo de esta sección son **archivos que viven en el repo** (no recursos de consola). Son idénticos
a la guía 02; van acá para que la guía sea autocontenida. Se suben a la EC2 con el rsync de §4.5.

### 11.1 `docker-compose.prod.yml` (standalone, completo)

**No es un override que se fusiona con nada** — es un archivo único y autosuficiente, sin Spark, sin
HDFS y sin Jupyter (acá no se usa: el ETL corre por Airflow + EMR Serverless + papermill, headless;
si necesitás explorar datos a mano, usá el Jupyter del stack local, `docs/01`). Incorpora
persistencia en `/data`, `restart`, límites, logging, métricas StatsD y las Airflow Variables de EMR
Serverless. Las variables `${...}` las provee el `.env` que genera `load-secrets.sh` (§7). Es
idéntico al de la guía 02 §14.1 — se levanta con `docker compose -f docker-compose.prod.yml up -d --build`:

```yaml
# docker-compose.prod.yml — stack de PRODUCCIÓN, standalone (un solo archivo, sin merge, sin
# Spark/HDFS/Jupyter: Spark corre en EMR Serverless, §5.4; Jupyter no se usa en prod, §4.5).
#   ./scripts/load-secrets.sh   # genera .env desde SSM (§7)
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
    AIRFLOW__METRICS__STATSD_ON: "True"
    AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
    AIRFLOW__METRICS__STATSD_PORT: "9125"
    AIRFLOW__METRICS__STATSD_PREFIX: airflow
    AIRFLOW_VAR_EMR_APP_ID: "${EMR_APP_ID}"
    AIRFLOW_VAR_EMR_JOB_ROLE_ARN: "${EMR_JOB_ROLE_ARN}"
    AIRFLOW_VAR_DATALAKE: "${DATALAKE_BUCKET}"
    AIRFLOW_VAR_ARTIFACTS: "${ARTIFACTS_BUCKET}"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "False"
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: "30"
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
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks   # papermill lee los .ipynb
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

  # ==================== MONITOREO ====================
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
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks: [hadoopnet]

networks:
  hadoopnet:
```

### 11.2 `scripts/deploy.sh` (deploy rápido dev, sin CI)

```bash
#!/usr/bin/env bash
# scripts/deploy.sh — deploy rápido a la EC2 (dev). Uso: ./scripts/deploy.sh <IP>
set -euo pipefail
IP="${1:?pasá la Elastic IP como argumento}"
KEY=~/.ssh/pyspark_stack
rsync -avz --delete --exclude '__pycache__' -e "ssh -i $KEY" \
  dags spark-apps notebooks \
  ec2-user@"$IP":/home/ec2-user/pyspark_stack/
echo "deploy hecho — Airflow detecta los DAGs en ~30s (refresh de docker-compose.prod.yml)"
```

> A diferencia de la guía 02 (que saca la IP de `terraform output`), acá la pasás como argumento (no
> hay Terraform). Config nueva (`monitoring/`, compose, `requirements.txt`) **no** la sube este script
> — usá el rsync completo de §4.5.

### 11.3 Los entrypoints PySpark, DAGs, monitoreo, tests

- **Entrypoints** `spark-apps/emr/customer_etl.py` y `spark-apps/emr/wordcount.py`: código completo en
  la guía 02 §6.4 (punto E). Son self-contained (sin `.master()`, leen/escriben `s3a://`).
- **DAGs de producción** `dags/customer_etl_emr_dag.py` y `dags/spark_trigger_emr_dag.py`:
  reproducidos en §12.
- **Configs de monitoreo** (`monitoring/prometheus/prometheus.yml`, `statsd_mapping.yml`,
  `loki-config.yml`, `promtail-config.yml`, provisioning de Grafana, `overview.json`): guía 02
  §12.4–§12.7.
- **Tests y tooling** (`tests/conftest.py`, `tests/test_dag_integrity.py`, `ruff.toml`,
  `.pre-commit-config.yaml`, `Makefile`): guía 02 §11.2.
- **`requirements.txt`** — agregá (guía 02 §9.1/§19.2):
  ```text
  apache-airflow-providers-amazon==9.29.0   # operadores EMR Serverless — imprescindible
  apache-airflow-providers-papermill==3.9.1 # notebooks
  pandas
  s3fs
  pyarrow
  dbt-core==1.9.*
  dbt-athena-community==1.9.*
  dbt-spark[session]==1.9.*   # solo si vas a usar el target `spark` de §11.4
  ```
- **`.gitignore`** — que cubra `.env`, `monitoring/alertmanager/alertmanager.yml` (tiene el SMTP
  password), salidas de papermill.

### 11.4 dbt Core (transformaciones SQL sobre las tablas Iceberg)

Reemplaza SQL que viviría hardcodeado en un DAG por modelos versionados en Git, con tests y docs
autogeneradas. **Airflow sigue siendo el único orquestador**: dbt es una task más (`BashOperator`),
igual que ya disparás papermill o EMR Serverless. Detalle completo y la explicación de por qué el
target `spark` no es un dbt-spark típico (EMR Serverless no tiene un endpoint persistente al que
conectarse) está en la [guía 02 §19](02-produccion-aws.md#19-transformaciones-sql-con-dbt) — acá,
los archivos tal cual, idénticos:

```text
dbt/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── athena/ventas_por_pais.sql   # la mayoría de los modelos: SQL directo sobre Athena/Iceberg
    └── spark/ventas_reproceso.sql   # solo si un modelo necesita reprocesar a escala Spark
```

```yaml
# dbt/profiles.yml — credenciales del rol IAM de la EC2, sin keys
pyspark_stack:
  target: athena
  outputs:
    athena:
      type: athena
      s3_staging_dir: "s3://{{ env_var('ARTIFACTS_BUCKET') }}/athena-results/"
      region_name: us-east-1
      database: pyspark_stack_analytics     # el Glue database de §5.4
      work_group: pyspark-stack-analytics   # el workgroup engine v3 de §10
      num_retries: 3
    spark:
      type: spark
      method: session   # sin endpoint: corre embebido en el propio job EMR Serverless
      schema: pyspark_stack_analytics
      host: localhost
```

Task de Airflow (fragmento — se dispara tras el job EMR que puebla `curated/`):

```python
from airflow.providers.standard.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id="dbt_run_ventas",
    bash_command="cd /opt/dbt && dbt run --target athena --select ventas_por_pais --vars '{\"ds\": \"{{ ds }}\"}'",
    env={"ARTIFACTS_BUCKET": "{{ var.value.artifacts }}"},
)
run_emr >> dbt_run
```

Montá `./dbt:/opt/dbt` en `airflow-scheduler` — sumalo al `docker-compose.prod.yml` de §11.1, junto
a `./notebooks`:

```yaml
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks
      - ./dbt:/opt/dbt
```

### 11.5 Great Expectations (calidad de datos)

Gate más estricto que el `SELECT count(*)` de §10: valida schema/nulls/rangos de `curated/` **antes**
de promoverlo a `analytics/` o de que dbt corra sobre esa tabla. Corre vía SQL sobre Athena (el mismo
`pyathena`/workgroup de §10 y §11.4) — sin Spark, sin cargar el dataset a memoria. Detalle completo
en la [guía 02 §20](02-produccion-aws.md#20-calidad-de-datos-con-great-expectations); acá, idéntico:

```text
# requirements.txt
great-expectations==1.3.*
pyathena==3.*
```

`great_expectations/expectations/curated_ventas.json` (fragmento; JSON no admite comentarios, la
ruta va en el texto):

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

Task de Airflow (corre entre el job EMR y dbt/la promoción a `analytics/`):

```python
from airflow.providers.standard.operators.bash import BashOperator

validar_calidad = BashOperator(
    task_id="validar_calidad_ventas",
    bash_command=(
        "cd /opt/great_expectations && "
        "great_expectations checkpoint run curated_ventas_checkpoint"
    ),
)
run_emr >> validar_calidad >> dbt_run   # solo transforma/promueve si la validación pasa
```

Sale con código de error si alguna expectation falla — la task queda roja en Airflow, mismo mecanismo
que cualquier otra falla de DAG (no es un canal de alertas nuevo). Montá
`./great_expectations:/opt/great_expectations` en `airflow-scheduler`, junto a `./dbt`:

```yaml
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags
      - ./notebooks:/opt/notebooks
      - ./dbt:/opt/dbt
      - ./great_expectations:/opt/great_expectations
```

---

## 12. Los DAGs de producción (EMR Serverless)

Idénticos a la guía 02 §10.2. Ambos protegen el import del provider `amazon` con `try/except` (si no
está, dev local no rompe) y tienen `schedule=None` (los dispara la Lambda `trigger-airflow`, §6).

**`dags/customer_etl_emr_dag.py`** — orquesta `customer_etl` en EMR Serverless y apaga la EC2 al
terminar:

```python
"""customer_etl en producción: Airflow orquesta, EMR Serverless computa."""
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
        schedule=None,
        catchup=False,
        max_active_runs=1,   # auditoría §3.1: 2 disparos el mismo día no duplican el trabajo
        tags=["emr", "prod", "etl"],
    ) as dag:
        run = EmrServerlessStartJobOperator(
            task_id="customer_etl",
            application_id="{{ var.value.emr_app_id }}",
            execution_role_arn="{{ var.value.emr_job_role_arn }}",
            deferrable=True,   # espera en el triggerer, sin ocupar worker slot
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

**`dags/spark_trigger_emr_dag.py`** — variante demo (`wordcount`) para validar el camino Airflow →
EMR sin datos en `raw/`:

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
            deferrable=True,
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

**Apagado job-aware** (§13): la task `trigger_stop` (`trigger_rule="all_done"`, se apaga aunque el ETL
falle) invoca la Lambda `startstop` asíncrona; la Lambda, antes de apagar, consulta vía SSM si hay
DAG runs `running` y no apaga si los hay → con varios DAGs, solo el último en terminar deja apagar la
caja. El cron de stop de las 22:00 (§4.4) queda como red de seguridad por si un DAG cuelga.

> Las Airflow Variables `emr_app_id`, `emr_job_role_arn`, `datalake` y `artifacts` salen del `.env`
> (§7) como env `AIRFLOW_VAR_*` (§11.1). El patrón de tarea (¿PySpark o Python puro?) y el pipeline
> unificado están en la guía 02 §9.0.

---

## 13. Operación, seguridad y ahorro

**Operación (cheat-sheet):**

```bash
# Prender/apagar la EC2 a mano (además del cron):
aws ec2 stop-instances --instance-ids <i-xxxx>
aws lambda invoke --function-name pyspark-stack-startstop \
  --cli-binary-format raw-in-base64-out --payload '{"action":"start"}' /dev/stdout

# Disparar un DAG a mano (mismo camino que EventBridge/S3):
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --cli-binary-format raw-in-base64-out --payload '{"dag":"customer_etl_emr"}' /dev/stdout
```

> Sin CLI: prender/apagar desde **EC2 → Instances → Instance state**; disparar un DAG desde **Lambda →
> `pyspark-stack-trigger-airflow` → Test**.

**Seguridad (checklist):**
- [ ] Buckets con Block Public Access, cifrado y política solo-TLS (§5.1).
- [ ] SG de EC2: 22 (y 443 si exponés la web) **solo** desde tu IP; el resto por túnel; los triggers
      van por SSM, no por la web.
- [ ] IAM least-privilege: `startstop` solo actúa sobre instancias con el tag; `trigger-airflow` solo
      `ssm:SendCommand` sobre esta instancia; EMR con su propio rol scopeado a los buckets; la EC2
      solo `StartJobRun` + `PassRole` (con `iam:PassedToService`).
- [ ] Spark (EMR) y Airflow usan `s3a://`/`s3://` con rol IAM, sin access keys en disco.
- [ ] Tráfico EC2↔S3 por el S3 VPC Gateway Endpoint (§5.5). (EMR Serverless no aplica: corre
      fuera de tu VPC — ver §5.5.)
- [ ] Secretos en SSM Parameter Store / Secrets Manager (§7), no en texto plano.
- [ ] `.env`, `monitoring/alertmanager/alertmanager.yml` en `.gitignore`.

**Palancas de ahorro (orden de impacto):**
1. **EMR Serverless (escala a cero)** → Spark paga solo mientras corre (~$9/mes). Mayor palanca.
2. **Auto start/stop de la EC2** → de ~$60 a ~$12/mes.
3. **S3 lifecycle a IA/Glacier** (§5.1).
4. **Snapshots DLM con retención 7 días** (§5.3).

### Resumen: qué corre y dónde

| Subsistema | Dónde corre | Storage durable |
|---|---|---|
| Spark (jobs ETL) | **EMR Serverless** (escala a cero) | `s3a://` con su rol de ejecución |
| Airflow (5 svcs) + Postgres | contenedores en EC2 `t3.large` | Postgres en `/data` (EBS) + snapshots |
| Monitoreo (Prom/Grafana/AM/Loki) | contenedores en EC2 | Prometheus/Loki en `/data` + snapshots |
| Disparo de DAGs | Lambda trigger-airflow (SSM) + EventBridge | — |
| Encendido | EC2 con auto start/stop | — |

---

## 14. Airflow, 3 sabores

Airflow es **solo el orquestador**; cada task elige su motor. Los tres que usa el stack (detalle en la
guía 02 §17):

### 14.1 Python puro (en la EC2)

Para dato chico (<~1 GB), APIs, mover/validar archivos. Corre en el proceso del scheduler
(LocalExecutor), con el rol IAM de la EC2 (§5.2). Toda la telemetría vive en la EC2 (Airflow +
Prometheus + Loki):

```python
# dags/small_etl_dag.py — el caso "no necesito Spark"
from datetime import datetime
import pandas as pd
from airflow.sdk import DAG, task, Variable

with DAG("small_etl", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False,
         tags=["python"]) as dag:
    @task
    def transform(ds=None):
        base = f"s3://{Variable.get('datalake')}"
        df = pd.read_csv(f"{base}/raw/ventas.csv")   # s3fs + rol IAM, sin keys
        out = df[df["monto"] > 0].groupby("pais")["monto"].sum().reset_index()
        out.to_parquet(f"{base}/curated/ventas_por_pais/{ds}.parquet")
    transform()
```

### 14.2 PySpark en EMR Serverless

Para dato mediano/grande, joins/`groupBy`. La EC2 **no corre Spark**: dispara y espera. La telemetría
del cómputo vive en **AWS** (CloudWatch + consola EMR + S3), no en la EC2:

```python
# dags/emr_etl_dag.py — el caso "sí necesito Spark"
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

with DAG("emr_etl", schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False,
         tags=["spark", "emr"]) as dag:
    run = EmrServerlessStartJobOperator(
        task_id="ventas_spark",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_job_role_arn }}",
        job_driver={"sparkSubmit": {
            "entryPoint": "s3://{{ var.value.artifacts }}/emr/ventas.py",
            "entryPointArguments": ["{{ var.value.datalake }}", "{{ ds }}"],
            "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g",
        }},
        configuration_overrides={"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": "s3://{{ var.value.artifacts }}/emr/logs/"}}},
        wait_for_completion=True,
    )
```

### 14.3 SQL con Athena

Consulta SQL / BI / assert de calidad barato tras el job EMR. Requiere la infra de §10. El motor es
managed → su telemetría vive en AWS (CloudWatch + consola Athena):

```python
# encadenado tras el job EMR
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

assert_calidad = AthenaOperator(
    task_id="assert_calidad",
    query="SELECT count(*) AS filas FROM ventas WHERE dt = '{{ ds }}'",
    database="pyspark_stack_analytics",
    output_location="s3://{{ var.value.artifacts }}/athena-results/",
    workgroup="pyspark-stack-analytics",
)
run >> assert_calidad
```

**Dónde miro cada cosa:**

| Sabor | Orquestación | Métricas del cómputo | Logs | Costo se ve en |
|---|---|---|---|---|
| Python puro | Web Airflow | **Prometheus/Grafana** (EC2) | **Loki** (EC2) | — (parte de la EC2) |
| EMR Serverless | Web Airflow | **CloudWatch** `AWS/EMRServerless` + consola EMR | S3 `emr/logs/` + CloudWatch Logs | CloudWatch |
| Athena | Web Airflow | **CloudWatch** (workgroup) | consola Athena (*Query history*) | `DataScannedInBytes` |

---

## 15. Runbook final

### 15.1 De cero a producción — la secuencia

Los pasos de consola se hacen en el orden de §1. Una vez que **toda la infra existe**, el arranque del
stack en la EC2:

```bash
IP=<tu-elastic-ip>

# 1. Subir TODO a la EC2 (incluye scripts/, monitoring/ y los compose)
rsync -avz --exclude '.git' --exclude '.env' --exclude '__pycache__' \
  -e "ssh -i ~/.ssh/pyspark_stack" ./ ec2-user@$IP:/home/ec2-user/pyspark_stack/

# 2. Publicar los entrypoints PySpark a S3 (EMR los toma de ahí)
ACCT=$(aws sts get-caller-identity --query Account --output text)
aws s3 sync spark-apps/emr/ "s3://pyspark-stack-artifacts-$ACCT/emr/" --exclude '__pycache__/*'

# 3. En la EC2 (ssh ... && cd pyspark_stack):
#    a) crear monitoring/alertmanager/alertmanager.yml con el app password real (no está en git)
#    b) generar el .env desde SSM y verificarlo:
./scripts/load-secrets.sh
wc -l .env           # 11 variables (las que emite load-secrets.sh)
# Si exponés la web por HTTPS (§4.6), AIRFLOW_DOMAIN se agrega A MANO (no lo genera el script):
#   echo "AIRFLOW_DOMAIN=airflow.midominio.com" >> .env     → wc -l .env pasa a 12
ls -l .env           # -rw------- (chmod 600)

# 4. Validar el merge de los compose ANTES de levantar:
docker compose -f docker-compose.prod.yml config --quiet   # sin salida = OK

# 5. Levantar el stack completo:
docker compose -f docker-compose.prod.yml up -d --build

# 6. (OPCIONAL) Exponer la web de Airflow por HTTPS a tu IP: seguí §4.6.
```

### 15.2 Smoke tests (de abajo hacia arriba)

Validá capa por capa y pará en la primera que falle:

```bash
ID=<i-xxxx>; IP=<tu-elastic-ip>; ACCT=$(aws sts get-caller-identity --query Account --output text)

# ── 1. INFRA AWS ──
aws ec2 describe-instances --instance-ids "$ID" \
  --query 'Reservations[].Instances[].{estado:State.Name,imdsv2:MetadataOptions.HttpTokens}'  # running + required
aws ssm describe-instance-information \
  --query "InstanceInformationList[?InstanceId=='$ID'].PingStatus"   # "Online" (si no, el trigger NO anda)
aws s3 ls | grep pyspark-stack                            # datalake + artifacts
aws scheduler list-schedules --query 'Schedules[].Name'   # start / stop / daily-etl
aws dlm get-lifecycle-policies --query 'Policies[].State' # ENABLED
aws emr-serverless list-applications --query 'applications[?name==`pyspark-stack-spark`].[id,state]'

# ── 2. RED / HOST ──
nc -zv "$IP" 22 && echo "SSH ok"
curl --max-time 5 "http://$IP:8082" && echo "MAL: Airflow HTTP expuesto" || echo "OK: 8082 cerrado"
ssh -i ~/.ssh/pyspark_stack ec2-user@"$IP" 'cd pyspark_stack && docker compose ps'  # todos Up/healthy

# ── 3. STACK FUNCIONAL ──
S="ssh -i ~/.ssh/pyspark_stack ec2-user@$IP"
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags list-import-errors'  # vacío = OK
$S 'aws s3 cp /etc/hostname "s3://pyspark-stack-datalake-'"$ACCT"'/raw/smoke-iam.txt"'  # prueba el rol IAM

# ── 4. NEGOCIO end-to-end ──
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags unpause customer_etl_emr'
aws lambda invoke --function-name pyspark-stack-trigger-airflow \
  --cli-binary-format raw-in-base64-out --payload '{"dag":"customer_etl_emr"}' /dev/stdout
$S 'cd pyspark_stack && docker compose exec -T airflow-scheduler airflow dags list-runs customer_etl_emr'

# ── 5. MONITOREO (por túnel -L 9090 -L 3000 -L 9093 -L 3100) ──
curl -sf localhost:9090/-/healthy && echo "Prometheus OK"
curl -sf localhost:3000/api/health && echo "Grafana OK"
curl -sf localhost:9093/-/healthy && echo "Alertmanager OK"
curl -sf localhost:3100/ready && echo "Loki OK"

# Probar una alerta real:
docker stop node-exporter     # a los ~3 min llega el email TargetDown (job=node)
docker start node-exporter    # y después el "resolved"

# La prueba final — el ciclo de ahorro completo:
aws ec2 stop-instances --instance-ids "$ID"
aws lambda invoke --function-name pyspark-stack-startstop \
  --cli-binary-format raw-in-base64-out --payload '{"action":"start"}' /dev/stdout
# Confirmar que TODO vuelve solo: docker compose ps (todos Up), targets de Prometheus UP.
```

### 15.3 Teardown manual (borrar todo, en orden inverso)

Sin `terraform destroy`, hay que borrar cada recurso a mano para no dejar cargos. Orden inverso al de
creación (dependencias primero los consumidores, luego los recursos base):

1. **EventBridge → Scheduler**: borrar `pyspark-stack-start`, `pyspark-stack-stop`,
   `pyspark-stack-daily-etl`.
2. **Lambda**: borrar `pyspark-stack-startstop` y `pyspark-stack-trigger-airflow`.
3. **S3 → bucket datalake → Properties → Event notifications**: borrar `on-upload-raw`.
4. **EMR → EMR Serverless**: *Stop* y luego *Delete* la aplicación `pyspark-stack-spark`.
5. **EC2**: *Terminate* la instancia `pyspark-stack-node`. Después **Volumes**: borrar el volumen de
   datos (30 GiB) — **ojo, acá perdés Postgres/monitoreo**; si querés conservarlo, hacé un snapshot
   antes. **Snapshots**: borrar los del DLM si no los querés.
6. **EC2 → Elastic IPs**: *Release* la EIP (si no, sigue cobrando).
7. **EC2 → Lifecycle Manager**: borrar la policy de snapshots.
8. **VPC → Endpoints**: borrar el S3 gateway endpoint.
9. **VPC → Security groups**: borrar `pyspark-stack-sg`.
10. **S3**: vaciar y borrar `pyspark-stack-artifacts-<acct>`. El `pyspark-stack-datalake-<acct>` tiene
    tus datos — borralo solo si estás seguro (está versionado; hay que vaciar todas las versiones).
11. **Systems Manager → Parameter Store**: borrar los `/pyspark-stack/*`.
12. **Athena/Glue** (si los creaste): borrar el workgroup `pyspark-stack-analytics` y la base
    `pyspark_stack_analytics`. **CloudWatch → Log groups**: borrar `/aws/emr-serverless/pyspark-stack`.
13. **IAM → Roles**: borrar `pyspark-stack-ec2-role`, `pyspark-stack-emr-serverless-job`,
    `pyspark-stack-github-actions`, el service role del DLM (si lo creó la consola) y los roles que
    EventBridge Scheduler creó para los schedules, más los roles de ejecución de las Lambdas.
    **IAM → Identity providers**: borrar el OIDC de GitHub. **EC2 → Key pairs**: borrar
    `pyspark-stack-key`.
14. **Route 53** (si usaste §4.6): borrar el `A record` `airflow.midominio.com`.

> Con esto no queda nada cobrando. Verificá en **Billing → Cost Explorer** los días siguientes que el
> gasto cae a cero.

---

## 16. Gobierno, costo y resiliencia (extras)

Cuatro piezas AWS-nativas, independientes de todo lo anterior — no tocan Airflow, EMR Serverless ni
el compose. Las tres primeras son gratis o casi gratis; la cuarta (DLQ) es la más urgente: sin ella,
si `trigger-airflow` falla a mitad de un `SendCommand`, el evento `S3 ObjectCreated` que lo disparó
se pierde en silencio y nadie se entera hasta el día siguiente (o nunca, si es event-driven). El
detalle Terraform completo (por si más adelante migrás a IaC) está en la guía 02 §18; acá, consola.

**Paso 0 — un tema SNS para todas las alarmas.** Consola: **SNS → Topics → Create topic** →
*Standard* → nombre `pyspark-stack-alerts` → **Create topic** → **Create subscription** → *Protocol*
Email → tu dirección → **Create subscription** → confirmá el mail que te llega (si no confirmás, la
suscripción queda `PendingConfirmation` y nunca te llega nada).

### 16.1 DLQ para `trigger-airflow` y `startstop`

1. **SQS → Create queue** ×2, *Standard*: `pyspark-stack-trigger-airflow-dlq` y
   `pyspark-stack-startstop-dlq`, *Retention period* **14 days**.
2. **Lambda → `pyspark-stack-trigger-airflow` → Configuration → Asynchronous invocation → Edit** →
   *Dead-letter queue* → la cola `trigger-airflow-dlq` → **Save**. Repetí en
   `pyspark-stack-startstop` con su propia cola.
3. En el rol de ejecución de **cada** Lambda (IAM → Roles → el rol de esa función) →
   **Add permissions → Create inline policy** → JSON:

   ```json
   { "Version": "2012-10-17", "Statement": [
     { "Effect": "Allow", "Action": "sqs:SendMessage", "Resource": "<ARN de SU cola dlq>" }
   ]}
   ```

   *Policy name* `lambda-dlq` → **Create policy**.
4. **CloudWatch → Alarms → Create alarm** ×2 → métrica `AWS/SQS → ApproximateNumberOfMessagesVisible`,
   filtrada por cada `QueueName` → *Statistic* Maximum, *Period* 5 minutes → condición **Greater
   than 0** → acción: notificar al topic `pyspark-stack-alerts` del Paso 0.

> **Reprocesar un mensaje de la DLQ:** `aws sqs receive-message --queue-url <url>` te da el evento
> original; reenvialo a mano (`aws lambda invoke --function-name ... --payload file://evento.json`)
> y después `aws sqs delete-message`. Sin redrive automático: a ~13 corridas/mes no se justifica un
> pipeline de reproceso, es una cola chica que revisás cuando suena la alarma.
>
> Verificá (CLI): `aws lambda get-function --function-name pyspark-stack-trigger-airflow
> --query 'Configuration.DeadLetterConfig'` debe devolver el ARN de la cola, no `null`.

### 16.2 AWS Budgets

**Billing and Cost Management → Budgets → Create budget** → *Customize (advanced)* → *Cost budget* →
*Period* Monthly → monto (referencia: ~$35/mes start/stop o ~$83/mes 24/7, §2 — poné margen) → dos
*Alert thresholds*: **80% Actual** y **100% Forecasted** (avisa *antes* de llegar, según la
proyección de AWS) → tu email en ambas → **Create budget**. Gratis: los primeros presupuestos no
tienen costo.

### 16.3 Cost Anomaly Detection

**Billing and Cost Management → Cost Anomaly Detection → Create monitor** → *Monitor type* AWS
services → **Create** → **Create alert subscription** → *Frequency* Daily summary → *Threshold* $5
(por debajo es ruido a esta escala) → tu email → **Save**. Detecta picos que no siguen el patrón
histórico — por ejemplo un job de EMR Serverless que quedó escalando workers de más, o un DAG en loop.

### 16.4 IAM Access Analyzer

**IAM → Access Analyzer → Create analyzer** → *Zone of trust* Current account → nombre
`pyspark-stack-analyzer` → **Create analyzer**. Gratis, sin mantenimiento. A esta escala (una sola
cuenta, sin cross-account) no debería reportar nada — el valor es la detección temprana si algún día
agregás un rol con `Principal` mal acotado. Los hallazgos aparecen en la misma pantalla; opcional:
regla de EventBridge sobre `aws.access-analyzer` → el topic SNS del Paso 0, si querés que te avisen
sin entrar a la consola.

> Verificá (CLI): `aws accessanalyzer list-analyzers --query 'analyzers[].{name:name,status:status}'`
> → `status` debe ser `ACTIVE`.

---

## 17. Lineage de datos con OpenLineage

Saber "esta tabla de `analytics/` salió de qué archivo de `raw/`, pasando por qué job y qué modelo
dbt" — sin desplegar Marquez (otro contenedor con estado en una `t3.large` ya ajustada). Los eventos
de Airflow y dbt se escriben como JSON Lines a S3 y se consultan con Athena, mismo patrón que el
resto de la plataforma (§10). Detalle completo, incluida la tabla de qué cubre y qué no (Spark en
EMR Serverless queda afuera por defecto — necesita Marquez) en la
[guía 02 §21](02-produccion-aws.md#21-lineage-de-datos-con-openlineage).

**Instalación** (`requirements.txt`):

```text
apache-airflow-providers-openlineage==1.*   # verificá versión exacta compatible con Airflow 3.2.2
openlineage-dbt==1.*                        # ídem, contra la versión de dbt-core de §11.4
```

**Config** — en el `docker-compose.prod.yml` de §11.1, dentro de `x-airflow-common` →
`environment`:

```yaml
AIRFLOW__OPENLINEAGE__TRANSPORT: '{"type": "file", "log_file_path": "/opt/airflow/logs/openlineage/events.log"}'
AIRFLOW__OPENLINEAGE__NAMESPACE: "pyspark-stack-prod"
```

Y montá un path persistente para ese archivo:

```yaml
  airflow-scheduler:
    volumes:
      - /data/openlineage:/opt/airflow/logs/openlineage
```

Una task de Airflow sincroniza el archivo a S3 particionado por fecha, y una tabla externa en
Athena (con `UNNEST` sobre `inputs`/`outputs`) responde "¿qué produjo esta tabla?" — el SQL completo
y el DDL están en la guía 02 §21.4. Si más adelante necesitás lineage columna-a-columna dentro de
los jobs Spark, Marquez es el upgrade (guía 02 §21.5) — pero suma ~1-2 GB de RAM a la EC2, confirmá
margen antes de sumarlo.

---

Con esto el producto está en producción **construido 100% a mano por la consola**: mismos servicios,
misma arquitectura, mismo costo que la guía Terraform — pero cada recurso creado clic a clic y cada
política/código pegado desde esta guía. Para reproducibilidad, versionado y `destroy` limpio, la
versión Terraform es [`docs/02-produccion-aws.md`](02-produccion-aws.md).
