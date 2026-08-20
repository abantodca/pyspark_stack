# Producción en AWS, 100% por la consola

Misma arquitectura y mismo objetivo que la [guía 02](../02-produccion-aws-terraform.md) —una EC2 chica
(`t3.large`) que corre solo el orquestador (Airflow + Postgres + monitoreo) en Docker, con Spark
fuera de la caja en EMR Serverless, S3 como data lake, Lambda y EventBridge para disparar los DAGs y
el auto start/stop, monitoreo completo y CI/CD— pero construida **enteramente a mano en la consola
web de AWS**. Cero Terraform: cada recurso se crea clic a clic, y todo lo que hay que pegar
—políticas IAM, código de las Lambdas, `user_data`, secretos, configuraciones— está copy-paste en
esta misma guía.

> **Documento de referencia.** Esta variante manual todavía no fue ejecutada de extremo a extremo. La
> ruta recomendada y mantenida como código es Terraform ([guía 02](../02-produccion-aws-terraform.md));
> usá esta solo cuando haya una restricción explícita que impida IaC.

> [!WARNING]
> **Este NO es el camino recomendado, y además no fue ejecutado de extremo a extremo.**
> La ruta mantenida como código es Terraform ([guía 02](../02-produccion-aws-terraform.md)).
> Usá esta solo si hay una restricción explícita que impida IaC, o si querés
> **entender** qué crea cada bloque de Terraform antes de aplicarlo. Todo lo que ganás en visibilidad lo perdés en
> reproducibilidad: no hay `plan`, no hay `destroy`, y el teardown es una checklist de
> clics en orden inverso que, si se saltea un paso, deja recursos facturando.

> **En este documento: EJECUTAR clic a clic, ~4–6 h.** Es más lento que la guía 02, a
> propósito: cada paso es visible.
> **Salís con**: la misma plataforma que la guía 02 —EC2 orquestadora, data lake,
> EMR Serverless, disparadores, CI/CD— pero sin estado versionado que la describa.

> [!IMPORTANT]
> **Aun por consola, los comandos siguen sin llevar valores escritos a mano.** Este
> documento usa el mismo contrato de variables que la guía 02
> ([§3.1 de la 02](../02-produccion-aws-terraform.md#31-contrato-de-variables-de-entorno-léalo-antes-de-copiar-cualquier-comando)),
> pero en **modo `discover`**: como acá no hay `terraform output`, el cargador
> encuentra los recursos por sus tags.
>
> ```bash
> PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh   # EN TU MÁQUINA, una vez por terminal
> ```
>
> Por eso **los tags no son cosmética**: son el único índice que tiene el cargador. Si
> creás un recurso sin taguear, `discover` no lo encuentra y todos los comandos
> siguientes corren con la variable vacía.

**Convenciones** (las mismas de toda la documentación): los pasos marcados 🖱️ son
clics en la consola; los bloques `bash` corren en **tu máquina** salvo que digan
`# EN LA EC2`. Los tres contextos de ejecución —local, dentro de la instancia y
GitHub Actions con OIDC— no son intercambiables: están explicados en
[la guía 02, «Cómo leer esta guía»](../02-produccion-aws-terraform.md#cómo-leer-esta-guía).

**¿Consola o Terraform?** La consola sirve para entender qué crea cada bloque, para un despliegue
puntual o para aprender AWS tocando cada servicio. A cambio:

- **No hay estado ni reproducibilidad.** No existe `terraform plan`/`apply`/`destroy`: recrear todo
  en otra cuenta o región significa repetir cada clic.
- **Teardown manual.** Para no dejar cargos colgando hay que borrar cada recurso a mano en orden
  inverso (checklist en §17.3). No hay un botón «borrar todo».
- **Drift silencioso.** Un cambio a mano no queda versionado; a los tres meses nadie sabe por qué el
  security group tiene esa regla.
- **No mezcles las dos vías** para el mismo recurso. Si algo lo creaste acá y después querés pasarlo
  a Terraform, hace falta `terraform import`; si no, el `apply` duplica o choca por nombre.

## Índice

1. [Panorama y orden de creación](#1-panorama-y-orden-de-creación)
2. [Costo](#2-costo)
3. [Prerrequisitos](#3-prerrequisitos)
   - 3.1 [Contexto para los comandos: modo `discover`](#31-contexto-para-los-comandos-modo-discover)
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
11. [Archivos de aplicación: una sola fuente de verdad](#11-archivos-de-aplicación-una-sola-fuente-de-verdad)
12. [DAGs de producción](#12-dags-de-producción)
13. [Operación diaria](#13-operación-diaria)
14. [Smoke tests](#14-smoke-tests)
15. [Seguridad y costos](#15-seguridad-y-costos)
16. [DLQ, alertas y gobierno](#16-dlq-alertas-y-gobierno)
17. [Backup, recuperación y eliminación](#17-backup-recuperación-y-eliminación)

---

## 1. Panorama y orden de creación

> **En esta sección: LEER, ~10 min. No creás nada todavía.**
> **Salís con**: el orden de creación, que acá importa más que en la guía 02: sin
> `terraform apply` que resuelva el grafo de dependencias, **el orden lo ponés vos**.
> Crear un recurso antes que aquel del que depende no da un error claro: da un
> desplegable vacío en la consola donde debería estar la opción que buscás.

La arquitectura es **idéntica** a la de la guía 02 (el detalle conceptual y los diagramas están en
[`docs/03-arquitectura.md`](../03-arquitectura.md)). Una EC2 `t3.large` corre solo el orquestador en
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
| 13 | (En la EC2) subir repo, generar `.env`, `docker compose up` (§11.1) | todo lo anterior |

> Los nombres son fijos en toda la guía: región **`us-east-1`**, prefijo **`pyspark-stack`**,
> `<acct>` = tu **Account ID** (arriba a la derecha en la consola, o *IAM → Dashboard*). Donde veas
> `<acct>` reemplazalo por ese número de 12 dígitos.

---

## 2. Costo

> **En esta sección: LEER, ~5 min.**
> **Salís con**: qué vas a pagar. **La vía de creación no cambia el precio**: es el
> mismo número que la guía 02.

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

> **En esta sección: VERIFICAR, ~10 min.**
> **Salís con**: la cuenta, el CLI y el par de claves listos.

> [!IMPORTANT]
> **Aunque construyas por consola, el AWS CLI no es opcional.** Las verificaciones de
> cada sección, los smoke tests (§14) y la operación diaria (§13) van por CLI: la
> consola es para *crear*, no para *comprobar*. Y sin CLI no podés cargar el contexto
> en modo `discover`, que es lo que hace que los comandos de esta guía no lleven
> valores escritos a mano.

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

### 3.1 Contexto para los comandos: modo `discover`

Los pasos de **creación** de esta guía son clics en la consola, así que ahí los nombres literales
son lo correcto: es lo que vas a tipear en un formulario. Pero los bloques de **verificación y
operación** son CLI, y ahí valen las mismas razones que en la
[guía 02 §3.1](../02-produccion-aws-terraform.md#31-contrato-de-variables-de-entorno-léalo-antes-de-copiar-cualquier-comando):
una IP o un account id pegado a mano caduca o apunta a la cuenta equivocada.

Acá no hay state de Terraform del cual leer, así que `scripts/prod-env.sh` corre en su segundo
modo: **`discover`**, que descubre los mismos valores con el AWS CLI (por tag `Name`, por nombre de
aplicación EMR y por convención de bucket) y exporta **exactamente las mismas variables**.

Es el mismo archivo en las dos guías: **copialo de la
[guía 02 §5.5, Paso 0c](../02-produccion-aws-terraform.md#55-desplegar-subir-código-y-túnel-ssh)**
(allá se crea junto al `terraform apply` que lo alimenta; acá lo alimenta el AWS CLI). No hace falta
que leas nada más de esa sección: el archivo es autocontenido y `chmod +x scripts/prod-env.sh` lo
deja listo.

```bash
export PROD_ENV_SOURCE=discover
source ./scripts/prod-env.sh
./scripts/prod-env.sh --check     # confirmá qué encontró antes de operar
```

**Cuándo sourcearlo.** `discover` descubre lo que ya existe, así que ahora mismo solo encontraría el
account id y la región: todavía no creaste nada. La primera vez que sirve de verdad es en
[§4.3](#43-ec2--ebs--user_data--elastic-ip), en cuanto la EC2 esté corriendo. A partir de ahí,
volvé a sourcearlo **cada vez que crees recursos nuevos** en la consola, para que los descubra —
por eso los bloques de verificación de las secciones siguientes lo repiten.

Si `--check` muestra `INSTANCE_ID` vacío después de §4.3, revisá que la EC2 tenga el tag
`Name=pyspark-stack-node`: el modo `discover` busca por ese tag.

> Si cambiaste el prefijo al crear los recursos, exportá `NAME_PREFIX` antes de sourcear:
> `NAME_PREFIX=mi-stack PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh`.

---

## 4. Núcleo: EC2 con Docker

> **En esta sección: CREAR en la consola, ~90 min.** Es la más larga.
> **Salís con**: la EC2 corriendo Airflow y Postgres, con disco de datos aparte, IP
> estable y apagado automático.

> **Regla de esta sección: tagueá TODO lo que crees, sin excepción.** En la guía 02 los
> tags son buena práctica; acá son **infraestructura crítica**: el modo `discover` de
> `prod-env.sh` encuentra los recursos por tag, porque no hay `terraform output`. Un
> recurso sin taguear es un recurso que ninguno de los comandos siguientes va a
> encontrar — y la variable vacía no da error, da un comando que corre contra nada.

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
> ⚠️ **Es mantenimiento posterior, no un paso de esta sección.** Necesita `scripts/prod-env.sh`, que
> se copia en §3.1, y tiene sentido recién con el stack desplegado. Leelo ahora para saber que
> existe; volvé cuando tu IP cambie.
>
> ```bash
> #!/usr/bin/env bash
> # scripts/update-sg-ip.sh — pone tu IP de cliente actual en las reglas 22 y 443 del SG.
> set -euo pipefail
>
> # El modo discover resuelve $SECURITY_GROUP_ID y $AWS_REGION: no hay que repetir el
> # nombre del SG acá ni asumir la región.
> PROD_ENV_SOURCE=discover source "$(dirname "$0")/prod-env.sh"
>
> MYIP="$(curl -s https://checkip.amazonaws.com)/32"
> echo "IP actual: $MYIP  ·  SG: $SECURITY_GROUP_ID"
> SG_ID="$SECURITY_GROUP_ID"
> REGION="$AWS_REGION"
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
   - El volumen de datos se crea y adjunta después del primer arranque. Así puedes identificarlo
     por su ID exacto antes de formatearlo.
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
dnf install -y docker git && systemctl enable --now docker

COMPOSE_VERSION=v5.3.1
DOCKER_CONFIG=/usr/local/lib/docker
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -fSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
  -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
usermod -aG docker ec2-user

echo 'vm.max_map_count=262144' > /etc/sysctl.d/99-pyspark.conf && sysctl --system
```

**Crear y montar el volumen de datos.**

1. **EC2 → Volumes → Create volume**: 30 GiB, gp3, cifrado y en la misma AZ que la instancia.
2. Tag: `Name=pyspark-stack-data`.
3. **Actions → Attach volume** → instancia `pyspark-stack-node`.
4. Copia el ID del volumen, por ejemplo `vol-0123456789abcdef0`.
5. En Session Manager o SSH ejecuta:

```bash
VOLUME_ID="vol-0123456789abcdef0"
SERIAL="${VOLUME_ID//-/}"
DEVICE="$(lsblk -dpno NAME,SERIAL | awk -v serial="$SERIAL" '$2 == serial {print $1}')"

test -b "$DEVICE"
if ! blkid "$DEVICE" >/dev/null 2>&1; then
  sudo mkfs.xfs "$DEVICE"
fi

sudo mkdir -p /data
sudo mount "$DEVICE" /data
UUID="$(sudo blkid -s UUID -o value "$DEVICE")"
grep -q "$UUID" /etc/fstab ||
  echo "UUID=$UUID /data xfs defaults,nofail 0 2" | sudo tee -a /etc/fstab

sudo mkdir -p /data/{postgres,airflow-logs,backups/postgres,prometheus,grafana,loki}
sudo chown -R ec2-user:ec2-user /data
sudo chown 50000:0 /data/airflow-logs
printf 'e /data/airflow-logs - - - 7d\n' | sudo tee /etc/tmpfiles.d/airflow-logs.conf
sudo chown 65534:65534 /data/prometheus
sudo chown 472:472 /data/grafana
sudo chown 10001:10001 /data/loki
```

El script solo formatea el volumen cuyo serial coincide con el ID indicado. Nunca elige “el primer
NVMe”, porque el orden puede cambiar entre reinicios.

**Elastic IP** (sin ella, cada stop/start del ahorro cambiaría la IP pública y romperían los túneles)
— Consola: **EC2 → Elastic IPs → Allocate Elastic IP address** → **Allocate**. Luego, con la EIP
seleccionada: **Actions → Associate Elastic IP address** → *Instance* `pyspark-stack-node` →
**Associate**.

**No anotes la IP.** Con la EC2 ya creada y etiquetada, el modo `discover` de §3.1 la descubre solo:
es el primer momento de la guía en que el contexto sirve de verdad. De acá en adelante, todos los
`ssh` y `rsync` salen de `$SSH_TARGET`, no de una IP copiada a mano —que además cambiaría si algún
día reasignás la EIP—:

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh
./scripts/prod-env.sh --check     # INSTANCE_ID y PUBLIC_IP ya deben tener valor
```

> Verificá (CLI, opcional): la instancia queda `running` con IMDSv2 `required` y el agente SSM
> `Online` a los pocos minutos:
> ```bash
> PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh   # encuentra la EC2 por su tag Name
> aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
>   --query 'Reservations[].Instances[].{estado:State.Name,imdsv2:MetadataOptions.HttpTokens}'
> ```

### 4.4 Automatización: Lambda startstop + EventBridge Scheduler

En vez de apagar la EC2 a mano, una Lambda la prende/apaga y EventBridge Scheduler la dispara por
cron. La Lambda no apaga a ciegas: antes consulta **si hay DAG runs activos en Airflow** (guardia
anti-corte) y, si los hay, no apaga — así el apagado es *job-aware* (§12).

**Paso 1 — Crear la función.** Consola: **Lambda → Create function**.

- *Author from scratch* · *Function name* `pyspark-stack-startstop` · *Runtime* **Python 3.12** ·
  *Architecture* x86_64 → **Create function**.
- En el editor de código, reemplazá `lambda_function.py` por el código que se indica abajo. Luego
  **Deploy**.

El código es **el mismo** que el de la guía 02: no existe una versión "de consola". Copialo del
bloque `startstop.py` de
[guía 02 §5.4](../02-produccion-aws-terraform.md#54-automatización-eventbridge--lambda) —completo, con
el guard job-aware y la rama `force`— y pegalo en el editor de la Lambda.

> **Por qué no está repetido acá.** Es la regla de §11: la consola crea infraestructura, no
> mantiene una segunda copia de tus archivos. Cuando este documento traía su propio `startstop.py`,
> la copia se quedó sin la evaluación multi-instancia ni el `force` que la guía 02 sí tiene.

Guardalo también en tu repo. Acá vive en `lambdas/startstop.py`; en la guía 02 vive en
`infra/lambdas/startstop.py` porque allá lo empaqueta Terraform. Es el mismo archivo.

**Paso 2 — Handler, timeout y variables.**

- *Runtime settings → Edit* → **Handler** = `lambda_function.handler` (el código define `def
  handler`, no el `lambda_handler` que asume la consola).
- *Configuration → General configuration → Edit* → **Timeout** = **2 min** (el guard job-aware espera
  al SSM SendCommand).
- *Configuration → Environment variables → Edit* → agregá `TAG_KEY=AutoStartStop` y
  `TAG_VALUE=true`.

**Paso 3 — Permisos (IAM inline policy en el rol de ejecución de la Lambda).** *Configuration →
Permissions* → clic en el **Role name** (te lleva a IAM) → **Add permissions → Create inline policy →
JSON** → pegá el JSON de abajo, reemplazando `<acct>` por tu Account ID y `i-xxxx` por el id de tu
instancia.

Un formulario de la consola necesita los ARNs escritos, así que acá el reemplazo es inevitable —
pero no hace falta que los busques a mano. Este comando imprime las dos líneas ya resueltas, listas
para copiar:

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh
printf 'instancia: arn:aws:ec2:%s:%s:instance/%s\ndocumento: arn:aws:ssm:%s::document/AWS-RunShellScript\n' \
  "$AWS_REGION" "$ACCOUNT_ID" "$INSTANCE_ID" "$AWS_REGION"
```

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

**Paso 3b — Log retention.** **CloudWatch → Log groups** → buscá
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
**reemplaces todo el archivo** por una versión más completa — no lo vayas parcheando a mano, cada
sección te remite al archivo entero de nuevo.

El archivo es **el mismo** que el de la guía 02, que ya lo trae completo y comentado:

| Momento | De dónde lo copiás |
|---|---|
| Ahora (mínimo: Airflow + Postgres) | [guía 02 §5.5, Paso 0](../02-produccion-aws-terraform.md#55-desplegar-subir-código-y-túnel-ssh) |
| Versión definitiva | [guía 02 §14.1](../02-produccion-aws-terraform.md#141-docker-composeprodyml--base) |

Copiá también el **`Dockerfile.airflow.prod`** del Paso 0b de esa misma sección: el Compose lo
referencia en `build.dockerfile`, y es una imagen distinta de la del dev local.

> **Gotcha §4.5 — no reuses `Dockerfile.airflow`.** Es la imagen del stack local: trae JDK, Spark y
> Hadoop porque ahí Airflow corre `spark-submit` en el mismo contenedor. Acá el cómputo va a EMR
> Serverless, así que esos ~1.5 GB solo alargan el build y ocupan disco en una `t3.large`.
> `Dockerfile.airflow.prod` no los instala. (Esta guía documentaba `Dockerfile.airflow` por una
> copia vieja; si seguiste una versión anterior, rehacé el build.)

```bash
# La EC2 ya existe y está tagueada: `discover` encuentra su id y su IP sola.
# Nada de "<tu-elastic-ip>" pegado a mano — es el placeholder que más veces se
# queda sin reemplazar, y el rsync termina intentando conectar a un host inexistente.
export PROD_ENV_SOURCE=discover
source ./scripts/prod-env.sh
./scripts/prod-env.sh --check

aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"

# Subir el proyecto (docker-compose.prod.yml creado arriba incluido).
# --exclude '.env': el .env local (dev) no debe pisar el de prod,
rsync -avz --exclude '.git' --exclude '.env' --exclude '__pycache__' \
  -e "$RSYNC_SSH" ./ "$SSH_TARGET:$REMOTE_DIR/"

$SSH "$SSH_TARGET" \
  'cloud-init status --wait && docker compose version && df -h /data | tail -1'

$SSH "$SSH_TARGET" \
  "cd $REMOTE_DIR && docker compose -f $COMPOSE_PROD up -d --build"

$SSH -L 8082:localhost:8082 "$SSH_TARGET"
```

UIs (con el túnel abierto): Airflow `localhost:8082` — o, si exponés la web por HTTPS (§4.6), directo
en `https://airflow.midominio.com` sin túnel. Spark ya no corre en la EC2 (los jobs van a EMR
Serverless — su UI y logs se ven desde la consola de EMR / CloudWatch / S3, §9). No hay Jupyter en
prod: la exploración interactiva queda para el stack local (`docs/01`).

> Esto es el núcleo, no el final: la infra se arma incrementalmente. Seguí con S3 (§5), orquestación
> (§6), secretos (§7) y monitoreo (§9); cada una te da una versión más completa de
> `docker-compose.prod.yml` para reemplazar el creado arriba (Spark/HDFS/Jupyter nunca estuvieron en
> el archivo). El arranque **real** de producción es §11.1:
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

$SSH "$SSH_TARGET" "
  sudo docker run --rm -v /data/certs:/etc/letsencrypt certbot/dns-route53 certonly \
    --dns-route53 -d '$DOMAIN' -m '$EMAIL' --agree-tos -n &&
  sudo chmod -R g+rX /data/certs   # el api-server corre con gid 0 (grupo root): así lee el privkey
"
```

El cert queda en `/data/certs/live/$DOMAIN/{fullchain.pem,privkey.pem}` (en el EBS, sobrevive al
stop/start).

**Paso 4 — Editá el `airflow-apiserver` de tu `docker-compose.prod.yml` directamente** (es un solo
archivo, no hay nada que fusionar).

**`.env` — esta sección agrega `AIRFLOW_DOMAIN`.** El FQDN no es secreto, pero sí tiene que
sobrevivir: `load-secrets.sh` (§7) genera el `.env` **desde cero** en cada corrida, así que un
`echo ... >> .env` a mano se pierde en el próximo deploy y el `airflow-apiserver` queda sin TLS.
Publicalo en Parameter Store, que es la fuente que ese script lee:

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh
aws ssm put-parameter --name "/${NAME_PREFIX}/config/airflow_domain" \
  --type String --value "airflow.midominio.com" --overwrite
```

Para probar ahora mismo, antes de tener `load-secrets.sh`, alcanza con
`echo "AIRFLOW_DOMAIN=airflow.midominio.com" >> .env` en la EC2 — pero el `put-parameter` de arriba
es el que lo hace durable.

Reemplazá el bloque
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
      - "127.0.0.1:8082:8080"
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

> **En esta sección: CREAR en la consola, ~45 min.**
> **Salís con**: los buckets del lake cerrados y cifrados, y una aplicación de EMR
> Serverless con **su propio** rol de ejecución.

> **La confusión más cara de esta sección**: darle permisos de S3 al rol de la **EC2**
> no habilita al job de **Spark**. Son dos identidades distintas para dos cómputos
> distintos. Si el job falla con `AccessDenied`, mirá el rol de ejecución de EMR, no el
> de la instancia.

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

**Lifecycle del `datalake`** — *Management → Create lifecycle rule*:

- *Rule name* `tiering` · *Rule scope*: **Apply to all objects in the bucket** (aceptá el aviso).
- *Lifecycle rule actions*: **Move current versions of objects between storage classes**:
  - **Standard-IA** a los **30** días.
  - **Glacier Instant Retrieval** a los **90** días.
- **Create rule**.

**Lifecycle de `artifacts`** — cree reglas separadas porque los logs son operativos, no datos del
lake:

- Prefijo `logs/airflow/`: expirar objetos actuales a los **90 días**.
- Prefijo `emr/logs/`: expirar objetos actuales a los **90 días**.
- Todo el bucket: borrar versiones no actuales a los **30 días** y abortar multipart incompletos a
  los **7 días**. No transicione cada log pequeño a Glacier: requests y recuperación pueden costar
  más que el almacenamiento ahorrado.

> (Opcional) *Create folder* para `raw/`, `curated/`, `analytics/` — también aparecen solos con la
> primera escritura.

> Verificá (CLI): `aws s3 ls | grep pyspark-stack` → los 2 buckets.

**`.env` — esta sección agrega `DATALAKE_BUCKET` y `ARTIFACTS_BUCKET`.** El `.env` de la EC2 se
genera desde Parameter Store (§7), así que cada variable se publica en la sección que crea su
recurso, no todas juntas al final. Publicá estas dos ahora:

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh

aws ssm put-parameter --name "/${NAME_PREFIX}/config/datalake_bucket" \
  --type String --value "$DATALAKE_BUCKET" --overwrite
aws ssm put-parameter --name "/${NAME_PREFIX}/config/artifacts_bucket" \
  --type String --value "$ARTIFACTS_BUCKET" --overwrite
```

Sin CLI: **Systems Manager → Parameter Store → Create parameter**, *Type* **String**, con esos dos
nombres y el nombre real de cada bucket como valor. El inventario completo del `.env` está en la
[guía 02 §13.4](../02-produccion-aws-terraform.md#134-materializar-env).

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
> PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh   # $RAW_URI ya trae tu account id
> $SSH "$SSH_TARGET" "aws s3 cp /etc/hostname '$RAW_URI/smoke-iam.txt'"
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

- *Name* `pyspark-stack-spark` · *Type* **Spark** · *Release version* **emr-7.13.0**.
- *Application setup options*: **Use custom settings**:
  - **Pre-initialized capacity**: dejala en **0** (para escalar a cero de verdad).
  - **Application behavior**: **Auto-start** *On*; **Auto-stop** *On*, *idle timeout* **15 minutes**.
  - **Maximum capacity**: **16 vCPU / 64 GB** (techo de gasto).
  - **Network connections**: dejala **sin VPC** (los jobs solo tocan S3). Agregá VPC solo si el job
    accede a recursos privados de tu red (RDS privada, etc.).
- **Create application**. No hace falta que anotes el **Application ID**: `discover` lo encuentra
  por el nombre de la aplicación y lo deja en `$EMR_APP_ID`, y el bloque del final de esta
  sección lo publica en SSM para que los DAGs lo lean del `.env`.

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
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh
aws s3 sync spark-apps/emr/ "$EMR_ENTRYPOINTS_URI/"
```

> Sin CLI: **S3 → bucket artifacts → Create folder `emr/` → Upload** y subís los `.py` a mano.

**Probar un job a mano (opcional).** Así lo arma el operator de Airflow; equivalente CLI:

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh

# jq arma el JSON con los valores ya resueltos: dentro de comillas simples bash no
# expandiría nada, y un "<acct>" sin reemplazar produce un logUri inválido que mata el
# job con "Unable to push logs ... Parameter validation failed" sin correr una línea.
aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_JOB_ROLE_ARN" \
  --job-driver "$(jq -nc \
      --arg entry "$EMR_ENTRYPOINTS_URI/wordcount.py" \
      --arg bucket "$DATALAKE_BUCKET" \
      '{sparkSubmit: {
          entryPoint: $entry,
          entryPointArguments: [$bucket, "2026-07-16"],
          sparkSubmitParameters: "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=2"
        }}')" \
  --configuration-overrides "$(jq -nc \
      --arg logs "$EMR_LOGS_URI/" \
      '{monitoringConfiguration: {s3MonitoringConfiguration: {logUri: $logs}}}')"
```

> `wordcount.py` no depende de datos previos: sirve como primer smoke test. Para
> `customer_etl.py` primero tienen que existir `orders.csv`, `products.json` y `customers.csv`
> en `$RAW_URI/customer_etl/`.

La config de Spark va **por-job** (en `sparkSubmitParameters`), no en un `spark-defaults.conf`: en EMR
Serverless no hay caja donde montarlo. EMR escribe los logs a S3 (`emr/logs/`) y a CloudWatch, y
expone la Spark UI de cada corrida desde la consola de EMR.

> Verificá (CLI): `aws emr-serverless get-application --application-id "$EMR_APP_ID"
> --query 'application.state'` (con el contexto de §3.1 cargado).

**`.env` — esta sección agrega `EMR_APP_ID` y `EMR_JOB_ROLE_ARN`.** Son los dos valores que el DAG
de producción pasa al `EmrServerlessStartJobOperator`. Publicalos ahora, igual que los buckets:

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh

aws ssm put-parameter --name "/${NAME_PREFIX}/config/emr_app_id" \
  --type String --value "$EMR_APP_ID" --overwrite
aws ssm put-parameter --name "/${NAME_PREFIX}/config/emr_job_role_arn" \
  --type String --value "$EMR_JOB_ROLE_ARN" --overwrite
```

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

> **En esta sección: CREAR en la consola, ~40 min.**
> **Salís con**: los DAGs disparables por cron y por archivo nuevo en `raw/`, sin
> exponer la API de Airflow.

El disparo va por **SSM**, no por la API REST de Airflow: la Lambda no necesita ruta de
red a la EC2, solo permiso IAM sobre esa instancia. Es lo que permite que la API siga
cerrada.

Airflow corre dentro de la EC2. Para dispararlo desde AWS (por cron o cuando llega un archivo a S3)
se usa una **Lambda que ejecuta `airflow dags trigger` vía SSM `SendCommand`** — sin abrir puertos ni
depender de la web.

### 6.1 Lambda que dispara los DAGs vía SSM (con retry si la EC2 está apagada + contrato de datos)

**Paso 1 — Crear la función.** Consola: **Lambda → Create function** → *Function name*
`pyspark-stack-trigger-airflow` · *Runtime* **Python 3.12** → **Create function**. Pegá el código y
**Deploy**. Trae dos mejoras sobre una Lambda mínima: **(a)** si la EC2 está apagada, la prende y
deja que el reintento (SQS para eventos S3, retry async de Lambda para el cron) la vuelva a disparar
en unos minutos, en vez de fallar en silencio; **(b)** un **contrato de datos** liviano (stdlib, sin
Lambda Layers) rechaza archivos con columnas faltantes antes de gastar en cómputo de EMR.

El código es **el mismo** que el de la guía 02. Copialo del bloque `trigger_airflow.py` de
[guía 02 §7.1](../02-produccion-aws-terraform.md#71-lambda-que-dispara-los-dags-vía-ssm) y pegalo en el editor de la Lambda;
guardalo en tu repo como `lambdas/trigger_airflow.py` (en la guía 02 es
`infra/lambdas/trigger_airflow.py`, mismo contenido).

Trae, y conviene no recortarlos, el `run_id` determinístico —que evita el doble dagrun cuando SQS
reintenta— y el gate barato de contrato de datos por Range GET.

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
no `"*"` — a diferencia de `DescribeInstances`, sí admite scoping por recurso.

**Paso 3b — Log retention + límite de concurrencia.** *Configuration → General
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
> PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh
> aws ssm describe-instance-information \
>   --query "InstanceInformationList[?InstanceId=='$INSTANCE_ID'].PingStatus"   # ["Online"]
> aws lambda invoke --function-name "$LAMBDA_TRIGGER_NAME" \
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
Properties → Event notifications → Create event notification** → *Event name* `customer-etl-ready` ·
*Prefix* `raw/manifests/customer_etl/` · *Suffix* `.json` · *Event types* **All object create events** · *Destination*: **SQS queue** →
`pyspark-stack-trigger-events` → **Save changes**.

**Paso 4 — La Lambda consume la cola.** Consola: **Lambda → `pyspark-stack-trigger-airflow` →
Configuration → Triggers → Add trigger** → **SQS** → `pyspark-stack-trigger-events` → *Batch size*
**1** (un manifest completo = una invocación) → **Add**.

> Los dos disparadores (cron y evento S3) apuntan al DAG de producción `customer_etl_emr` (§12) —
> no al `customer_etl_dag` dev-local. Publique el manifest solamente después de los tres objetos;
> `customer_etl_emr` recibe bucket, key y run_date por `dag_run.conf` y el job registra el manifest.

> Verificá el retry: apagá la EC2, publicá un manifest válido y mirá **SQS → la cola →
> Monitoring** — el mensaje queda "in flight" (procesándose o esperando el próximo intento) hasta
> que la EC2 esté arriba y el DAG se dispare solo.

---

## 7. Secretos y parámetros

> **En esta sección: CREAR en la consola, ~20 min.**
> **Salís con**: los defaults débiles de desarrollo reemplazados por secretos reales en
> SSM, y el `.env` de la EC2 generado desde ahí en cada despliegue.

> [!WARNING]
> **Hasta terminar esta sección, el stack corre con credenciales de ejemplo.** El
> `docker-compose.prod.yml` arranca igual con los defaults de dev, así que nada te va a
> avisar. No dejes la EC2 accesible desde Internet hasta cerrar esto.

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

**Paso 3b — Cerrar la configuración NO secreta.** La EC2 necesita también valores que no son
secretos (`EMR_APP_ID`, los buckets, el ARN del rol del job). Reconstruirlos dentro del host
(`"pyspark-stack-datalake-${ACCT}"`, `list-applications | ?name=='pyspark-stack-spark'`) vuelve a
hardcodear el prefijo en un tercer lugar y falla en runtime el día que algo cambie de nombre. Por eso
van a Parameter Store como **String** (no son secreto), bajo `config/`.

**La mayoría ya la publicaste** en la sección que creó cada recurso, que es la regla del contrato:

| Ya publicado | Sección |
|---|---|
| `datalake_bucket`, `artifacts_bucket` | §5.1 |
| `emr_app_id`, `emr_job_role_arn` | §5.4 |

Acá se cierra el inventario con los dos que no pertenecen a ningún recurso, sino al stack entero:

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh

put_config() { aws ssm put-parameter --name "/${NAME_PREFIX}/config/$1" \
                 --type String --value "$2" --overwrite; }

put_config aws_region  "$AWS_REGION"
put_config name_prefix "$NAME_PREFIX"
```

Sin CLI: **Systems Manager → Parameter Store → Create parameter** (×2), *Type* **String**, nombres
`/pyspark-stack/config/aws_region` y `/pyspark-stack/config/name_prefix`.

Comprobá el inventario completo, que es exactamente lo que va a leer `load-secrets.sh`:

```bash
aws ssm get-parameters-by-path --path "/${NAME_PREFIX}/config" --recursive \
  --query 'Parameters[].[Name,Value]' --output text
```

La policy del Paso 3 ya cubre estos parámetros: `parameter/pyspark-stack/*` incluye `config/`.

> Si expusiste la web por HTTPS (§4.6), `airflow_domain` ya lo publicaste ahí. Es la única variable
> extra que necesita el TLS en esta guía: el compose de 02b deriva las rutas del cert del propio
> `${AIRFLOW_DOMAIN}`, en vez de las cinco variables separadas que usa la
> [guía 02 §5.6](../02-produccion-aws-terraform.md#56-exponer-la-web-de-airflow-https-nativo-acceso-desde-la-ip-del-operador).

**Paso 4 — Script que materializa el `.env` desde SSM** — `scripts/load-secrets.sh`, que corre
**en la EC2**.

El contenido está en la [guía 02 §13.4](../02-produccion-aws-terraform.md#134-materializar-env) y no se
reproduce acá a propósito: es un archivo del repositorio, y §11 de esta guía fija la regla de no
mantener una segunda copia que pueda divergir. Copialo de allí tal cual — **funciona igual por
consola que por Terraform**, porque no depende del state: lee todo lo que cuelga de
`/pyspark-stack/` en Parameter Store y lo convierte en líneas del `.env`, tomando el nombre de la
variable del último segmento del path (`/pyspark-stack/config/emr_app_id` → `EMR_APP_ID`).

Esa propiedad es la que hace que el Paso 3b alcance: los parámetros que acabás de crear a mano
aparecen solos en el `.env`, sin tocar el script.

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

> **En esta sección: CONFIGURAR, ~40 min.**
> **Salís con**: GitHub validando y desplegando sin una sola access key guardada.

> **`[CI]` no se prueba en tu terminal.** Un bloque que corre con tus credenciales de
> admin no demuestra nada sobre el rol de OIDC: son identidades distintas y fallan
> distinto. Los dos errores clásicos —`Could not assume role` (el `sub` del trust no
> coincide con tu org/repo) y `Unable to locate credentials` (falta
> `permissions: id-token: write`)— solo aparecen del lado de GitHub.

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

Los workflows son los de la [guía 02 §11](../02-produccion-aws-terraform.md#11-cicd-con-github-actions-y-oidc)
**menos el job `terraform`**: por este camino no hay IaC que validar. El resto —lint, validación de
DAGs y security— es idéntico, incluido el detalle que hace que el CI sirva: el job de DAGs instala
`apache-airflow==3.2.2` con el mismo constraints file que la imagen de producción, para que un DAG no
pueda pasar el CI contra una versión de Airflow distinta de la que corre en la EC2.

**`.github/workflows/ci.yml`** — 4 jobs: lint (ruff), validación de DAGs (pytest sobre el `DagBag`),
security (gitleaks) y **dbt Slim CI**, el único con credenciales AWS (rol `dbt_ci` de §8.1b, no el de
deploy; requiere que `dbt/` exista — es roadmap):

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

El test de integridad de DAGs (`tests/test_dag_integrity.py`) es el de la
[guía 02 §11.3](../02-produccion-aws-terraform.md): el repositorio ya trae la versión local, ampliala
con el contrato del DAG de producción.

**Puesta en marcha:** hacé el primer `git push` a `main` que toque `dags/` o `spark-apps/emr/` (con la
EC2 encendida el sync-down baja los DAGs y corre el smoke; apagada, queda en S3 y se aplica al próximo
encendido). Los PRs disparan **CI**.

---

## 9. Monitoreo

> **En esta sección: CREAR, ~30 min. Es opcional para el primer despliegue.**
> **Salís con**: Prometheus, Grafana, Alertmanager y Loki corriendo dentro de la misma
> EC2, sin costo adicional de AWS.

Corre en la misma caja que Airflow: es lo barato, y es también el límite — si la EC2
cae, cae el monitoreo con ella. Vale como observabilidad de laboratorio, no como
alerta de disponibilidad.

Observabilidad completa corriendo **dentro de la EC2** junto al `docker-compose`: métricas + alertas
+ logs centralizados (Prometheus + Grafana + Alertmanager + Loki). **No hay recursos de consola AWS
que crear acá**: es todo archivos de configuración en el repo + contenedores en `docker-compose.prod.yml`.
Las configuraciones son las de la guía 02 §12; el Compose que las monta es el override de
§14.2 de esa guía. Se reproducen abajo para que esta sea autocontenida.

Qué se monitorea:

| Señal | Exporter / fuente | Puerto interno |
|---|---|---|
| Host (CPU, RAM, disco, red) | `node-exporter` | 9100 |
| Contenedores (uso por servicio) | `cAdvisor` | 8080 |
| Airflow (DAGs, tasks, duraciones) | Airflow StatsD → `statsd-exporter` | 9102 |
| Spark (jobs) | **EMR Serverless** → CloudWatch + logs a S3 (`emr/logs/`) | — (managed) |
| Logs de todos los contenedores | `Grafana Alloy` → `Loki` | 3100 |
| Alertas | `Alertmanager` → email | 9093 |
| Dashboards | `Grafana` | 3000 |

Estructura `monitoring/`:

```
monitoring/
├── prometheus/{prometheus.yml, alerts.yml}
├── alertmanager/alertmanager.yml
├── statsd/statsd_mapping.yml
├── loki/loki-config.yml
├── alloy/config.alloy
└── grafana/
    ├── provisioning/{datasources/datasources.yml, dashboards/dashboards.yml}
    └── dashboards/overview.json
```

De estos archivos, `prometheus.yml`, `alerts.yml`, `alloy/config.alloy` y `loki/loki-config.yml`
están escritos en la [guía 02 §12.2](../02-produccion-aws-terraform.md#122-prometheus); el resto
(`statsd_mapping.yml` y el provisioning de Grafana) es **roadmap**. El override que los monta es la [guía 02
§14.2](../02-produccion-aws-terraform.md#142-docker-composeprodmonitoringyml--override-de-observabilidad).
Los dos más importantes de tener a mano:

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
        expr: 100 * (1 - node_filesystem_avail_bytes{mountpoint="/data"} / node_filesystem_size_bytes{mountpoint="/data"}) > 80
        for: 15m
        labels: { severity: warning }
        annotations: { summary: "Disco /data supera 80%" }
      - alert: HostDiskCritical
        expr: 100 * (1 - node_filesystem_avail_bytes{mountpoint="/data"} / node_filesystem_size_bytes{mountpoint="/data"}) > 90
        for: 5m
        labels: { severity: critical }
        annotations: { summary: "Disco /data supera 90%" }
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

`prometheus.yml` y `loki-config.yml` copialos tal cual de la
[guía 02 §12.2](../02-produccion-aws-terraform.md#122-prometheus). Loki conserva 7 días y
Prometheus 15 días con tope de 5 GiB. `statsd_mapping.yml`, el provisioning de Grafana y
`overview.json` siguen sin estar escritos: hasta que existan, corré solo el Compose base, sin el
override de monitoreo.

**Acceso (por túnel SSH):**

```bash
$SSH -L 3000:localhost:3000 -L 9090:localhost:9090 -L 9093:localhost:9093 -L 3100:localhost:3100 "$SSH_TARGET"
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

> **En esta sección: CREAR, ~15 min. Opcional.**
> **Salís con**: poder consultar el data lake con SQL puro, sin prender Spark ni un
> cluster.

Es la respuesta a «quiero mirar los datos», que es distinta de «quiero transformarlos».
Para lo segundo, el criterio de motor está en la
[guía 02 §17](../02-produccion-aws-terraform.md#17-qué-motor-usar-para-cada-tarea).

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
- **Data usage controls → Track query limit per query** → **5,000 MB** (sin esto,
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

**Paso 3b — Mantenimiento: compactación y expiración de snapshots.** Sin esto,
cada `MERGE` deja archivos chicos y un snapshot nuevo; después de meses de corridas 3x/semana el
*planning time* de las queries se degrada solo, sin que nadie lo note hasta que ya molesta. Desde
el mismo Query editor, o como task semanal de un DAG (`AthenaOperator`, guía 02 §16.3):

```sql
-- Compacta archivos chicos en archivos más grandes
OPTIMIZE pyspark_stack_analytics.ventas REWRITE DATA USING BIN_PACK;

-- Libera snapshots/archivos ya no referenciados, más viejos que la retención por defecto
VACUUM pyspark_stack_analytics.ventas;
```

> Verificá la sintaxis exacta contra tu versión de Athena engine — el soporte de mantenimiento de
> Iceberg se fue agregando de forma incremental. El detalle completo (DAG semanal, por qué viernes
> y no domingo por el auto start/stop) está en la [guía 02 §16.3](../02-produccion-aws-terraform.md#163-mantenimiento-iceberg).

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

## 11. Archivos de aplicación: una sola fuente de verdad

> **En esta sección: LEER y aplicar la regla, ~10 min.**
> **Salís con**: la frontera clara entre lo que crea la consola (infraestructura) y lo
> que vive en el repositorio (aplicación).

> **La regla que evita el peor problema de esta guía**: la consola crea
> infraestructura, **no** mantiene una segunda copia de tus archivos de aplicación. Si
> editás el Compose o un DAG desde la consola de la EC2, el próximo despliegue lo pisa
> — y si no lo pisa, es peor: divergiste sin darte cuenta.

La consola crea infraestructura; no debe mantener una segunda versión de los archivos del
repositorio. Usa los artefactos canónicos de
[`02-produccion-aws-terraform.md`](../02-produccion-aws-terraform.md):

| Artefacto | Sección canónica |
|---|---|
| `docker-compose.prod.yml` | guía 02 §5.5 (mínimo) y §14 (definitivo) |
| `scripts/prune-airflow-logs.sh` | repositorio; defensa local del Compose definitivo de guía 02 §14.1 |
| `Dockerfile.airflow.prod` | guía 02 §5.5 |
| `lambdas/startstop.py` | guía 02 §5.4 |
| `lambdas/trigger_airflow.py` | guía 02 §7.1 |
| `scripts/load-secrets.sh` | guía 02 §13.4 |
| Jobs Spark para EMR Serverless | guía 02 §6.4 |
| DAG EMR Serverless | guía 02 §9.4 |
| Deploy de desarrollo | guía 02 §10.1 |
| Workflows CI/CD | guía 02 §11 |
| Prometheus y alertas | guía 02 §12 |
| Retención de logs S3/CloudWatch/Loki y logging remoto de Airflow | guía 02 §6.1, §12 y §14.1 |
| Runbook de producción | guía 02 §15 |

Los nombres de archivo difieren en un solo punto: la guía 02 guarda las Lambdas bajo
`infra/lambdas/` porque allá las empaqueta Terraform, y acá van en `lambdas/` porque las pegás
en la consola. El **contenido es el mismo**; no mantengas dos versiones.

Este documento explica **dónde hacer clic** para crear AWS. La guía 02 explica **qué ejecutar**
para operar la plataforma.

### 11.1 Subir el repositorio

**Dónde:** terminal local.

```bash
export PROD_ENV_SOURCE=discover
source ./scripts/prod-env.sh

rsync -az \
  --exclude .git \
  --exclude .env \
  --exclude infra \
  -e "$RSYNC_SSH" \
  ./ "$SSH_TARGET:$REMOTE_DIR/"
```

Después, **en la EC2** (el `rsync` de arriba ya subió `scripts/`, `dags/` y el Compose):

```bash
cd /home/ec2-user/pyspark_stack
chmod +x scripts/load-secrets.sh                            # si lo creaste sin el bit ejecutable
./scripts/load-secrets.sh                                   # genera .env desde SSM (§7)
docker compose -f docker-compose.prod.yml config --quiet     # falla acá si falta una variable
docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml ps                 # todos 'running', airflow-init 'exited (0)'
```

### 11.2 Publicar entrypoints EMR

```bash
PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh
aws s3 sync spark-apps/emr/ "$EMR_ENTRYPOINTS_URI/" --delete
```

EMR Serverless lee esos archivos directamente desde S3; no los toma de la EC2.

---

## 12. DAGs de producción

> **En esta sección: ESCRIBIR, ~20 min.**
> **Salís con**: el contrato que cumple todo DAG productivo de este stack.

La idempotencia no es opcional acá: los disparadores de §6 pueden ejecutar el mismo DAG
dos veces, y S3 puede entregar el mismo evento más de una vez.

El DAG productivo debe:

- recibir `bucket` y `key` mediante `dag_run.conf`;
- usar `EmrServerlessStartJobOperator(deferrable=True)`;
- definir reintentos, timeout y `max_active_runs`;
- escribir resultados idempotentes;
- solicitar el apagado seguro al terminar;
- registrar el EMR job ID.

No agregues un sensor separado si el operador ya espera en modo deferrable. El triggerer realiza la
espera sin ocupar un worker.

Variables requeridas por Airflow:

```text
AIRFLOW_VAR_EMR_APP_ID
AIRFLOW_VAR_EMR_JOB_ROLE_ARN
AIRFLOW_VAR_DATALAKE
AIRFLOW_VAR_ARTIFACTS
```

El script `load-secrets.sh` de la guía 02 v3 genera esos valores en `.env`.

---

## 13. Operación diaria

> **En esta sección: EJECUTAR todos los días.**
> **Salís con**: el contexto cargado en modo `discover` y los comandos de rutina.

Es el punto de entrada después de cualquier cambio. Si algo no responde, el orden de
diagnóstico es siempre el mismo: **AWS → EC2/SSM → Docker → Airflow → EMR → datos**.
Detené en la primera capa que falle.

### 13.1 Preparar contexto

Es el mismo paso que en la guía 02 §8.1, con el modo `discover` en vez del state:

```bash
export PROD_ENV_SOURCE=discover
source ./scripts/prod-env.sh
./scripts/prod-env.sh --check
```

Eso deja `$INSTANCE_ID`, `$PUBLIC_IP`, `$ACCOUNT_ID`, `$DATALAKE_BUCKET`, `$ARTIFACTS_BUCKET`,
`$EMR_APP_ID`, `$SQS_TRIGGER_QUEUE_URL`, `$LAMBDA_STARTSTOP_NAME`, `$LAMBDA_TRIGGER_NAME`,
`$SSH_TARGET` y las rutas S3 derivadas. **De acá en adelante todos los comandos asumen esto
cargado**; si abrís una terminal nueva, sourcealo otra vez.

### 13.2 Encender, disparar y apagar

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

aws lambda invoke \
  --function-name "${NAME_PREFIX}-startstop" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"action":"stop"}' \
  /dev/stdout
```

`stop` respeta la guardia de DAGs activos. `force=true` se reserva para incidentes después de
revisar Airflow y EMR.

### 13.3 Revisar EMR

```bash
# $EMR_APP_ID ya lo resolvió el modo discover al sourcear prod-env.sh: no hace falta
# repetir el list-applications acá ni en los bloques de abajo.
aws emr-serverless list-job-runs \
  --application-id "$EMR_APP_ID" \
  --max-results 10 \
  --query 'jobRuns[].{id:id,name:name,state:state,created:createdAt}' \
  --output table
```

Para un fallo:

```bash
# El último job corrido; para uno puntual reemplazá por JOB_ID="<job-id>".
JOB_ID="$(aws emr-serverless list-job-runs --application-id "$EMR_APP_ID" \
  --query 'sort_by(jobRuns, &createdAt)[-1].id' --output text)"

aws emr-serverless get-job-run \
  --application-id "$EMR_APP_ID" \
  --job-run-id "$JOB_ID" \
  --query 'jobRun.{state:state,detail:stateDetails}'
```

---

## 14. Smoke tests

> **En esta sección: EJECUTAR después de cada cambio, ~10 min.**
> **Salís con**: la prueba de que la plataforma quedó operativa, no solo creada.

Se ejecutan **de abajo hacia arriba** y se detiene en el primer fallo, a propósito: un
DAG que no corre no se puede diagnosticar si SSM está offline. Cada test asume el
anterior verde.

Ejecutá de abajo hacia arriba y detené en el primer fallo.

### 14.1 AWS y SSM

```bash
aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].{state:State.Name,imdsv2:MetadataOptions.HttpTokens}'

aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
  --query 'InstanceInformationList[0].PingStatus' \
  --output text
```

Esperado: EC2 `running`, IMDSv2 `required`, SSM `Online`.

### 14.2 Host y Docker

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
  --instance-id "$INSTANCE_ID"
```

### 14.3 Evento S3

```bash
KEY="diagnostics/$(date -u +%Y%m%dT%H%M%SZ)/iam.txt"
printf 'ready\n' | aws s3 cp - "s3://${DATALAKE_BUCKET}/${KEY}"
```

S3 entrega eventos al menos una vez. La Lambda debe usar un run ID determinístico y el job debe
ser idempotente.

### 14.4 Acceso a las UIs

```bash
$SSH \
  -L 8082:127.0.0.1:8082 \
  -L 3000:127.0.0.1:3000 \
  -L 9090:127.0.0.1:9090 \
  -L 9093:127.0.0.1:9093 \
  -L 3100:127.0.0.1:3100 \
  "$SSH_TARGET"
```

Las UIs no se publican directamente en el security group.

---

## 15. Seguridad y costos

> **En esta sección: VERIFICAR, ~20 min.**
> **Salís con**: la lista de lo que hay que tener cerrado antes de considerar esto
> algo más que un laboratorio.

> [!WARNING]
> **Por consola, este control es más frágil que en la guía 02.** No hay `terraform
> plan` que te muestre en un diff que alguien abrió un puerto o aflojó una policy.
> Re-verificá esta sección periódicamente, no una sola vez.

### 15.1 Checklist de seguridad

- [ ] Security group: 22 y, si aplica, 443 solo desde tu `/32`.
- [ ] IMDSv2 obligatorio.
- [ ] EBS cifrado y `/data` montado por el ID exacto del volumen.
- [ ] Buckets privados, cifrados, versionados y solo TLS.
- [ ] Roles separados para EC2, EMR job, Lambda y GitHub.
- [ ] EMR job role con S3, logs y Glue limitados a la database/tablas necesarias.
- [ ] `.env` regenerado desde SSM y con permisos `600`.
- [ ] UIs enlazadas a loopback.
- [ ] Access Analyzer sin hallazgos externos inesperados.

### 15.2 Palancas de costo

1. Idempotencia para no ejecutar EMR dos veces.
2. Sin capacidad preinicializada en EMR salvo requisito de latencia.
3. Capacidad máxima y timeout por job.
4. Apagado seguro de EC2 al terminar el último DAG.
5. Retención de logs, snapshots y objetos S3.
6. Budget y Cost Anomaly Detection.

No copies un precio mensual fijo. Usa AWS Pricing Calculator y confirma la factura con Cost
Explorer. Incluye EC2, EBS, snapshots, IPv4 pública, S3, EMR Serverless, CloudWatch y Athena.

---

## 16. DLQ, alertas y gobierno

> **En esta sección: CREAR, ~25 min.**
> **Salís con**: que nada falle en silencio — cada camino de eventos con su DLQ, y un
> presupuesto que avise.

Una DLQ sin alarma es un buzón que nadie abre. El objetivo de esta sección no es tener
la cola: es enterarte.

### 16.1 DLQ correcta para cada camino

| Origen | Configuración |
|---|---|
| S3 → SQS → Lambda | redrive policy de la cola SQS |
| EventBridge Scheduler → Lambda | retry policy y Scheduler DLQ |
| Lambda invocada de forma asíncrona | async destination o Lambda DLQ |

No reutilices una única explicación para los tres casos.

### 16.2 Consola

1. **SQS → Create queue**: crea colas estándar separadas para eventos fallidos y Scheduler.
2. En la cola primaria `trigger-events`, configura redrive tras cinco recepciones.
3. **EventBridge Scheduler → Schedule → Settings**: configura cinco reintentos, una hora de edad
   máxima y la DLQ del Scheduler.
4. **CloudWatch → Alarms**: alarma si una DLQ tiene mensajes visibles.
5. **SNS → Topics**: conecta las alarmas a un email y confirma la suscripción.
6. **Billing → Budgets**: 80% real y 100% proyectado.
7. **Cost Anomaly Detection**: monitor por servicio.
8. **IAM → Access Analyzer**: analizador de acceso externo en cada región usada.

---

## 17. Backup, recuperación y eliminación

> **En esta sección: CONFIGURAR el backup; CONSULTAR el resto.**
> **Salís con**: snapshots automáticos y —lo que casi nadie hace— el procedimiento de
> eliminación en orden inverso.

> [!WARNING]
> **§17.3 es la contracara de no tener Terraform: no hay `destroy`.** Eliminar hay que
> hacerlo a mano, recurso por recurso, en orden inverso al de creación. Saltarse un
> paso deja cargos facturando en una cuenta que creés vacía —típicamente la EIP, los
> snapshots y el bucket con versiones—. Seguí la checklist entera y después verificá
> en Billing, no en la consola de cada servicio.
>
> **Un backup no probado no es un backup.** Restaurá una vez antes de necesitarlo.

### 17.1 Backup

- DLM crea snapshots de `/data`.
- S3 versioning protege objetos.
- Git conserva DAGs, scripts y configuración.
- SSM conserva secretos.

Un backup no está completo hasta probar una restauración.

### 17.2 Prueba de recuperación

1. Creá un volumen desde un snapshot.
2. Adjunta el volumen a una EC2 de recuperación.
3. Identifícalo por su volume ID y móntalo.
4. Regenera `.env` desde SSM.
5. Sincroniza el repositorio y DAGs.
6. Levanta Postgres y Airflow.
7. Ejecutá un DAG controlado.
8. Registrá el tiempo de recuperación.

### 17.3 Eliminación

El orden seguro es:

```text
jobs EMR → schedules/event sources → Lambdas/SQS → EC2/EBS/EIP
→ buckets de aplicación → roles → backend de estado, si existiera
```

Antes de borrar:

- descarga evidencia y logs;
- confirma snapshots;
- elimina versiones y delete markers de S3 únicamente con aprobación;
- libera la Elastic IP;
- revisa Cost Explorer al día siguiente.

---

## Referencias oficiales

- [EMR Serverless desde la consola](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/gs-console.html)
- [Comportamiento y capacidad de EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html)
- [Notificaciones S3 desde la consola](https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-event-notifications.html)
- [SQS como origen de Lambda](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html)
- [Scheduler, reintentos y DLQ](https://docs.aws.amazon.com/scheduler/latest/UserGuide/configuring-schedule-dlq.html)
