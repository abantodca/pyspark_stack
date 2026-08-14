# 07 — Secuencia de ejecución: dependencias y lectura DevOps

> **En este documento: ENTENDER, ~20 min.** No es un runbook: no ejecutes nada desde acá.
> **Salís con**: el mapa de por qué cada comando de la guía 02 va donde va, cuáles dejan de
> hacer falta cuando avanzás, y en qué se diferencia esta secuencia de lo que haría alguien
> que ya opera esto en producción.

La guía 02 se arma **como un rompecabezas**: cada sección escribe archivos `.tf`, aplica, y deja
outputs que la sección siguiente consume como variables de entorno. Eso hace que el orden no sea
una convención sino una dependencia real — y que algunos comandos existan solo por lo que pasó
antes, mientras otros dejan de tener sentido apenas avanzás una sección.

> Desde que la guía es **task-first** ([guía 02 §3.0b](../02-produccion-aws-terraform.md#30b-el-orquestador-de-comandos-taskfileyml)),
> lo que ejecutás es una task y el `terraform` equivalente vive en el desplegable «Qué corre por
> dentro» de cada bloque. Este documento cuenta **tasks**; los comandos crudos que aparecen abajo
> son los de esos desplegables.

Este documento separa tres cosas que en una lectura lineal se confunden:

1. **La espina dorsal**: qué se ejecuta de verdad, sección por sección.
2. **Las dependencias**: qué comando existe *porque* otro ya corrió.
3. **La lectura DevOps**: qué haría distinto alguien que ya tiene esto en producción.

---

## 1. La espina dorsal

Toda la guía 02 se reduce a un ciclo que se repite. Todo lo demás es verificación:

```text
escribir el módulo  →  task infra:validate MODULE=X  →  componer el module "X"  →
   task infra:init  →  task infra:apply MODULE=X  →  source prod-env.sh  →  verificar con el CLI
     ▲                                                                            │
     └──────────────────── la sección siguiente ──────────────────────────────────┘
```

El conteo real sobre los archivos de la guía:

| Comando | Veces | Dónde |
|---|---|---|
| `task infra:apply` | 25, de las cuales **19 con `MODULE=`** | guía 02 §5–§7, §11, §13, §16, §18 |
| `task infra:validate` | 14 | una por módulo, sin backend ni credenciales |
| `task infra:init` | 14 | tras componer un módulo nuevo (baja el `source`) |
| `source ./scripts/prod-env.sh` | 39 | es la junta entre un `apply` y los comandos que lo siguen |
| `aws ... describe/get/list` | mayoría de los bloques | son **gates**, no pasos: confirman lo que el `apply` dice haber creado |

**Que `init` aparezca catorce veces y no una es consecuencia de los módulos, no ruido.** Con un
módulo raíz plano, `init` solo hace falta cuando cambia el backend o un provider; con composición,
cada `module "X"` nuevo agrega un `source` local que Terraform tiene que instalar antes del primer
plan.

**El `MODULE=` es andamio de construcción**, igual que el `-target` que envuelve. Aparece 19 veces
porque el stack se levanta módulo por módulo, y desaparece en el runbook (guía 02 §15) y en el
control de cambios (guía 02 §21.2), donde todo `plan`/`apply` es completo.

### El orden de los `apply`, y qué desbloquea cada uno

| Orden | Sección | Qué crea | Qué desbloquea |
|---|---|---|---|
| 1 | guía 02 §4 | Bucket del state (`infra/bootstrap`, state local) | Que `infra/envs/prod` tenga dónde guardar su state |
| 2 | guía 02 §5.1 | `module.network` — SG, subnet, AZ | `$SECURITY_GROUP_ID`; el orquestador tiene dónde nacer |
| 3 | guía 02 §5.2–§5.3 | `module.orchestrator` — IAM, EC2, EBS, EIP | `$INSTANCE_ID`, `$PUBLIC_IP`, `$SSH_TARGET` → todo el resto |
| 4 | guía 02 §5.4 | `module.scheduler` — Lambda startstop + schedules | `$LAMBDA_STARTSTOP_NAME` |
| 5 | guía 02 §6.1–§6.3 | `module.storage` + `module.backups` | `$DATALAKE_BUCKET`, `$RAW_URI`, `$CURATED_URI` |
| 6 | guía 02 §6.4 | `module.emr` — app + rol de ejecución + Glue DB | `$EMR_APP_ID`, `$EMR_ENTRYPOINTS_URI` |
| 7 | guía 02 §7 | `module.triggers` — Lambda trigger + Scheduler + SQS | `$LAMBDA_TRIGGER_NAME`, `$SQS_TRIGGER_QUEUE_URL` |
| 8 | guía 02 §13 | `module.secrets` — parámetros SSM + permiso de lectura | El `.env` de la EC2 deja de tener defaults débiles |

Un `apply` fuera de orden no falla con un error claro: falla con una **variable vacía**. Por eso el
contrato de guía 02 §3.1 y el validador `check-doc-env.py` existen — es el modo de falla más caro de
toda la guía.

---

## 2. Las dependencias que no se ven

Estos comandos parecen accesorios y no lo son: cada uno está ahí por algo que pasó antes.

| Comando | Existe porque… | Si lo salteás |
|---|---|---|
| `aws ec2 wait instance-status-ok` | La instancia figura `running` mucho antes de terminar el `user_data` | El `rsync` siguiente falla con *connection refused* |
| `PROD_ENV_REFRESH=1 source ...` (guía 02 §5.5) | La caché de 900 s puede ser **anterior** a que la EC2 existiera | `$PUBLIC_IP` vacío y el `rsync` intenta conectar a un host inexistente |
| `ssh-keygen -R` antes del `rsync` | La EIP sobrevive al reemplazo de la instancia, pero la **host key cambia** | *Host key verification failed*, sin pista de la causa |
| `task emr:sync` | EMR Serverless lee los entrypoints **de S3**, no de la EC2 | El job corre con el código viejo, en silencio |
| `docker compose config --quiet` antes del `up` | `load-secrets.sh` puede haber fallado y dejado el `.env` incompleto | Los contenedores arrancan con variables vacías |
| `chmod 600` sobre la caché del contexto | El JSON de outputs queda en `/tmp` con nombres de recursos | Otro usuario del host los lee |

**La dependencia más importante es la menos visible**: `source ./scripts/prod-env.sh` no es un paso
de setup, es lo que convierte cada bloque de la guía en copiable. Un bloque ejecutado en una terminal
nueva sin sourcear no da error de permisos: corre contra nombres vacíos.

---

## 3. Comandos que dejan de hacer falta

Esta es la parte que una lectura lineal no muestra. Son comandos correctos **cuando los ejecutás**,
que una sección posterior vuelve innecesarios o directamente desaconsejables:

| Comando | Vigente en | Lo reemplaza | Por qué deja de servir |
|---|---|---|---|
| `aws ec2 modify-security-group-rules` (Opción B de guía 02 §5.1) | Mientras no exista `terraform.tfvars` | `my_ip_cidr` + `apply` | Modificar el SG por CLI **crea deriva**: el próximo `apply` lo revierte sin avisar |
| `task prod:deploy` sin secretos | Hasta guía 02 §13.4 | la misma task, que desde ahí corre `load-secrets.sh` | Antes de esa sección el stack arranca con los defaults débiles del Paso 0: sirve para probar el host, no para operar |
| `aws emr-serverless start-job-run` a mano (guía 02 §6.4) | Hasta guía 02 §7.1 | El DAG de guía 02 §9.4 | Sirve para validar el rol y el entrypoint. Después, disparar a mano evita el registro en Airflow |
| `task prod:deploy` (rsync del repo) | Hasta guía 02 §11 | El workflow de despliegue con OIDC | El `rsync` desde tu máquina despliega **tu working tree**, no un commit |
| `task infra:apply` (apply pelado) | Todas las secciones de enseñanza | `task release:check` + `release:apply` de guía 02 §15 | Ver abajo |

Sobre el último punto, con precisión: la guía **ya trae el flujo correcto**, pero recién en el
runbook de guía 02 §15 (`fmt -check` → `init` → `validate` → `plan -out=tfplan` → `show` →
`apply tfplan`). Las trece secciones anteriores usan `apply` pelado. Pedagógicamente se entiende —
un `plan` intermedio en cada sección haría la guía el doble de larga—, pero conviene tenerlo claro:
**el `apply` pelado es el modo de aprendizaje, no el de operación.** Un `apply` sin plan revisado
sobre infraestructura que ya tiene datos es la forma habitual de reemplazar una instancia sin querer.

---

## 4. Lectura DevOps: qué haría distinto

Cinco observaciones sobre el diseño de la secuencia, de mayor a menor impacto.

### 4.1 SSM Session Manager en lugar de SSH — el cambio más grande

La guía abre el **puerto 22** a tu IP, administra un **key pair**, mantiene `known_hosts` y expone
las UIs por **túnel SSH**. Al mismo tiempo, la EC2 ya tiene el **agente SSM** y un rol de instancia
con permisos SSM — tanto que las dos Lambdas operan la instancia con `SendCommand`.

Teniendo eso, un operador de producción no abriría el 22:

```bash
# Shell interactiva sin puerto 22 abierto, sin key pair, sin known_hosts.
aws ssm start-session --target "$INSTANCE_ID"

# El túnel a la UI de Airflow, por el mismo canal.
aws ssm start-session --target "$INSTANCE_ID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8082"],"localPortNumber":["8082"]}'
```

Qué se gana: el SG queda **sin ninguna regla de entrada**, desaparecen la clave privada y su
rotación, y cada sesión queda auditada en CloudTrail. Qué se pierde: `rsync` deja de funcionar
directo (necesita el proxy SSM o, mejor, pasar a desplegar por artefactos).

No es un defecto de la guía —el camino SSH es más fácil de entender y de depurar— pero es la
diferencia más clara entre "laboratorio controlado", que es lo que el documento declara ser, y una
exposición productiva.

### 4.2 Construir la imagen en el host

`docker compose up -d --build` **construye en la `t3.large`**. En producción la imagen se construye
una vez en CI, se publica en ECR y el host solo hace `pull`. Ventajas: el build no compite por CPU
con Airflow, el artefacto es idéntico entre entornos, y el despliegue pasa a ser reversible por tag.

La guía ya tiene la mitad hecha: guía 02 §11 valida el build en CI. Lo que falta es publicarlo y que
la EC2 consuma esa imagen en vez de reconstruirla.

### 4.3 El `apply` incremental sí es la decisión correcta

Un `apply` por sección, sobre un único state, con `output` declarados a medida que se crean los
recursos, es exactamente cómo se enseña IaC bien. La alternativa —módulos desde el minuto cero—
esconde la relación entre recurso y comando, que es justo lo que hay que aprender. Cuando el stack
crezca, el corte natural es separar por **ciclo de vida** (red y state que casi no cambian, cómputo
que cambia seguido), no por tipo de recurso.

### 4.4 El contrato de variables está por encima de lo habitual

Que ningún comando lleve un ID escrito a mano, que un validador lo verifique, y que el mismo
contrato funcione con Terraform y sin él (`PROD_ENV_SOURCE=discover`) es más disciplina de la que
suele tener un runbook real. Es la mejor decisión de diseño de la guía y conviene no aflojarla.

### 4.5 Lo que falta para llamarlo producción

No es crítica de la guía —lo declara explícitamente en su encabezado— sino la lista corta de lo que
un operador pediría antes de servir datos reales:

- **La EC2 es punto único de fallo** de Airflow, Postgres y el monitoreo. Un backup del Postgres
  (no solo el snapshot EBS) y una prueba de restauración documentada.
- **Sin alarma sobre las DLQ** no hay forma de enterarse de que un disparo se perdió: guía 02 §18.1
  las crea, y esa sección debería ser obligatoria, no roadmap.
- **El `.env` en el host** se materializa desde SSM en cada deploy. Correcto, pero queda en disco en
  claro; el paso siguiente es que el contenedor lea de SSM al arrancar.

---

## 5. Resumen en una tabla

| Si estás… | Corré | No corras |
|---|---|---|
| Aprendiendo, sección por sección | `task infra:apply MODULE=<mod>` tras leer el `.tf` que escribiste | El runbook completo de guía 02 §15 |
| Aplicando un cambio sobre algo que ya corre | `task release:check` → revisar el plan → `task release:apply` | `task infra:apply` pelado |
| Depurando "el comando no encuentra el recurso" | `PROD_ENV_REFRESH=1 source ./scripts/prod-env.sh` | Escribir el ID a mano para salir del paso |
| Desplegando código de aplicación | El workflow de guía 02 §11 sobre un commit | `task prod:deploy` desde tu working tree |
| Entrando al host | `aws ssm start-session` si lo habilitás | Dejar el 22 abierto "por las dudas" |
