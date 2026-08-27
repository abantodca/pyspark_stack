# Documentación de `pyspark_stack`

> **En este documento: ORIENTARSE, ~5 min.** Es el índice y la matriz de estado, no
> un tutorial.
> **Salís con**: sabiendo qué documento abrir, en qué orden, y —lo más importante—
> qué de todo esto **ya funciona** y qué es todavía diseño.

Esta carpeta separa lo que ya funciona de la arquitectura objetivo. Un componente se considera
**implementado** solo cuando existe como código versionado y está cubierto por una validación
repetible. Lo marcado como **roadmap** no forma parte todavía del runbook de producción.

> [!IMPORTANT]
> **La columna «Estado» es la que evita el error caro.** «Guía completa» significa que
> el documento está entero y es coherente, **no** que se desplegó y se validó de punta
> a punta en AWS. Antes de ejecutar una sección, mirá acá. Un bloque marcado *roadmap*
> es una decisión de diseño escrita para el día que la implementes, no un
> procedimiento probado.

### Cómo está ordenada la carpeta

Arriba quedan **solo los seis documentos que se leen de punta a punta**. Lo que se
consulta —el material de referencia— vive un nivel adentro, para que abrir `docs/`
muestre el camino y no el inventario.

```
docs/
├── 01-stack-local.md                  desarrollo local
├── 02-produccion-aws-terraform.md     producción en AWS: la guía completa
├── 03-arquitectura.md                 el porqué
├── 04-dataops-local.md                proyectos medallion y operación local
├── 05-hdfs-desde-la-terminal.md       operar el lakehouse a mano, sin tasks
├── 06-medallion-desde-cero.md         el taller: escribir los 15 proyectos, en orden
├── adr/                               decisiones estructurales, con sus alternativas descartadas
└── README.md                          este índice
```

| Documento | Propósito | Estado |
|---|---|---|
| [01 — Stack local](01-stack-local.md) | Anatomía del Compose y de los contenedores | Implementado |
| [02 — Producción con Terraform](02-produccion-aws-terraform.md) | Arquitectura objetivo y runbook IaC, §1–§22 en un solo documento | Referencia; artefactos no presentes |
| [03 — Arquitectura](03-arquitectura.md) | Vista lógica, seguridad y evolución | Implementado + roadmap |
| [04 — DataOps local](04-dataops-local.md) | Operación de 15 pipelines medallion | Implementado |
| [05 — HDFS desde la terminal](05-hdfs-desde-la-terminal.md) | Ver, buscar, subir, consultar y exportar data del lakehouse con los comandos crudos | Implementado; comandos verificados contra el stack |
| [06 — Medallion desde cero](06-medallion-desde-cero.md) | Taller copy-paste: los 15 proyectos en orden creciente, más la metodología | **Es la fuente del código de `dags/`** |

Las decisiones que no se rediscuten mientras seguís las guías están en
[`adr/`](adr/README.md) — ocho ADR con su contexto, sus consecuencias y lo que se descartó.

### Cómo está organizada la guía 02

Es **un solo documento** con numeración continua de §1 a §22, en el orden en que se
construye la plataforma: cada sección usa lo que dejó la anterior. Se recorre en seis
tramos, y el [índice](02-produccion-aws-terraform.md#índice) enlaza sección por sección.

La infraestructura objetivo se describe como **composición Terraform**, pero este checkout no
incluye `infra/` ni los scripts o Compose de producción. La guía 02 es una referencia de diseño:
no es un runbook ejecutable ni debe promoverse desde este árbol.

| Tramo | Secciones | Qué resuelve |
|---|---|---|
| [1 · Fundamentos](02-produccion-aws-terraform.md#1-panorama-de-la-arquitectura) | §1–§4 | Panorama, costo, prerrequisitos y el **contrato de variables** (guía 02 §3.1) |
| [2 · Núcleo EC2](02-produccion-aws-terraform.md#5-núcleo-ec2-con-docker) | §5 | Red, IAM, EC2+EBS, auto start/stop, deploy y HTTPS |
| [3 · Datos y cómputo](02-produccion-aws-terraform.md#6-data-lake-en-s3) | §6–§7 | Buckets, backups, EMR Serverless y los disparadores |
| [4 · Operación](02-produccion-aws-terraform.md#8-operación-diaria-y-diagnóstico) | §8–§10 | Día a día, diagnóstico, patrones DataOps y despliegue |
| [5 · Entrega y hardening](02-produccion-aws-terraform.md#11-cicd-con-github-actions-y-oidc) | §11–§15 | CI/CD con OIDC, observabilidad, secretos, Compose y runbook |
| [6 · Evolución](02-produccion-aws-terraform.md#16-athena-e-iceberg) | §16–§22 + apéndices | Athena, gobierno, costos y lo marcado *roadmap* |

## Por dónde empezar

No se leen en orden numérico: se leen según qué querés hacer hoy.

```mermaid
flowchart TD
    Q([¿Qué querés hacer?])

    Q --> L{"Aprender / desarrollar<br/>en mi máquina"}
    L --> L1["01 · Stack local<br/><i>cómo está armado el Compose</i>"]
    L1 --> L6["06 · Medallion desde cero<br/><i>escribir los 15 proyectos</i>"]
    L6 --> L2["04 · DataOps local<br/><i>cómo se operan</i>"]
    L2 --> L3["05 · HDFS desde la terminal<br/><i>subir, buscar y sacar TU data</i>"]

    Q --> P{"Desplegar en AWS"}
    P --> P0["03 · Arquitectura<br/><i>el porqué, antes del cómo</i>"]
    P0 --> P1["02 · Producción con Terraform<br/><i>el camino principal</i>"]

    Q --> O{"Operar / diagnosticar<br/>algo que ya corre"}
    O --> O1["02 · Operación y diagnóstico<br/><i>sección 8, con el catálogo de 8.6</i>"]

    Q --> H{"Entender por qué<br/>algo está así"}
    H --> H1["03 · Arquitectura"]
    H --> H2["adr/<br/><i>las ocho decisiones y lo descartado</i>"]

    style P1 fill:#d1ecf1,stroke:#0c5460
    style L1 fill:#d1ecf1,stroke:#0c5460
    style L6 fill:#d4edda,stroke:#155724
```

**El orden que sí importa**: 01 → **06** → 04 → 05 → 03 → 02. La 06 es donde se escribe
el código: `dags/` arranca vacío y esa guía lo entrega completo, en quince pasos.
 El stack local es el
prerrequisito real del de producción —se desarrolla acá y se despliega allá—, y la
guía 02 arranca con un gate explícito que lo verifica. Saltar directo a 02 sin un DAG
que corra en local funciona hasta el primer error, y ahí se paga en minutos de EMR lo
que en Docker cuesta segundos.

**02 y 02b son caminos alternativos para lo mismo, no complementarios.** 02
(Terraform) es el camino principal y la fuente de verdad. 02b (consola) sirve para
*entender* qué crea cada bloque, o para una cuenta donde no podés correr Terraform.
**No los mezcles sobre el mismo recurso**: si algo lo creaste a mano y lo querés en
Terraform, va `terraform import` antes del siguiente `apply`.

## Convenciones comunes a todas las guías

| Convención | Qué significa |
|---|---|
| **Dónde corre el bloque** | Tres contextos, no intercambiables: tu máquina (el default), dentro de la EC2 (`# EN LA EC2`, credenciales del rol de instancia) o GitHub Actions con OIDC. Un bloque de CI ejecutado en local no demuestra nada: prueba tus permisos de admin, no los del rol de OIDC |
| **`task` arriba, desplegable abajo** | Los bloques ejecutables de la guía 02 muestran la task que corrés y, en «Qué corre por dentro», el `terraform`/`aws` equivalente. Las tasks de producción se escriben en [guía 02 §3.0b](02-produccion-aws-terraform.md#30b-el-orquestador-de-comandos-taskfileyml); las locales ya están en el repo. Son las mismas que corre el CI |
| **En esta sección / Salís con** | El encabezado de cada sección dice si toca **leer**, **ejecutar** o **consultar**, y cuánto lleva. Las de «consultar» no se leen de corrido |
| **Mapa del camino** | Prerrequisitos, diagrama de pasos y reglas de la sección, antes del primer comando |
| ✅ **Gate** | El criterio de aceptación de una sección. Si no lo cumplís, no sigas a la siguiente |
| > **Gotcha** | Un fallo real que ya pasó, con su causa. Están donde muerden, no en un anexo |
| *Roadmap* | Diseño, no runbook. No está implementado ni validado |

## Qué contiene el repositorio

El repositorio versiona el proyecto local. Que un bloque figure en la guía de producción no
significa que exista, esté desplegado o haya pasado una prueba integrada en AWS.

| Capacidad | Dónde vive |
|---|---|
| Spark, HDFS, Jupyter y Airflow en local | Repositorio — implementado |
| Los 15 pipelines medallion y su runtime | [Guía 06](06-medallion-desde-cero.md) — el código vive en la guía, no en `dags/` |
| Orquestador de comandos (`Taskfile.yml`) | Repositorio — implementación completa para local |
| Terraform de red, EC2, S3, EMR Serverless y automatización | Arquitectura objetivo — no materializado |
| DAG de Airflow contra EMR Serverless | Guía 02 §9.4 |
| Jobs Spark para EMR Serverless | Guía 02 §6.4 |
| Compose de producción y carga de secretos desde SSM | Guía 02 §13.4 y §14.1 |
| Validación en CI y despliegue con OIDC | Guía 02 §11 |
| Observabilidad Prometheus/Grafana/Loki | Guía 02 §12 y §14.2 — roadmap |
| Tablas Iceberg | Guía 02 §16 — roadmap; el job de referencia escribe Parquet |
| dbt, Great Expectations y OpenLineage | Guía 02 §19–§22 — roadmap |

## Regla de mantenimiento

Los comandos, políticas y configuraciones ejecutables viven en sus archivos canónicos. La
documentación explica decisiones y enlaza esos archivos; no mantiene una segunda copia que pueda
divergir. Cada cambio de arquitectura debe actualizar esta matriz.

Los comandos locales no llevan valores escritos a mano: leen las variables de `.env` y el Compose
versionado. La guía 02 conserva su contrato de variables como referencia de diseño, no como
automatización activa. La documentación se revisa junto con cada cambio de contrato u operación.
