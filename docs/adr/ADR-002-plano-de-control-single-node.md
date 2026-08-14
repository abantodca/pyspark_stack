# ADR-002 — El plano de control es single-node, sin alta disponibilidad

**Estado:** accepted · 2026-08-12

## Contexto

Airflow, su base Postgres y el monitoreo tienen que correr en algún lado. Las opciones van desde un
servicio administrado (MWAA) hasta un ECS multi-AZ, y todas cuestan más que una EC2 chica.

Este stack es una **plataforma de laboratorio controlado**: el objetivo declarado es aprender y
operar DataOps real con un perfil de costo de ~$35/mes, no sostener un SLA.

## Decisión

**Airflow, Postgres y el monitoreo comparten una sola EC2**, sin réplica ni failover. El estado vive
en un EBS aparte (`/data`) con snapshots diarios por DLM, y el data lake vive en S3 —que sí es
durable y versionado—.

La consecuencia se declara al principio de la guía, en un `[!WARNING]`: **la EC2 es un punto único
de fallo** para Airflow, Postgres y el monitoreo.

## Consecuencias

**Se gana:**

- El costo del plano de control baja a una `t3.large` que además se apaga fuera de horario
  ([ADR-008](ADR-008-apagado-job-aware.md)).
- La operación es entendible de punta a punta: un `docker compose` en un host, no un orquestador
  sobre otro orquestador.

**Se pierde:**

- **No hay HA.** Si la instancia muere, Airflow no corre hasta que la recrees. El RTO real es el que
  tarde `terraform apply` + el deploy (§15), y el RPO es el último snapshot de DLM.
- Un `apply` que reemplace la instancia corta lo que esté corriendo. Por eso el runbook exige
  ventana acordada.

**Guard:** el volumen `/data` tiene `prevent_destroy`, así que un `destroy` accidental aborta el plan
entero en vez de llevarse Postgres.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| MWAA (Airflow administrado) | ~$350/mes de base: diez veces el presupuesto del stack completo |
| Airflow en ECS Fargate multi-AZ | Suma ALB, RDS y service discovery; multiplica el costo y la superficie a entender |
| Dos EC2 con failover | La complejidad de coordinar el scheduler y la base no se paga a esta escala |

## Dónde vive

Guía 02 [guía 02 §1](../02-produccion-aws-terraform.md#1-panorama-de-la-arquitectura),
[guía 02 §5](../02-produccion-aws-terraform.md#5-núcleo-ec2-con-docker) (módulo `orchestrator`) y el
`[!WARNING]` del encabezado. La discusión de alternativas está en
[03 — Arquitectura](../03-arquitectura.md).
