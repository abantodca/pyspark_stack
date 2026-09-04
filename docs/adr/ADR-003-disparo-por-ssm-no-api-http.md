# ADR-003 — Disparo por SSM sin exponer la API de Airflow

**Estado:** superseded en su alcance event-driven · 2026-09-04

## Contexto

Hace falta disparar DAGs desde fuera de la EC2: por cron (EventBridge Scheduler) y por evento
(`ObjectCreated` en S3). La forma directa sería llamar a la API REST de Airflow por HTTP, lo que
obliga a exponer un puerto y a gestionar credenciales de API.

El diseño de red del stack es que **la única puerta abierta a Internet** es, opcionalmente, HTTPS
restringido a tu `/32`. Todo lo demás va por túnel SSH.

## Decisión original

Una Lambda `trigger-airflow` ejecuta `airflow dags trigger` **dentro** de la instancia vía
`ssm:SendCommand`. No se abre ningún puerto nuevo, no hay token de API que rotar, y el permiso está
acotado por ARN a esa instancia y al documento `AWS-RunShellScript`.

La misma Lambda arranca la EC2 si el evento la encuentra apagada, lo que hace compatible el disparo
por evento con el apagado automático de [ADR-008](ADR-008-apagado-job-aware.md).

## Alcance vigente

La guía 02 simplificada no materializa `trigger-airflow`, SQS ni DLQ. Mantiene el principio de no
exponer una API pública para automatización: el operador dispara el DAG desde la EC2 mediante el
Taskfile y usa SSH/SSM como canales restringidos. La automatización por eventos queda diferida y
deberá recuperar este ADR —junto con reintentos, DLQ y replay— antes de aceptar datos reales.

## Consecuencias

**Se gana:**

- Superficie de ataque sin cambios: disparar DAGs no agrega ni un puerto.
- La autorización es IAM, no una credencial de aplicación: auditable en CloudTrail y revocable sin
  tocar Airflow.

**Se pierde:**

- **Latencia**: `SendCommand` es asíncrono y hay que poolear `GetCommandInvocation`. No sirve para
  disparos interactivos de baja latencia.
- Depende del **agente SSM**: si está `ConnectionLost`, el disparo falla con un `AccessDenied` que
  parece de permisos y no lo es. Es el primer paso del diagnóstico de guía 02 §8.6.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Exponer la API REST de Airflow | Abre un puerto a Internet y agrega credenciales de API que hay que rotar y auditar aparte |
| API Gateway + Lambda contra la API de Airflow | Misma exposición, con una capa más y sin resolver la credencial |
| Airflow deferrable sensors poleando S3 | Obliga a tener el scheduler encendido 24×7: choca con el apagado automático |

## Dónde vive

El disparo operativo vigente está en la guía 02 [§8.3](../02-produccion-aws-terraform.md#83-prueba-end-to-end)
y su diagnóstico en [§8.6](../02-produccion-aws-terraform.md#86-diagnóstico-rápido). La arquitectura
event-driven original no forma parte del recorrido actual.
