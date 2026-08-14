# ADR-008 — El apagado automático es *job-aware*: no corta DAGs en vuelo

**Estado:** accepted · 2026-08-12

## Contexto

La EC2 se apaga fuera de horario para que la factura siga la forma del uso: convierte ~$60/mes fijos
en ~$12 con una ventana de 8 h × 22 días. La forma simple de hacerlo es que EventBridge Scheduler
llame directamente a `ec2:StopInstances` por cron.

El problema es obvio en cuanto se piensa en la operación: si la ventana de stop cae mientras un DAG
está corriendo, el apagado lo corta a la mitad, y Airflow ni siquiera llega a registrar el estado
final de la tarea.

## Decisión

El stop pasa por una **Lambda** que antes de apagar consulta, vía SSM, cuántos DAG runs están en
`running` dentro de la instancia. Si hay alguno —o si **no puede verificarlo**— no apaga y deja el
motivo en el log. `force=true` existe solo para una intervención manual de emergencia.

El sesgo es explícito: ante la duda, **no apagar**. Una EC2 encendida de más cuesta centavos; un DAG
cortado a la mitad cuesta un incidente.

## Consecuencias

**Se gana:**

- El ahorro no tiene como precio la integridad de las corridas.
- Habilita el patrón inverso: la última tarea del DAG (`request_safe_stop`) invoca la misma Lambda al
  terminar, así que la instancia se apaga cuando el trabajo terminó de verdad, no cuando el reloj lo
  dice.

**Se pierde:**

- La Lambda necesita `ssm:SendCommand` sobre la instancia y un timeout de 120 s (espera al comando):
  más permisos y más piezas que un `StopInstances` a secas.
- **Un stop que "no funciona" suele ser la guarda funcionando.** `{"msg": "N DAG run(s) activos, no
  apago"}` es el comportamiento correcto, y es la confusión más común de la sección.
- Si el agente SSM está caído, la Lambda es conservadora y no apaga: se paga la EC2 hasta que
  alguien lo note.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| EventBridge Scheduler → `ec2:StopInstances` directo | Corta DAGs en vuelo. Es la razón de ser de este ADR |
| Apagar solo al terminar el DAG, sin cron | Si un DAG falla antes de la última tarea, la EC2 queda encendida indefinidamente. El cron es la red de seguridad |
| Alarma de CloudWatch por CPU baja | Airflow idle también tiene CPU baja: apagaría entre tareas de un mismo DAG |

## Dónde vive

Guía 02 [guía 02 §5.4](../02-produccion-aws-terraform.md#54-automatización-eventbridge--lambda) (módulo
`scheduler` y el código de la Lambda) y [guía 02 §9.4](../02-produccion-aws-terraform.md#94-dag-de-referencia-para-emr-serverless).
