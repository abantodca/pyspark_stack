# ADR-004 — El lock del state lo hace S3 con `use_lockfile`, sin tabla DynamoDB

**Estado:** accepted · 2026-08-12

## Contexto

Un backend S3 compartido necesita bloqueo: dos `apply` simultáneos sobre el mismo state lo corrompen.
Durante años la única forma fue una tabla DynamoDB dedicada, y así lo sigue mostrando la mayoría de
las guías que se encuentran buscando.

Terraform 1.10 agregó `use_lockfile`: el lock se materializa como un objeto `<key>.tflock` en el
mismo bucket, usando *conditional writes* nativos de S3 (`If-None-Match`).

## Decisión

El backend usa `use_lockfile = true` y **no** hay tabla DynamoDB. Como contrapartida, la guía exige
**Terraform ≥ 1.10** y lo verifica en los prerrequisitos: entre 1.6 y 1.9 el parámetro se ignora **en
silencio** y te quedás sin lock creyendo que lo tenés.

## Consecuencias

**Se gana:**

- Un recurso menos que crear, pagar y destruir en el teardown.
- El bootstrap se simplifica: crea un bucket y nada más.

**Se pierde:**

- Piso de versión más alto. Un colaborador con Terraform 1.9 no falla: **corre sin lock**. Por eso el
  chequeo de versión está en §3 y no como nota al pie.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Tabla DynamoDB (`dynamodb_table`) | Está deprecada, agrega un recurso a mantener y a destruir, y cuesta (poco, pero cuesta) |
| Sin lock | Un `apply` concurrente —persona + CI, o dos terminales— corrompe el state. No es hipotético |
| State local | No sobrevive a otra máquina ni al CI, y no hay lock posible |

## Dónde vive

Guía 02 [§3](../02-produccion-aws-terraform.md#3-terraform-y-estado-remoto) (bootstrap, `backend.tf`
y validación de la versión de Terraform).
