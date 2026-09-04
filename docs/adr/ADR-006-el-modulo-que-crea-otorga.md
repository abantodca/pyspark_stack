# ADR-006 — El módulo que crea el recurso es el que otorga el acceso a él

**Estado:** accepted · 2026-08-12

## Contexto

El rol de instancia de la EC2 necesita permisos sobre cosas que crean **otros** módulos: los buckets
del lake (`storage`), la app de EMR (`emr`), los parámetros de SSM (`secrets`) y la Lambda de
apagado (`scheduler`). Con la composición de [ADR-005](ADR-005-composicion-envs-modules.md) había que
decidir dónde vive cada `aws_iam_role_policy`.

Hay dos ubicaciones posibles: un módulo `iam` central que concentre todas las políticas, o cada
módulo declarando las suyas.

## Decisión

**Cada módulo declara las políticas que dan acceso a los recursos que él crea**, y recibe
`instance_role_name` como variable. `modules/orchestrator/` crea el rol con un único permiso base
(SSM) y lo publica como output; los demás le cuelgan lo suyo.

## Consecuencias

**Se gana:**

- Borrar `module.storage` se lleva su policy: no quedan permisos huérfanos apuntando a buckets que ya
  no existen. Con un módulo IAM central, esa limpieza es manual y se olvida.
- Cada permiso está al lado del recurso que protege: al leer el módulo, se ve qué expone y a quién.

**Se pierde:**

- **No hay un solo archivo donde ver todo lo que puede hacer la EC2.** Para auditar el rol completo
  hay que mirar cuatro módulos —o preguntarle a IAM, que es la fuente real:
  `aws iam list-role-policies --role-name <prefijo>-ec2-role`.
- El nombre de la policy tiene que ser único entre módulos: dos `aws_iam_role_policy` con el mismo
  `name` sobre el mismo rol se pisan sin que Terraform lo note.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Un módulo `iam` central | Recrea el `iam.tf` de 400 líneas que la composición vino a resolver, y acopla ese módulo a todos los demás |
| Políticas gestionadas (`aws_iam_policy` + attachment) desde el entorno | Mueve la decisión de permisos a la capa que solo debería cablear |
| Un rol por servicio en vez de uno de instancia | La EC2 tiene un solo instance profile: habría que multiplexar a mano |

## Dónde vive

Guía 02 [§4.2](../02-produccion-aws-terraform.md#42-iam-y-key-pair) (el rol base y `_shared/`),
[§6.2](../02-produccion-aws-terraform.md#62-iam-acceso-s3-del-orquestador-sin-claves),
[guía 02 §6.4](../02-produccion-aws-terraform.md#64-cómputo-spark-emr-serverless) y
[§10.3](../02-produccion-aws-terraform.md#103-permitir-lectura-desde-ec2).
