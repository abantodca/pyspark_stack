# ADR-005 — La infra es una composición `envs/prod` + `modules/*`, no un módulo raíz plano

**Estado:** accepted · 2026-08-12 · *supersede la estructura plana `infra/prod/*.tf`*

## Contexto

La primera versión de la guía escribía toda la infraestructura como un módulo raíz plano:
`infra/prod/` con unos veinte `.tf` cuyos nombres describían responsabilidades (`network.tf`,
`iam.tf`, `emr.tf`…). Funcionaba, y para un stack chico es una estructura legítima.

El problema aparece con el crecimiento: todo comparte namespace, cualquier `resource` puede
referenciar a cualquier otro sin declararlo, y el acoplamiento deja de ser visible. Un
`aws_iam_role_policy` perdido en un `iam.tf` de 400 líneas no dice qué componente depende de cuál.

## Decisión

`infra/envs/prod/` **compone y no declara ningún `resource`**; los doce módulos de `infra/modules/`
tienen interfaz pública (`variables.tf` de entrada, `outputs.tf` de salida) e implementación privada.
Cada sección de la guía sigue el mismo bucle: pegar el módulo → validarlo aislado (`init -backend=false`
+ `validate`) → componer el `module "X"` → `apply -target`.

## Consecuencias

**Se gana:**

- El acoplamiento se lee en una línea: `instance_role_name = module.orchestrator.instance_role_name`.
- El radio de cambio se achica: se valida y se aplica un módulo por vez, y un error se localiza en
  minutos en vez de en un apply monolítico de quince.
- Clonar un entorno es copiar `envs/prod/` y cambiar `terraform.tfvars`; los módulos no se tocan.

**Se pierde:**

- **Más ceremonia por sección**: tres archivos por módulo en vez de uno, y todo valor que cruza una
  frontera hay que declararlo dos veces (`variable` de un lado, `output` del otro).
- Un `init` más cada vez que se compone un módulo nuevo: Terraform tiene que instalar el `source`
  local.
- `-target` se vuelve parte del flujo de construcción, con la advertencia de que es un andamio y no
  una forma de operar: en producción, `plan`/`apply` completos.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Seguir con el módulo raíz plano | Deja de escalar cuando la infra crece; el diff de un cambio chico obliga a leer el grafo entero |
| Un state por módulo (con `terraform_remote_state`) | Multiplica los backends y hace que un cambio transversal necesite N applies en orden manual |
| Módulos publicados en un registry propio | Versionado y publicación para doce módulos que solo usa este repo |
| Módulos de terceros (`terraform-aws-modules/*`) | Genéricos: traen opciones que no usamos y esconden lo que la guía quiere enseñar |

## Dónde vive

Guía 02 [§1.4.1](../02-produccion-aws-terraform.md#141-estructura-de-infraestructura-composición-y-módulos)
(layout), [§3](../02-produccion-aws-terraform.md#3-terraform-y-estado-remoto) (backend y composición)
y el [mapa de archivos](../02-produccion-aws-terraform.md#apéndice-mapa-de-archivos).
