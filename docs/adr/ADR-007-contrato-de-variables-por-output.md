# ADR-007 — Ningún comando lleva valores escritos a mano: salen de `terraform output`

**Estado:** accepted · 2026-08-12

## Contexto

Una guía de despliegue tiene dos formas de mostrar comandos: con placeholders que el lector
reemplaza (`--instance-id <tu-id>`), o leyendo variables de entorno. La primera es la habitual y
tiene dos modos de falla caros:

- Un ID pegado **caduca**: la instancia se recrea, la IP cambia, el account id es otro en otra
  cuenta. El comando pasa a estar mal y nadie se entera hasta que falla.
- Un `<placeholder>` sin reemplazar es peor: no falla, **ejecuta contra el lugar equivocado** —o abre
  un security group a una IP que no existe y te deja sin SSH—.

## Decisión

Todo valor que decide AWS o Terraform se publica como `output` en la sección que crea el recurso;
`scripts/prod-env.sh` los exporta en MAYÚSCULAS a la shell; los comandos usan la variable. Los
únicos `<entre-ángulos>` que quedan son valores que solo el lector puede elegir (un dominio, un job
id puntual).

La regla en una línea: *si un valor lo decide AWS o Terraform, se publica como `output`; si lo decide
tu máquina (rutas locales), tiene default overridable en el cargador.*

Dos validadores sin credenciales sostienen el invariante en cada cambio de la documentación:
`scripts/check-doc-links.py` y `scripts/check-doc-env.py`.

## Consecuencias

**Se gana:**

- El mismo bloque copiado tal cual funciona en otra cuenta, otra región y con otro `name_prefix`.
- El error típico es ruidoso y temprano: un nombre de recurso vacío por no haber sourceado el
  contexto, en vez de un comando que corre contra el recurso equivocado.
- `check-doc-env.py` detecta el uso de una variable **antes** de la sección que la crea: el modo de
  falla más caro de una guía incremental.

**Se pierde:**

- El lector tiene que correr `source ./scripts/prod-env.sh` en **cada terminal nueva**. Es el paso
  que más se olvida, y por eso está como `[!IMPORTANT]` en el encabezado.
- La EC2 no puede sourcear ese script (no tiene Terraform ni el state), así que los mismos valores
  hay que publicarlos por SSM Parameter Store: dos caminos para el mismo dato.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Placeholders `<entre-ángulos>` | Se ejecutan sin reemplazar, contra el recurso equivocado, sin error |
| Valores reales pegados en los bloques | Caducan y filtran el account id de quien escribió la guía |
| Que la EC2 lea el state de Terraform | Le daría permiso de lectura sobre el bucket del state: exactamente lo que el diseño evita |

## Dónde vive

Guía 02 [guía 02 §3.1](../02-produccion-aws-terraform.md#31-contrato-de-variables-de-entorno-léalo-antes-de-copiar-cualquier-comando),
[guía 02 §13.3b](../02-produccion-aws-terraform.md#133b-cerrar-la-configuración-no-secreta-en-ssm) (el mismo
contrato para la EC2) y `scripts/prod-env.sh` (versionado, no se edita).
