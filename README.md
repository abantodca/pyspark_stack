# pyspark_stack

Plataforma de datos local y reproducible: Airflow 3.2 + Spark 4.0.3 + HDFS + Jupyter, orquestada con Docker Compose. Incluye guías para llevarla a producción en AWS.

La idea es simple: se desarrolla local (Spark + HDFS, stack completo) y se despliega a EMR Serverless. En producción la arquitectura es híbrida: Airflow sigue como orquestador en una EC2 chica (t3.large) + EMR Serverless para el cómputo Spark, con el data lake en S3 y sin HDFS. El "Arranque rápido" de abajo es siempre el stack local completo para desarrollar.

```
Airflow 3.2 (5 procesos) + Postgres  →  orquestación
Spark 4.0.3 (master + worker)        →  cómputo
HDFS (namenode + datanode)           →  almacenamiento
Jupyter (PySpark 4)                  →  notebooks (solo dev)
```

## Arranque rápido

Requisitos: Docker + Docker Compose. Recomendado 16 GB+ de RAM para el stack completo.

```bash
# 1) Secretos locales (además activa el perfil "dev" → Jupyter, ver .env.example)
cp .env.example .env

# 2) Stack completo
docker compose up -d --build
```

| UI | URL |
|----|-----|
| Airflow | http://localhost:8082 |
| Jupyter | http://localhost:8888 |
| Spark master | http://localhost:8081 |
| HDFS | http://localhost:9870 |

Jupyter es dev-only: vive en el perfil `dev` de Compose y en producción no arranca. Copiar `.env.example` deja `COMPOSE_PROFILES=dev`, así que `docker compose up` lo incluye; sin esa variable, se levanta con `docker compose --profile dev up -d jupyter`.

## Documentación

- [Índice y estado de implementación](docs/README.md): distingue lo implementado, lo pendiente de prueba y el roadmap.
- [docs/01 — Docker Compose explicado](docs/01-docker-compose-explicado-mejorado.md): anatomía del stack local.
- [docs/02 — Producción en AWS](docs/02-produccion-aws-dataops-operativa-v3.md): camino híbrido con Terraform; todavía no validado de extremo a extremo en AWS.
- [docs/02b — Producción por consola](docs/02b-produccion-aws-consola-mejorado.md): alternativa manual de referencia.
- [docs/03 — Arquitectura](docs/03-arquitectura-mejorada.md): arquitectura implementada y evolución planificada.
- [docs/04 — Ejemplos locales](docs/04-ejemplos-local-paso-a-paso-mejorado.md): tutorial progresivo del stack local.
- [docs/05 — Production readiness](docs/05-production-readiness-checklist.md): checklist y evidencias exigidas antes del primer despliegue.
- [ANALISIS_FALLOS.md](ANALISIS_FALLOS.md): registro histórico de fallos del stack y sus fixes.

## Seguridad

- Los secretos locales tienen defaults deliberados. El Compose de producción exige valores explícitos y `scripts/load-secrets.sh` los materializa desde AWS SSM.
- `.env`, estados de Terraform y `alertmanager.yml` están en `.gitignore`. Nunca subir secretos reales.
