# Checklist de preparación para producción

Este checklist es el gate entre “código preparado” y “despliegue autorizado”. No ejecuta cambios
en AWS. Conserva como evidencia la salida de cada control y registra excepciones con responsable,
fecha de vencimiento y riesgo aceptado.

## 1. Repositorio

- [ ] `python scripts/check-doc-links.py` termina correctamente.
- [ ] CI está verde sobre el commit candidato.
- [ ] El árbol de trabajo está limpio y el commit está etiquetado.
- [ ] No existen `.env`, `*.tfvars`, `*.tfstate`, claves o certificados versionados.
- [ ] Las imágenes y dependencias fueron revisadas y sus actualizaciones son deliberadas.

## 2. Configuración estática

- [ ] `terraform fmt -check -recursive` termina correctamente.
- [ ] `terraform -chdir=infra/prod init -backend=false && terraform -chdir=infra/prod validate`.
- [ ] `docker compose config --quiet` valida el stack local.
- [ ] El Compose productivo valida usando valores de prueba, nunca secretos reales.
- [ ] El override HTTPS solo se usa después de emitir y verificar el certificado.
- [ ] `pytest -q tests/test_dag_integrity.py` no reporta errores de importación.

## 3. Seguridad previa

- [ ] La cuenta usa MFA y no se opera con el usuario root.
- [ ] El backend Terraform existe, tiene versionado, cifrado y bloqueo.
- [ ] `my_ip_cidr` es `/32` y corresponde a la IP autorizada.
- [ ] Los parámetros SSM existen y son `SecureString`.
- [ ] Ningún secreto productivo usa los valores de `.env.example`.
- [ ] Se revisó el plan IAM, especialmente `iam:PassRole`, SSM y acceso S3.
- [ ] IMDSv2, cifrado EBS, bloqueo público S3 y política TLS-only aparecen en el plan.

## 4. Plan Terraform

- [ ] Se guardó `terraform plan -out=...` y se revisó el resumen completo.
- [ ] No hay reemplazo inesperado de EC2, EBS, buckets, roles o reglas de red.
- [ ] Cualquier operación destructiva está justificada y respaldada.
- [ ] Costos, región, AZ, horarios UTC y retención coinciden con el entorno.
- [ ] Otra persona revisó el plan cuando el entorno contiene datos reales.

## 5. Primera validación integrada

- [ ] SSM muestra la instancia como `Online`.
- [ ] `/data` corresponde al EBS esperado y persiste tras stop/start.
- [ ] `scripts/load-secrets.sh` genera `.env` con modo `0600`.
- [ ] Airflow importa `customer_etl_emr` sin errores.
- [ ] Los entrypoints EMR están en el bucket de artifacts.
- [ ] Un job pequeño termina y escribe únicamente en el prefijo esperado.
- [ ] Un segundo intento de la misma entrada no duplica el resultado.
- [ ] Logs de Airflow, Lambda y EMR permiten reconstruir la ejecución.
- [ ] El autoapagado no corta DAGs activos y el cierre forzado respeta el horario acordado.

## 6. HTTPS opcional

- [ ] DNS resuelve a la EIP correcta.
- [ ] Certificado y clave existen bajo `/data/certs` y sus symlinks resuelven.
- [ ] Se configuraron `AIRFLOW_DOMAIN`, `AIRFLOW_BASE_URL`, `AIRFLOW_EXECUTION_API_URL`,
      `AIRFLOW_SSL_CERT` y `AIRFLOW_SSL_KEY`.
- [ ] Se usa `docker-compose.prod.https.yml`.
- [ ] El puerto 443 solo acepta la IP autorizada y 8082 no está expuesto por el security group.

## 7. Criterio de salida

El primer despliegue se considera aceptado únicamente cuando hay evidencia del smoke test, prueba
end-to-end, persistencia tras reinicio, restauración de backup y teardown ensayado en un entorno
sin datos. Iceberg, monitoreo, dbt, calidad y lineage no bloquean la primera versión porque están
marcados como roadmap; deben tener su propio cambio, pruebas y criterio de aceptación.
