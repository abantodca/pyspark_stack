resource "aws_emrserverless_application" "spark" {
  name          = "${var.name_prefix}-spark"
  type          = "SPARK"
  release_label = "emr-7.5.0"

  # Arranca sola al recibir un job y se apaga tras 15 min idle → escala a cero, cero mantenimiento.
  auto_start_configuration { enabled = true }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  # Techo de capacidad: acota el gasto máximo aunque un job pida de más.
  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  # network_configuration: NO hace falta para jobs S3-only (EMR sale por el service network de AWS).
  # Solo se agrega si el job accede a recursos DENTRO de tu VPC (RDS privada, ElastiCache, etc.):
  #   network_configuration {
  #     subnet_ids         = data.aws_subnets.default.ids
  #     security_group_ids = [aws_security_group.pyspark.id]
  #   }
}

data "aws_iam_policy_document" "emr_job_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "emr_job" {
  name               = "${var.name_prefix}-emr-serverless-job"
  assume_role_policy = data.aws_iam_policy_document.emr_job_assume.json
}

data "aws_iam_policy_document" "emr_job" {
  statement {
    sid       = "S3ReadWriteData"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/*", "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    sid       = "S3List"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.datalake.arn, aws_s3_bucket.artifacts.arn]
  }
  statement {   # el job escribe sus logs a este log group (cifrado, con retención — abajo)
    sid       = "CloudWatchLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*"]
  }
  # Tablas Iceberg (§16.1): el catálogo es Glue (GlueCatalog de Iceberg), así que el job necesita
  # leer/crear/actualizar la metadata de tabla ahí — sin esto el CREATE TABLE/INSERT del job falla
  # con AccessDenied al intentar registrar el snapshot nuevo en Glue.
  statement {
    sid     = "GlueCatalogIceberg"
    actions = [
      "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.analytics.arn,          # definida en §16.2
      "${aws_glue_catalog_database.analytics.arn}/*",   # Glue expone las tablas como sub-recurso de la DB
    ]
  }
}
resource "aws_iam_role_policy" "emr_job" {
  name   = "emr-serverless-job"
  role   = aws_iam_role.emr_job.id
  policy = data.aws_iam_policy_document.emr_job.json
}

# Logs del job cifrados con retención acotada (CloudWatch Logs cifra en reposo por defecto con
# clave AWS-managed; para KMS propia, agregá kms_key_id).
resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${var.name_prefix}"
  retention_in_days = 30
}

# Base de datos en el Glue Data Catalog: el catálogo lógico donde Iceberg (§16.1) registra las
# tablas de curated/analytics. Va acá (no en §16, que es la sección opcional de Athena) porque lo
# necesita el job Spark para escribir Iceberg, uses o no Athena para consultar después.
resource "aws_glue_catalog_database" "analytics" {
  name = "${replace(var.name_prefix, "-", "_")}_analytics"   # Glue no admite '-' en el nombre
}