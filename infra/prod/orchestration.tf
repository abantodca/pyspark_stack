data "archive_file" "trigger_airflow" {
  type        = "zip"
  source_file = "${path.module}/lambda/trigger_airflow.py"
  output_path = "${path.module}/lambda/trigger_airflow.zip"
}

data "aws_iam_policy_document" "trigger_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "trigger_airflow" {
  name               = "${var.name_prefix}-trigger-airflow"
  assume_role_policy = data.aws_iam_policy_document.trigger_assume.json
}
data "aws_iam_policy_document" "trigger_airflow" {
  statement {   # solo puede mandar el comando a NUESTRA instancia
    actions   = ["ssm:SendCommand"]
    resources = [
      "arn:aws:ec2:${local.region}:${local.account_id}:instance/${aws_instance.pyspark.id}",
      "arn:aws:ssm:${local.region}::document/AWS-RunShellScript",
    ]
  }
  statement {
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ssm:DescribeInstanceInformation"]
    resources = ["*"]   # DescribeInstanceInformation no admite ARN de recurso
  }
  statement {   # DescribeInstances no admite ARN de recurso (a diferencia de StartInstances, abajo)
    sid       = "DescribeEc2"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }
  statement {   # arrancar la EC2 si el evento la encuentra apagada (§7.1) — SOLO nuestra instancia,
    # nunca "*": StartInstances sí admite scoping por ARN a diferencia de Describe. Auditoría §1.1.
    sid       = "StartEc2IfStopped"
    actions   = ["ec2:StartInstances"]
    resources = ["arn:aws:ec2:${local.region}:${local.account_id}:instance/${aws_instance.pyspark.id}"]
  }
  statement {   # el contrato de datos (§7.1) hace un Range GET del objeto que disparó el evento
    sid       = "ContractPeek"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/raw/*"]
  }
  statement {   # consumir la cola SQS primaria (§7.3) — lo exige el event source mapping
    sid       = "ConsumeTriggerQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.trigger_events.arn]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}
resource "aws_iam_role_policy" "trigger_airflow" {
  name   = "trigger-airflow"
  role   = aws_iam_role.trigger_airflow.id
  policy = data.aws_iam_policy_document.trigger_airflow.json
}
# Mismo criterio que §5.4: retención acotada, no infinita (auditoría §1.2).
resource "aws_cloudwatch_log_group" "trigger_airflow" {
  name              = "/aws/lambda/${var.name_prefix}-trigger-airflow"
  retention_in_days = 14
}

resource "aws_lambda_function" "trigger_airflow" {
  function_name    = "${var.name_prefix}-trigger-airflow"
  filename         = data.archive_file.trigger_airflow.output_path
  source_code_hash = data.archive_file.trigger_airflow.output_base64sha256
  handler          = "trigger_airflow.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.trigger_airflow.arn
  timeout          = 60
  # Techo de invocaciones concurrentes (auditoría §3.1 — "thundering herd"): sin esto, subir 50
  # archivos a la vez dispara hasta 50 invocaciones en paralelo, cada una intentando un
  # EmrServerlessStartJobOperator contra un `maximum_capacity` de 16 vCPU (§6.4). Con el límite en 2,
  # SQS deja el resto de los mensajes en cola (no los pierde, no los reintenta antes de tiempo) y se
  # van procesando de a poco. Complementa —no reemplaza— el `max_active_runs=1` del DAG (§10.4).
  reserved_concurrent_executions = 2
  environment {
    variables = {
      INSTANCE_ID = aws_instance.pyspark.id
      DEFAULT_DAG = "customer_etl_emr" # el DAG de producción (EMR Serverless, §10.2) — no el flujo dev local
    }
  }
  # DLQ (dead_letter_config) todavía NO va acá: aws_sqs_queue.trigger_airflow_dlq recién se crea
  # en §18.1, que la engancha agregando este bloque a este mismo resource (sin duplicarlo).
  depends_on = [aws_cloudwatch_log_group.trigger_airflow]
}

data "aws_iam_policy_document" "sched_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "sched_etl" {
  name               = "${var.name_prefix}-etl-scheduler"
  assume_role_policy = data.aws_iam_policy_document.sched_assume.json
}
resource "aws_iam_role_policy" "sched_etl" {
  name = "invoke-trigger"
  role = aws_iam_role.sched_etl.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "lambda:InvokeFunction",
    Resource = aws_lambda_function.trigger_airflow.arn }] })
}
resource "aws_scheduler_schedule" "daily_etl" {
  name = "${var.name_prefix}-daily-etl"
  # 12:00 UTC, L-V: dentro de la ventana de encendido (start 11:00 / stop 22:00 UTC, §5.4).
  # Fuera de la ventana, el SendCommand se perdería en silencio (§7.1).
  schedule_expression          = "cron(0 12 ? * MON-FRI *)"
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.trigger_airflow.arn
    role_arn = aws_iam_role.sched_etl.arn
    input    = jsonencode({ dag = "customer_etl_emr" }) # DAG de producción (§10.2)
  }
}

# Cola primaria: S3 escribe acá, no invoca la Lambda directo (eso es lo que habilita el retry
# transparente de §7.1). visibility_timeout ~6x el timeout de la Lambda (60s) Y suficiente para
# cubrir un boot completo de la EC2 (~2-5 min, §5.5): 360s cumple las dos cosas a la vez.
resource "aws_sqs_queue" "trigger_events" {
  name                       = "${var.name_prefix}-trigger-events"
  visibility_timeout_seconds = 360
  # redrive_policy (hacia aws_sqs_queue.trigger_airflow_dlq) todavía NO va acá: esa cola recién
  # se crea en §18.1, que la engancha agregando este bloque a este mismo resource.
}

# Permite que S3 (y SOLO el bucket datalake) escriba en la cola.
data "aws_iam_policy_document" "trigger_events_queue" {
  statement {
    sid       = "AllowS3Send"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.trigger_events.arn]
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.datalake.arn]
    }
  }
}
resource "aws_sqs_queue_policy" "trigger_events" {
  queue_url = aws_sqs_queue.trigger_events.id
  policy    = data.aws_iam_policy_document.trigger_events_queue.json
}

resource "aws_s3_bucket_notification" "on_upload" {
  bucket = aws_s3_bucket.datalake.id
  queue {
    queue_arn     = aws_sqs_queue.trigger_events.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
  }
  depends_on = [aws_sqs_queue_policy.trigger_events]
}

# La Lambda consume la cola (no hace falta aws_lambda_permission: eso es solo para invocación
# directa por un servicio; acá Lambda hace polling de SQS con los permisos sqs:* de §7.1).
resource "aws_lambda_event_source_mapping" "trigger_events" {
  event_source_arn = aws_sqs_queue.trigger_events.arn
  function_name    = aws_lambda_function.trigger_airflow.arn
  batch_size       = 1   # 1 archivo = 1 invocación: así un archivo lento/rechazado no bloquea a los demás
}