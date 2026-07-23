data "archive_file" "startstop" {
  type        = "zip"
  source_file = "${path.module}/lambda/startstop.py"
  output_path = "${path.module}/lambda/startstop.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "lambda" {
  name               = "${var.name_prefix}-startstop-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]                       # Describe no admite ARN específico
  }
  statement {
    actions   = ["ec2:StartInstances", "ec2:StopInstances"]
    resources = ["*"]
    # least-privilege: solo instancias con el tag correcto
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/AutoStartStop"
      values   = ["true"]
    }
  }
  # Guardia anti-corte: el handler `stop` consulta los DAG runs activos vía SSM antes de apagar.
  statement {
    actions = ["ssm:SendCommand"]
    resources = [
      "arn:aws:ec2:${local.region}:${local.account_id}:instance/${aws_instance.pyspark.id}",
      "arn:aws:ssm:${local.region}::document/AWS-RunShellScript",
    ]
  }
  statement {
    actions   = ["ssm:GetCommandInvocation"]
    resources = ["*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}
resource "aws_iam_role_policy" "lambda" {
  name   = "startstop-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

# Sin esto, Lambda crea el log group solo en la primera invocación, con retención INFINITA por
# defecto (auditoría §1.2) — a este volumen no pesa en dólares, pero es basura acumulándose para
# siempre por descuido. `depends_on` en la Lambda de abajo es necesario: si Lambda llega primero,
# auto-crea el log group y este `resource` falla con ResourceAlreadyExistsException al aplicar.
resource "aws_cloudwatch_log_group" "startstop" {
  name              = "/aws/lambda/${var.name_prefix}-startstop"
  retention_in_days = 14
}

resource "aws_lambda_function" "startstop" {
  function_name    = "${var.name_prefix}-startstop"
  filename         = data.archive_file.startstop.output_path
  source_code_hash = data.archive_file.startstop.output_base64sha256
  handler          = "startstop.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.lambda.arn
  timeout          = 120   # el guard job-aware espera al SSM SendCommand (chequeo de DAG runs)
  environment {
    variables = { TAG_KEY = "AutoStartStop", TAG_VALUE = "true" }
  }
  depends_on = [aws_cloudwatch_log_group.startstop]
}

# Rol que EventBridge Scheduler asume para invocar la Lambda.
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-startstop-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}
resource "aws_iam_role_policy" "scheduler" {
  name = "invoke-lambda"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow", Action = "lambda:InvokeFunction",
      Resource = aws_lambda_function.startstop.arn
    }]
  })
}

resource "aws_scheduler_schedule" "start" {
  name                         = "${var.name_prefix}-start"
  schedule_expression          = var.start_cron
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.startstop.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ action = "start" })
  }
}
resource "aws_scheduler_schedule" "stop" {
  name                         = "${var.name_prefix}-stop"
  schedule_expression          = var.stop_cron
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.startstop.arn
    role_arn = aws_iam_role.scheduler.arn
    # force: es el cierre DURO del día. Saltea el guard job-aware a propósito (ver startstop.py):
    # sin esto, un DAG colgado dejaba la instancia encendida indefinidamente. El apagado normal
    # —el que sí respeta los jobs en vuelo— lo dispara la task trigger_stop del DAG, sin force.
    input    = jsonencode({ action = "stop", force = true })
  }
}