resource "aws_key_pair" "pyspark" {
  key_name   = "${var.name_prefix}-key"
  public_key = var.ssh_public_key
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "ec2" {
  name               = "${var.name_prefix}-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"   # entrar sin abrir puertos
}
resource "aws_iam_instance_profile" "ec2" {
  name = "${var.name_prefix}-ec2-profile"
  role = aws_iam_role.ec2.name
}

data "aws_iam_policy_document" "ec2_s3" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/*", "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.datalake.arn, aws_s3_bucket.artifacts.arn]
  }
}
resource "aws_iam_role_policy" "ec2_s3" {
  name   = "ec2-s3a"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_s3.json
}

data "aws_iam_policy_document" "ec2_emr" {
  statement {
    sid = "EmrServerlessSubmit"
    actions = [
      "emr-serverless:StartJobRun",
      "emr-serverless:GetJobRun",
      "emr-serverless:StartApplication",
      "emr-serverless:GetApplication",
    ]
    # ARN de la app + sus jobruns (GetJobRun opera sobre el sub-recurso jobruns/*).
    resources = [
      aws_emrserverless_application.spark.arn,
      "${aws_emrserverless_application.spark.arn}/jobruns/*",
    ]
  }
  statement {
    # ListApplications NO tiene resource type en IAM: exige "*", no se puede acotar por ARN.
    # Lo usa scripts/load-secrets.sh (§13.1) para resolver EMR_APP_ID. Sin este statement el
    # script muere con AccessDenied por el `set -euo pipefail` y NO se genera el .env.
    sid       = "EmrServerlessList"
    actions   = ["emr-serverless:ListApplications"]
    resources = ["*"]
  }
  statement {
    sid       = "PassEmrJobRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_job.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }
}
resource "aws_iam_role_policy" "ec2_emr" {
  name   = "ec2-emr-serverless"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_emr.json
}

# Apagado job-aware: la task final `trigger_stop` del DAG (§10.3) invoca la Lambda startstop
# ({"action":"stop"}) al terminar el pipeline. Para eso el rol de la EC2 (bajo el que corre
# Airflow) necesita invocar ESA Lambda — y solo esa (least-privilege).
data "aws_iam_policy_document" "ec2_invoke_startstop" {
  statement {
    sid       = "InvokeStartStopLambda"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.startstop.arn]
  }
}
resource "aws_iam_role_policy" "ec2_invoke_startstop" {
  name   = "ec2-invoke-startstop"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_invoke_startstop.json
}

# Outputs que consumen los DAGs (los cargás como Airflow Variables o env AIRFLOW_VAR_*, §9.0/§14.1):
output "emr_app_id"       { value = aws_emrserverless_application.spark.id }
output "emr_job_role_arn" { value = aws_iam_role.emr_job.arn }