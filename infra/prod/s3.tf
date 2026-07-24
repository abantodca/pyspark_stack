locals {
  datalake  = "${var.name_prefix}-datalake-${local.account_id}"
  artifacts = "${var.name_prefix}-artifacts-${local.account_id}"   # scripts + logs + deploy/
}

resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake
  # OJO con la semántica: prevent_destroy NO saltea este recurso, ABORTA el `terraform destroy`
  # entero. Para el teardown hay que borrar esta línea primero. Y aun así el destroy falla con
  # BucketNotEmpty: el bucket tiene versionado, así que además de vaciarlo hay que borrar las
  # versiones y los delete markers (o agregar `force_destroy = true`). Usá `scripts/teardown.sh`
  # (§8.5) en vez de editar esto a mano: lo hace y lo revierte solo.
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket" "artifacts" { bucket = local.artifacts }

# for_each necesita keys conocidas en tiempo de plan: un toset de ids fallaría en el PRIMER
# apply con "Invalid for_each argument"; un map con keys estáticas y values computados funciona.
locals {
  buckets = {
    datalake  = aws_s3_bucket.datalake.id
    artifacts = aws_s3_bucket.artifacts.id
  }
}

# Privados + cifrados + solo-TLS + versionado, para ambos buckets.
resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = local.buckets
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = local.buckets
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_versioning" "all" {
  for_each = local.buckets
  bucket   = each.value
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.buckets
  bucket   = each.value
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid = "DenyInsecureTransport", Effect = "Deny", Principal = "*", Action = "s3:*",
      Resource  = ["arn:aws:s3:::${each.value}", "arn:aws:s3:::${each.value}/*"],
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# Lifecycle: transición a clases baratas para bajar el costo de almacenamiento.
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    id     = "tiering"
    status = "Enabled"
    filter {} # aplica a todo el bucket; el provider exige filter o prefix
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

output "datalake_bucket"  { value = aws_s3_bucket.datalake.id }
output "artifacts_bucket" { value = aws_s3_bucket.artifacts.id }