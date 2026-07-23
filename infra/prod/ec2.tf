data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    # "2023*" excluye las variantes minimal/ecs, que no traen el agente SSM (lo usa toda la §7).
    values = ["al2023-ami-2023*-x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "pyspark" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.pyspark.key_name
  vpc_security_group_ids = [aws_security_group.pyspark.id]
  subnet_id              = data.aws_subnets.default.ids[0]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }
  user_data                   = templatefile("${path.module}/user_data.sh.tftpl", {})
  user_data_replace_on_change = true

  # IMDSv2 obligatorio: un SSRF en Airflow/Grafana no puede robar las credenciales
  # del instance profile. hop_limit = 2: los contenedores llegan al IMDS cruzando el bridge
  # de Docker (+1 hop); con el default (1) el token no llega y s3a con rol IAM falla.
  metadata_options {
    http_tokens                 = "required"
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 2
  }

  # Name = "<prefix>-node" (el workflow de CI busca la instancia por este tag);
  # AutoStartStop = "true" (la Lambda startstop filtra por él).
  tags = {
    Name          = "${var.name_prefix}-node"
    AutoStartStop = "true"
  }
}

# EIP: sin ella, cada stop/start cambiaría la IP pública (túneles SSH, output public_ip).
# Costo: AWS cobra toda IPv4 pública (~$3.6/mes, ver tabla de §2), asociada o no.
resource "aws_eip" "pyspark" {
  domain = "vpc"
  tags   = { Name = "${var.name_prefix}-eip" }
}
resource "aws_eip_association" "pyspark" {
  instance_id   = aws_instance.pyspark.id
  allocation_id = aws_eip.pyspark.id
}

resource "aws_ebs_volume" "data" {
  # De la variable, NO de aws_instance.pyspark.availability_zone: así el volumen no se arrastra
  # detrás de la instancia si esta se recrea, y la AZ es un dato fijo del stack.
  availability_zone = var.availability_zone
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "${var.name_prefix}-data" } # ← el DLM (backups) respalda por este tag
  lifecycle {
    prevent_destroy = true # el disco de estado (Postgres/Prometheus/Loki) NO se borra por accidente
  }
}
resource "aws_volume_attachment" "data" {
  device_name = "/dev/xvdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.pyspark.id
}