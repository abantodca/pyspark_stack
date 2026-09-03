# infra/modules/orchestrator/main.tf
resource "aws_key_pair" "pyspark" {
  key_name   = "${var.name_prefix}-key"
  public_key = var.ssh_public_key
}

resource "aws_iam_role" "ec2" {
  name               = "${var.name_prefix}-ec2-role"
  assume_role_policy = file("${path.module}/../_shared/assume-ec2.json")
}
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" # entrar sin abrir puertos
}
resource "aws_iam_instance_profile" "ec2" {
  name = "${var.name_prefix}-ec2-profile"
  role = aws_iam_role.ec2.name
}

resource "aws_instance" "pyspark" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.pyspark.key_name
  vpc_security_group_ids = [var.security_group_id]
  subnet_id              = var.subnet_id
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    # Identificador exacto del EBS persistente. El script lo usa para resolver el NVMe correcto
    # antes de montar o formatear; no depende del orden nvme1n1/nvme2n1.
    data_volume_id   = aws_ebs_volume.data.id
    parameter_prefix = "/${var.name_prefix}"
  })
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
  # AutoStartStop = "true" (inventario/costos; la Lambda opera por INSTANCE_ID).
  tags = {
    Name          = "${var.name_prefix}-node"
    AutoStartStop = "true"
  }
}

# EIP: sin ella, cada stop/start cambiaría la IP pública (túneles SSH, output public_ip).
# Costo: AWS cobra toda IPv4 pública asociada o no; inclúyala en la línea base de la sección 2.
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