data "aws_vpc" "default" {
  default = true
}
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  # Sin este filtro, ids[0] es "la que devolvió la API primero" y puede cambiar entre applies.
  filter {
    name   = "availability-zone"
    values = [var.availability_zone]
  }
}

resource "aws_security_group" "pyspark" {
  name        = "${var.name_prefix}-sg"
  # OJO: AWS solo acepta a-zA-Z0-9 y . _-:/()#,@[]+=&;{}!$* en las descripciones de SG.
  # Nada de comillas simples ni acentos: fallan con InvalidParameterValue al crear el grupo.
  description = "SSH desde mi IP. Web de Airflow (443) desde mi IP si airflow_domain no esta vacio. Resto por tunel."
  vpc_id      = data.aws_vpc.default.id
  ingress {
    description = "SSH desde mi IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }
  # HTTPS de Airflow SOLO si se configuró airflow_domain (§5.6), y SOLO desde tu IP.
  # Vacío el dominio => 0 reglas 443 => nada expuesto (comportamiento original).
  dynamic "ingress" {
    for_each = var.airflow_domain == "" ? [] : [1]
    content {
      description = "HTTPS web de Airflow desde mi IP"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = [var.my_ip_cidr]
    }
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}