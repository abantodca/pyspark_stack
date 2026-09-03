# infra/modules/orchestrator/outputs.tf
output "instance_id" { value = aws_instance.pyspark.id }
output "public_ip" { value = aws_eip.pyspark.public_ip }
output "data_volume_id" { value = aws_ebs_volume.data.id }
output "key_name" { value = aws_key_pair.pyspark.key_name }

# Punto de extensión: cada módulo adjunta aquí su propia policy.
output "instance_role_name" { value = aws_iam_role.ec2.name }
output "instance_role_arn" { value = aws_iam_role.ec2.arn }
