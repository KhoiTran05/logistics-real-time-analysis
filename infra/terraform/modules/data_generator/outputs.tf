output "instance_id" {
  value = aws_instance.generator.id
}

output "private_ip" {
  value = aws_instance.generator.private_ip
}
