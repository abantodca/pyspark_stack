output "lambda_startstop_name" {
  value = aws_lambda_function.startstop.function_name
}

output "lambda_startstop_arn" {
  value = aws_lambda_function.startstop.arn
}

output "schedule_start_name" {
  value = aws_scheduler_schedule.start.name
}

output "schedule_stop_name" {
  value = aws_scheduler_schedule.stop.name
}
