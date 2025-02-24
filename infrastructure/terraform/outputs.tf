#output "s3_bucket_name" {
#  value = aws_s3_bucket.citysim_data_bucket.bucket
#}

output "cloudwatch_log_group" {
  value = aws_cloudwatch_log_group.citysim_log_group.name
}

output "raw_data_bucket" {
  value = aws_s3_bucket.citysim_raw_data_bucket.bucket
}

output "processed_data_bucket" {
  value = aws_s3_bucket.citysim_processed_data_bucket.bucket
}

# output "kafka_cluster_arn" {
#  value = aws_msk_cluster.kafka_cluster.arn
#}
