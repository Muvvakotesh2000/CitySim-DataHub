# -------------------------------
# CREATE A NEW VPC (Optional, Free)
# -------------------------------
resource "aws_vpc" "citysim_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "citysim-vpc"
  }
}

# -------------------------------
# CREATE PRIVATE SUBNETS (Optional, Free)
# -------------------------------
resource "aws_subnet" "subnet1" {
  vpc_id                  = aws_vpc.citysim_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = false

  tags = {
    Name = "citysim-subnet-1"
  }
}

resource "aws_subnet" "subnet2" {
  vpc_id                  = aws_vpc.citysim_vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = false

  tags = {
    Name = "citysim-subnet-2"
  }
}

resource "aws_subnet" "subnet3" {
  vpc_id                  = aws_vpc.citysim_vpc.id
  cidr_block              = "10.0.3.0/24"
  availability_zone       = "us-east-1c"
  map_public_ip_on_launch = false

  tags = {
    Name = "citysim-subnet-3"
  }
}

# -------------------------------
# CREATE A SECURITY GROUP (Optional, Free)
# -------------------------------
resource "aws_security_group" "citysim_sg" {
  vpc_id = aws_vpc.citysim_vpc.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Open only for testing, restrict later
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "citysim-security-group"
  }
}

# -------------------------------
# CREATE RAW DATA S3 BUCKET (For Raw Kafka Messages)
# -------------------------------
resource "aws_s3_bucket" "citysim_raw_data_bucket" {
  bucket        = var.s3_raw_bucket_name
  force_destroy = true

  tags = {
    Name = "citysim-raw-data-bucket"
  }
}

# -------------------------------
# CREATE PROCESSED DATA S3 BUCKET (For Analytics Results)
# -------------------------------
resource "aws_s3_bucket" "citysim_processed_data_bucket" {
  bucket        = var.s3_processed_bucket_name
  force_destroy = true

  tags = {
    Name = "citysim-processed-data-bucket"
  }
}

# -------------------------------
# CREATE CLOUDWATCH LOG GROUP (Free-Tier up to 5GB)
# -------------------------------
resource "aws_cloudwatch_log_group" "citysim_log_group" {
  name = "/aws/citysim/logs"

  tags = {
    Name = "citysim-log-group"
  }
}
