#!/bin/bash

EC2_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=airflow-dev" "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

echo "🔌 Connessione a $EC2_IP"
ssh -i security/airflow-dev-key.pem ec2-user@$EC2_IP