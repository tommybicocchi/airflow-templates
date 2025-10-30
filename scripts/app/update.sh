#!/bin/bash
set -e

EC2_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=airflow-dev" "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

echo "📤 Update DAGs su $EC2_IP"

ssh -i security/airflow-dev-key.pem ec2-user@$EC2_IP << 'EOF'
cd ~/projects/airflow-mwaa-dev
git pull
docker-compose -f docker-compose-local.yml restart scheduler
echo "✅ DAGs aggiornati!"
EOF