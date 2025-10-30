#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KEY_PATH="$ROOT_DIR/security/airflow-dev-key.pem"

echo "🔍 Cercando EC2..."

EC2_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=airflow-dev" "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

if [ "$EC2_IP" == "None" ] || [ -z "$EC2_IP" ]; then
    echo "❌ EC2 non trovata!"
    exit 1
fi

echo "✅ EC2: $EC2_IP"
echo "🚀 Deploy su EC2..."

ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no ec2-user@$EC2_IP << 'ENDSSH'
set -e

cd ~
mkdir -p projects && cd projects

if [ -d "airflow-mwaa-dev" ]; then
    cd airflow-mwaa-dev
    git pull
else
    git clone git@github.com:tommybicocchi/airflow-mwaa-dev.git
    cd airflow-mwaa-dev
fi

# Configura docker-compose
sed -i 's/127.0.0.1:8080/0.0.0.0:8080/g' docker/docker-compose-local.yml

# Aggiungi requirements
cat > requirements/requirements.txt << 'EOF'
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"

apache-airflow-providers-snowflake==5.8.0
apache-airflow-providers-mysql==5.7.3
apache-airflow-providers-amazon==8.0.0
pandas==1.5.3
EOF

# Verifica se immagine esiste, altrimenti build
if ! docker images | grep -q "amazon/mwaa-local.*2_10_3"; then
    echo "🔨 Building Docker image (prima volta, ~10 min)..."
    ./mwaa-local-env build-image
fi

# Avvia con docker-compose direttamente
echo "🚀 Avvio containers..."
cd docker
docker-compose -f docker-compose-local.yml down 2>/dev/null || true
docker-compose -f docker-compose-local.yml up -d

echo "⏳ Aspettando che Airflow sia pronto..."
sleep 30

docker ps

echo "✅ Deploy completato!"
ENDSSH

echo ""
echo "🎉 FATTO!"
echo "🌐 http://$EC2_IP:8080 (admin/test)"
echo ""
echo "📝 Logs: bash scripts/app/connect.sh"
echo "   Poi: cd ~/projects/airflow-mwaa-dev/docker && docker-compose logs -f"