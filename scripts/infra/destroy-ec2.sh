#!/bin/bash

echo "💥 Distruggendo infrastruttura Airflow..."

# Trova l'istanza EC2
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=airflow-dev" "Name=instance-state-name,Values=running,pending,stopped" \
  --query 'Reservations[0].Instances[0].InstanceId' \
  --output text 2>/dev/null)

if [ "$INSTANCE_ID" != "None" ] && [ "$INSTANCE_ID" != "" ]; then
    echo "🖥️ Terminando istanza: $INSTANCE_ID"
    aws ec2 terminate-instances --instance-ids $INSTANCE_ID
    
    echo "⏳ Aspettando terminazione..."
    aws ec2 wait instance-terminated --instance-ids $INSTANCE_ID
    echo "✅ Istanza terminata"
else
    echo "❌ Nessuna istanza 'airflow-dev' trovata"
fi

# Elimina Security Group
echo "🛡️ Eliminando Security Group..."
aws ec2 delete-security-group --group-name airflow-dev-sg 2>/dev/null && echo "✅ Security Group eliminato" || echo "❌ Security Group non trovato"

# Elimina Key Pair
echo "🔑 Eliminando Key Pair..."
aws ec2 delete-key-pair --key-name airflow-dev-key 2>/dev/null && echo "✅ Key Pair eliminato" || echo "❌ Key Pair non trovato"

# Elimina chiave locale (opzionale - chiedi conferma)
read -p "🗑️ Eliminare anche la chiave locale security/airflow-dev-key.pem? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -f security/airflow-dev-key.pem
    echo "✅ Chiave locale eliminata"
else
    echo "ℹ️ Chiave locale mantenuta"
fi

echo ""
echo "🎉 Cleanup completato!"