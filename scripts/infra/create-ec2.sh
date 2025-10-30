#!/bin/bash

# Configurazioni
KEY_NAME="airflow-dev-key"
INSTANCE_TYPE="t3.large"
REGION="eu-north-1"
AMI_ID="ami-055e4d03ab1de5def"  # Amazon Linux 2023 EU North 1

echo "🚀 Creando EC2 per Airflow..."

# Crea cartella security se non esiste
mkdir -p security

# Elimina key pair esistente (se esiste)
aws ec2 delete-key-pair --key-name $KEY_NAME 2>/dev/null

# Crea nuovo Key Pair e salva in security/
echo "🔑 Creando chiave SSH..."
aws ec2 create-key-pair \
  --key-name $KEY_NAME \
  --query 'KeyMaterial' \
  --output text > security/${KEY_NAME}.pem

# Imposta permessi corretti
chmod 400 security/${KEY_NAME}.pem
echo "✅ Chiave salvata in: security/${KEY_NAME}.pem"

# Elimina security group esistente (se esiste)
aws ec2 delete-security-group --group-name airflow-dev-sg 2>/dev/null

# Crea Security Group
echo "🛡️ Creando Security Group..."
SG_ID=$(aws ec2 create-security-group \
  --group-name airflow-dev-sg \
  --description "Airflow Dev Security Group" \
  --query 'GroupId' --output text)

# Aggiungi regole SSH e Airflow UI
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp --port 22 --cidr 0.0.0.0/0

aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp --port 8080 --cidr 0.0.0.0/0

echo "✅ Security Group creato: $SG_ID"

# Lancia EC2 (senza user-data, faremo setup manuale)
echo "🖥️ Lanciando istanza EC2..."
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --count 1 \
  --instance-type $INSTANCE_TYPE \
  --key-name $KEY_NAME \
  --security-group-ids $SG_ID \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=airflow-dev}]' \
  --query 'Instances[0].InstanceId' --output text)

echo "✅ EC2 creato: $INSTANCE_ID"
echo "⏳ Aspettando che sia pronto..."

# Aspetta che sia running
aws ec2 wait instance-running --instance-ids $INSTANCE_ID

# Ottieni IP pubblico
PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids $INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)

echo "✅ Istanza online: $PUBLIC_IP"

# Aspetta che SSH sia disponibile
echo "⏳ Aspettando che SSH sia disponibile..."
while ! ssh -i security/${KEY_NAME}.pem -o ConnectTimeout=5 -o StrictHostKeyChecking=no ec2-user@$PUBLIC_IP 'echo "SSH Ready"' 2>/dev/null; do
    echo "   Tentativo SSH..."
    sleep 10
done

echo "✅ SSH disponibile!"

# Trasferisci script di setup
echo "📁 Trasferendo script di setup..."
scp -i security/${KEY_NAME}.pem -o StrictHostKeyChecking=no scripts/setup-ec2.sh ec2-user@$PUBLIC_IP:~/

# Esegui setup automatico
echo "⚙️ Eseguendo setup automatico..."
ssh -i security/${KEY_NAME}.pem -o StrictHostKeyChecking=no ec2-user@$PUBLIC_IP 'chmod +x setup-ec2.sh && ./setup-ec2.sh'

echo ""
echo "🎉 SETUP COMPLETATO!"
echo "📍 Instance ID: $INSTANCE_ID"
echo "🌐 IP Pubblico: $PUBLIC_IP"
echo "🔑 Connessione SSH:"
echo "   ssh -i security/${KEY_NAME}.pem ec2-user@$PUBLIC_IP"
echo ""
echo "🌐 Airflow UI sarà disponibile su: http://$PUBLIC_IP:8080"
echo "🐳 Docker è installato e configurato!"
echo ""
echo "🚀 Pronto per deployare Airflow!"