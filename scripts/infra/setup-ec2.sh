#!/bin/bash

echo "=== Aggiornamento sistema ==="
sudo yum update -y

echo "=== Installazione Docker ==="
sudo yum install docker -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

echo "=== Installazione Docker Compose ==="
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

echo "=== Installazione Git e Python ==="
sudo yum install git python3 python3-pip -y

echo "=== Verifica installazioni ==="
docker --version
docker-compose --version
git --version
python3 --version
aws --version

echo "=== Setup completato! ==="