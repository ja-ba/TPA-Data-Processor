#!/bin/bash

#UBUNTU#
#Schedule: 0,0,0,0,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0

###Load files###
#Download repository from git
#Create airflow directories and copy relevant files:
sudo mkdir -p /home/ubuntu/airflow
cd /home/ubuntu/airflow
sudo rm -f /home/ubuntu/airflow/docker-compose.yaml

sudo curl -L -o /home/ubuntu/airflow/docker-compose.yaml https://raw.githubusercontent.com/ja-ba/TAPA-Data-Processor/main/airflow/Airflow_config/docker-compose.yaml
sudo mkdir -p ./dags ./logs ./plugins
sudo rm /home/ubuntu/airflow/dags/airflow_data_processing.py -f
sudo curl -L -o /home/ubuntu/airflow/dags/airflow_data_processing.py https://raw.githubusercontent.com/ja-ba/TAPA-Data-Processor/main/airflow/Airflow_DAGs_Provisioning/airflow_data_processing.py

#Copy bash file to cron daily folder
sudo rm -f /etc/cron.daily/update_files.sh
sudo curl -L -o /etc/cron.daily/update_files.sh https://raw.githubusercontent.com/ja-ba/TAPA-Data-Processor/main/airflow/Airflow_config/update_files.sh
sudo mv /etc/cron.daily/update_files.sh /etc/cron.daily/update_files
sudo chmod +x /etc/cron.daily/update_files

#Install Docker2
sudo apt-get -y remove docker docker-engine docker.io containerd runc
sudo apt-get update  -y
sudo apt-get -y install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
sudo mkdir -m 0755 -p /etc/apt/keyrings
yes | curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get -y update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo apt-get -y remove docker docker-engine docker.io containerd runc
sudo apt-get update  -y
sudo apt-get -y install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
sudo mkdir -m 0755 -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get -y update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin


#Airflow via compose
sudo -i
cd /home/ubuntu/airflow
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo docker compose up airflow-init
sudo docker compose up

#To bring down
#cd /home/ubuntu/airflow
#sudo docker compose down --remove-orphans

#Find postgres SQL database
#cd /var/lib/docker/volumes/airflow_pgdata/_data


#Admin --> Connections:
#Conenction Id: oci_data_processing
#Connection Type: SSH
#Description:...
#Username: opc
#Port: 22
#Extra {"private_key": "..."}

