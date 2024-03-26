#!/bin/bash

#Create airflow directories and copy relevant files:
sudo rm -f /home/ubuntu/airflow/docker-compose.yaml
wait
sudo curl -L -o /home/ubuntu/airflow/docker-compose.yaml https://raw.githubusercontent.com/ja-ba/TAPA-Data-Processor/main/airflow/Airflow_config/docker-compose.yaml
wait
sudo rm /home/ubuntu/airflow/dags/airflow_data_processing.py -f
wait
sudo curl -L -o /home/ubuntu/airflow/dags/airflow_data_processing.py https://raw.githubusercontent.com/ja-ba/TAPA-Data-Processor/main/airflow/Airflow_DAGs_Provisioning/airflow_data_processing.py
wait

#Copy bash file
sudo rm -f /etc/cron.daily/update_files.sh
wait
sudo curl -L -o /etc/cron.daily/update_files.sh https://raw.githubusercontent.com/ja-ba/TAPA-Data-Processor/main/airflow/Airflow_config/update_files.sh
wait
sudo mv /etc/cron.daily/update_files.sh /etc/cron.daily/update_files
wait
sudo chmod +x /etc/cron.daily/update_files


sudo curl -L -o https://raw.githubusercontent.com/ja-ba/TAPA-Data-Processor/main/LICENCE
