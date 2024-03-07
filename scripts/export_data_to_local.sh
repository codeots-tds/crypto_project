#!/bin/bash

#HDFS data directory
HDFS_DIR="/user/ra-terminal/crypto_project/output"
#local system directory path
LOCAL_DIR="/home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/data/temp_data"
#Copying data output files from hdfs to local directory
hdfs dfs -get "${HDFS_DIR}/*" "${LOCAL_DIR}"
echo "Data copied from HDFS to local directory successfully."

#making the script executeable
#chmod +x export_data_to_local.sh

#executing command in terminal to use script:
#./export_data_to_local.sh

