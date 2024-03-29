#!/bin/bash

#Delete data from HDFS 
LOCAL_DIR="/home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/data/temp_data"

# Navigate to the directory
cd "$LOCAL_DIR"

# Delete all files in the directory
rm -f *

echo "All files in $LOCAL_DIR have been deleted."


#JSON Data
LOCAL_DIR_JSON_DATA="/home/ra-terminal/Desktop/portfolio_projects/crypto_project/app/data/datafiles"

# Navigate to the directory
cd "$LOCAL_DIR_JSON_DATA"

# Delete all files in the directory
rm -f *

echo "All files in $LOCAL_DIR_JSON_DATA have been deleted."

#JSON Line Datafiles
LOCAL_DIR_JSON_LINEDATA="/home/ra-terminal/Desktop/portfolio_projects/crypto_project/app/data/line_datafiles"

# Navigate to the directory
cd "$LOCAL_DIR_JSON_LINEDATA"

# Delete all files in the directory
rm -f *

echo "All files in $LOCAL_DIR_JSON_LINEDATA have been deleted."