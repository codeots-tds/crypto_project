#!/bin/bash

#downloading datafiles from hadoop to local after being processed by hadoop jobs
hdfs dfs -get /user/ra-terminal/crypto_project/output/* /home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/data/temp_data

#deleting files from hadoop file system after transferring them to local
hdfs dfs -rm -r /user/ra-terminal/crypto_project/output/*