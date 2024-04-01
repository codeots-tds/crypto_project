#!/bin/bash

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-input /user/ra-terminal/crypto_project/data/line_datafiles \
-output /user/ra-terminal/crypto_project/output \
-mapper /home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/jobs/mapper.py \
-reducer NONE