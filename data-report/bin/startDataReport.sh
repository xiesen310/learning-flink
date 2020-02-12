#!/bin/bash
# 需要在 /etc/profile 中配置 FLINK_HOME
fink run -m yarn-cluster \
-yqu default \
-yn 2 \
-ys 2 \
-yjm 1024 \
-ytm 1024 \
-c top.xiesen.report.DataReport
/data/soft/jars/DataReport/data-report-1.0.jar