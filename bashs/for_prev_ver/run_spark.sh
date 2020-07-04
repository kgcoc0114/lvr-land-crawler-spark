#! /bin/sh
PROJ="lvr-land-crawler-spark"

export PYSPARK_PYTHON=$HOME/$PROJ/.venv/bin/python; export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; /usr/local/spark/bin/spark-submit --py-files $HOME/$PROJ/dependency.zip $HOME/$PROJ/prev_ver/land_data_etl.py
