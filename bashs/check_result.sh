#! /bin/sh
PROJ="lvr-land-crawler-spark"
DIR="$HOME/$PROJ/output"

if [ $(ls -1 $DIR |wc -l) -eq 0 ]; then
    exit 1
else
    exit 0
fi
