#! /bin/sh
PROJ="lvr-land-crawler-spark"

if [-f "$HOME/$PROJ/dependency.zip" ]; then
    echo "ok!"
else
    cd "$HOME/$PROJ"
    zip dependency.zip tools/*
fi

