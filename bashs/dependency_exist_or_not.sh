#! /bin/sh
PROJ="lvr-land-crawler-spark"


if [-f "$HOME/$PROJ/dependency.zip" ]; then
    cd "$HOME/$PROJ"
    rm -r dependency.zip
    zip dependency.zip tools/*
else
    cd "$HOME/$PROJ"
    zip dependency.zip tools/*
fi

