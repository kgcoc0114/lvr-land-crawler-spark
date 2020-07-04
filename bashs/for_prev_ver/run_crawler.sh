#! /bin/sh

PROJ="lvr-land-crawler-spark"

cd "$HOME/$PROJ"

if [ "$1" = "hist_season" ]; then
    pipenv run python3 prev_ver/land_data_crawler.py hist_season "$2S$3"

elif [ "$1" = "hist_date" ]; then
    pipenv run python3 prev_ver/land_data_crawler.py hist_date $2

elif [ "$1" = "curr" ]; then
    pipenv run python3 prev_ver/land_data_crawler.py curr

else
    pipenv run python3 prev_ver/land_data_crawler.py hist_season 108S2
fi

