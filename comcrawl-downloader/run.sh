#!/bin/bash

for (( i=1; i<=$1; i++ ))
do
    screen -dmS comcrawl-$i sh -c "conda activate comcrawl; python src/main.py $i;"
done
