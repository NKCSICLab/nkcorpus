#!/bin/bash

for ((i = 1; i <= $1; i++)); do
  screen -dmS chinese-extraction-"$i" bash -c "source activate;conda activate nkunlp; bash loop.sh"
done
