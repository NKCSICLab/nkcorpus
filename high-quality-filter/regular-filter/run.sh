#!/bin/bash

for ((i = 1; i <= $1; i++)); do
  screen -dmS langid-"$i" bash -c "source activate;conda activate gpt3; bash loop.sh"
done
