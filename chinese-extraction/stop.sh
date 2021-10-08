#!/bin/bash

for scr in $(screen -ls chinese-extraction- | awk '{print $1}'); do
  screen -S "$scr" -X stuff "^C"
done
