#!/bin/bash

for scr in $(screen -ls langid- | awk '{print $1}'); do
  screen -S "$scr" -X stuff "^C"
done
