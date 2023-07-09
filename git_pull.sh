#!/bin/bash

echo "$(date) - now execute git pull..."

while true
do
    timeout 5s git pull

    if [ $? -eq 0 ]
    then
        echo "$(date) - git pull completed successfully"
        break
    else
        echo "$(date) - git pull took longer than 5 seconds, killing and retrying"
    fi
done
