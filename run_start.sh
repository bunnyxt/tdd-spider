#!/bin/bash

if [[ $# -eq 0 ]]; then
  echo 'Usage: ./run_start.sh [script_name]'
  exit 1
fi

script_name=$1
if [[ ! -f $script_name ]]; then
  echo "Cannot find script ${script_name}"
  exit 1
fi

echo "Now start script ${script_name}..."

# TODO check whether already started this script at least once

nohup python -u ${script_name} >/dev/null 2>&1 &

if [[ $? = "0" ]]; then
  echo 'Success!'
else
  echo 'Fail!'
fi
