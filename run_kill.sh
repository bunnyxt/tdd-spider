#!/bin/bash

if [[ $# -eq 0 ]]; then
  echo 'Usage: ./run_kill.sh [pid]'
  exit 1
fi

pid=$1

kill -9 $pid

if [[ $? = "0" ]]; then
  echo 'Success!'
else
  echo 'Fail!'
fi