#!/usr/bin/env bash

date_str=$(date +"%Y%m%d")  # default use today, format: 20220331
if [[ $# -gt 0 ]]; then
  date_str="${1}"  # can manually set date_str
  if [[ ! "${date_str}" =~ ^[0-9]{8}$ ]]; then
    echo "Fail to parse date string ${date_str}! It should be 8 digit number (ex. 20220331). exit"
    exit 1
  fi
fi
log_folder='log'  # log files location
logs_candidate="51_${date_str}??00_*.log"  # globbing format
pack_name="51_log_pack_${date_str}"  # target pack name

echo "Now start ${date_str} daily pack of log 51..."

cd "${log_folder}" || { echo "Fail to enter log folder ${log_folder}! exit"; exit 1; }

mkdir "${pack_name}" || { echo "Fail to create log pack folder ${pack_name}! exit"; exit 1; }
echo "Log pack folder ${pack_name} created."

# shellcheck disable=SC2086
mv ${logs_candidate} "${pack_name}/" || { echo "No log found! exit"; rm -r "${pack_name}"; exit 1; }
echo "Candidate logs moved into log pack folder ${pack_name}."

tar -zcvf "${pack_name}.tar.gz" "${pack_name}"
echo "Log pack ${pack_name}.tar.gz created."

rm -r "${pack_name}"
echo "Log pack folder ${pack_name} removed."

echo "Finish ${date_str} daily pack of log 51!"
