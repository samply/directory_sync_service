#!/usr/bin/env bash

set -e

# Replace template slots with relevant environment variables
# Supply reasonable defaults, where possible
file=/etc/bridgehead/directory_sync.conf
sed -i "s,{directory_sync.directory.url},${DIRECTORY_URL:-https://directory.bbmri-eric.eu}," $file
sed -i "s/{directory_sync.directory.user_name}/${DIRECTORY_USER_NAME}/" $file
sed -i "s/{directory_sync.directory.pass_code}/${DIRECTORY_PASS_CODE}/" $file
sed -i "s,{directory_sync.fhir_store_url},${FHIR_STORE_URL:-http://store:8080}," $file
sed -i "s!{directory_sync.timer_cron}!${TIMER_CRON}!" $file
sed -i "s/{directory_sync.retry_max}/${RETRY_MAX:-10}/" $file
sed -i "s/{directory_sync.retry_interval}/${RETRY_INTERVAL:-20}/" $file

# Start the server
exec java -jar directory_sync_service.jar

