#!/bin/bash
# This script will authenticate your Helix stream prod admin profile, and provide temporary
# credentials that you can use to run the streams_kafka_gwproxy tests.

aws sso login --profile 10gen-helix-streaming-prod-admin

temp_creds=$(aws configure export-credentials --profile 10gen-helix-streaming-prod-admin)

AWS_ACCESS_KEY_ID=`echo $temp_creds | jq -r .AccessKeyId`
AWS_SECRET_ACCESS_KEY=`echo $temp_creds | jq -r .SecretAccessKey`
AWS_SESSION_TOKEN=`echo $temp_creds | jq -r .SessionToken`

echo "export AWS_ACCESS_KEY_ID=\"${AWS_ACCESS_KEY_ID}\""
echo "export AWS_SECRET_ACCESS_KEY=\"${AWS_SECRET_ACCESS_KEY}\""
echo "export AWS_SESSION_TOKEN=\"${AWS_SESSION_TOKEN}\""
