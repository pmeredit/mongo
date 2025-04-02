#!/bin/sh

# Env variables used to log into the web console at :9001.
export MINIO_ROOT_USER=jstest
export MINIO_ROOT_PASSWORD=jstestpass

# Enable job control (used to background and foreground processes)
set -m 

# Start server in background
minio server /data --console-address ":9001" &

until (mc alias set myminio http://localhost:9000 jstest jstestpass > /dev/null); do echo 'Waiting for MinIO to start...'; sleep 1; done

mc admin user add myminio admin password
mc admin policy attach myminio readwrite --user admin
mc admin accesskey create myminio admin --access-key myAccessKey --secret-key myAccessSecret
mc mb myminio/jstest

# The python interface looks for this log to know when to unblock the jstest.
echo 'MinIO admin setup complete!'

# Trace all incoming requests
mc admin trace myminio

