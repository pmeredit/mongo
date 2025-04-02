# ASP S3 JSTests

This directory contains the jstest for testing S3 functionality for the ASP `$emit` operator.

This jstest spins up [MinIO](https://min.io/docs/minio/container/index.html) in a docker container. MinIO is a S3-compatible object bucket service.

## Debugging tests

When a jstest fails unexpectedly you can debug the test using the web console provided by minio. To do so, you can follow these steps:

1. Prevent the jstest from stopping the docker container after the test is finished.
2. Run the jstest again and let it fail.
3. Run this command on your laptop: `ssh -L 9001:localhost:9001 <user>@<workstation IP>`
   - This command forwards your workstation's port 9001 to your laptop.
4. Open `http://localhost:9001` on your browser. You should see a login page for MinIO.
5. Login with the following credentials:
   - Username: `jstest`
   - Password: `jstestpass`
6. Do whatever debugging you need to do.
