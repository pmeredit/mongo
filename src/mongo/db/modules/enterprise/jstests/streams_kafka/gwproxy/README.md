# GWProxy (VPC Peering/Private Networking)

This directory contains tests which utilize the GWProxy (VPC Peering) stack against the normal Kafka operator.

As this test requires read-only access to the SRE ECR repository in AWS, it can be run one of two ways:

1. On an evergreen node, which can run this with sts assume role, or

2. On your local evergreen development node, using temporary credentials generated with your
   Helix Streaming Prod credentials. There is a helper script located in:

src/mongo/db/modules/enterprise/jstests/streams_kafka/gwproxy/user_utils

which you can use to log in and generate the appropriate environment variables, and then
execute the tests with:

$ python ./buildscripts/resmoke.py run --suites=streams_kafka_gwproxy --additionalFeatureFlags=featureFlagStreams
