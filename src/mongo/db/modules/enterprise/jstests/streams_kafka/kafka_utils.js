import {getPython3Binary} from "jstests/libs/python.js";

export class LocalKafkaCluster {
    constructor() {
        this.python_file = "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/kafka.py";
        this.partitionCount = 0;
    }

    start(partitionCount, kafkaConfigOverrides) {
        this.partitionCount = partitionCount;
        // Stop any previously running containers to ensure we are running from a clean state.
        this.stop();

        // Build array of kafka command line override arguments
        let overrideCLIArgs = new Array();
        if (kafkaConfigOverrides) {
            for (let [param, val] of Object.entries(kafkaConfigOverrides)) {
                overrideCLIArgs.push("-ka");
                overrideCLIArgs.push(`${param}`);
                overrideCLIArgs.push(`${val}`);
            }
        }

        // Start the kafka containers.
        let ret = runMongoProgram(getPython3Binary(),
                                  "-u",
                                  this.python_file,
                                  "-v",
                                  `--partitions=${[partitionCount]}`,
                                  "start",
                                  ...overrideCLIArgs);
        assert.eq(ret, 0, "Could not start Kafka containers.");
    }

    stop() {
        let ret = runMongoProgram(getPython3Binary(), "-u", this.python_file, "-v", "stop");
        assert.eq(ret, 0, "Could not stop Kafka containers.");
    }

    // Fetches the state of a specific consumer group ID. Returns an object
    // that looks like:
    // [
    //    {
    //        "group": "consumer-group-1",
    //        "topic": "outputTopic1",
    //        "partition": 0,
    //        "current_offset": 1,
    //        "log_end_offset": 1,
    //        "lag": 0,
    //        "consumer_id": "-",
    //        "host": "-",
    //        "client_id": "-"
    //      },
    //      ...
    //  ]
    getConsumerGroupId(groupId, rawOutput = false) {
        jsTestLog(groupId);
        clearRawMongoProgramOutput();
        let ret = runMongoProgram(getPython3Binary(),
                                  "-u",
                                  this.python_file,
                                  "-v",
                                  "get-consumer-group",
                                  `--group-id=${groupId}`);
        if (ret != 0) {
            jsTestLog(`Could not run get-consumer-group command: ${ret}`);
            return null;
        }

        // Find the specific stdout line that we're looking for that has the
        // JSON dump of the consumer group state.
        let output = rawMongoProgramOutput(".*").split("\n").find(
            (line) => line.includes(`"group": "${groupId}"`));
        if (!output) {
            return null;
        }
        output = output.substring(output.indexOf("["));
        jsTestLog(`Consumer group '${groupId}' state: ${output}`);

        const obj = JSON.parse(output);
        if (rawOutput) {
            return obj;
        }
        // Return a partition keyed object.
        return Object.values(obj).reduce((acc, p) => {
            acc[p["partition"]] = p;
            return acc;
        }, {});
    }

    // Returns a list of the active members of the specified consumer group.
    getConsumerGroupMembers(groupId) {
        clearRawMongoProgramOutput();
        let ret = runMongoProgram(getPython3Binary(),
                                  "-u",
                                  this.python_file,
                                  "-v",
                                  "list-consumer-group-members",
                                  `--group-id=${groupId}`);
        if (ret != 0) {
            jsTestLog(`Could not run list-consumer-group-members command: ${ret}`);
            return null;
        }

        let output = rawMongoProgramOutput(".*").split("\n").find(
            (line) => line.includes(`"group": "${groupId}"`));
        if (!output) {
            return null;
        }
        output = output.substring(output.indexOf("["));
        jsTestLog(`Consumer group '${groupId}' members: ${output}`);
        return JSON.parse(output);
    }

    // Returns a list of the compressions used in the specified topic.
    getCompressCodecDetails(topic, partition = 0) {
        clearRawMongoProgramOutput();
        const ret = runMongoProgram(getPython3Binary(),
                                    "-u",
                                    this.python_file,
                                    "-v",
                                    "get-compress-codec-details",
                                    "--topic",
                                    topic,
                                    "--partition",
                                    partition.toString());
        if (ret != 0) {
            jsTestLog(`Could not run get-compress-codec-details command: ${ret}`);
            return null;
        }
        const output = rawMongoProgramOutput(".*").split(/\W+/).filter(
            (word) => ["none", "gzip", "snappy", "lz4", "zstd"].includes(word));
        jsTestLog(`output: ${output}`);
        return output;
    }
}
