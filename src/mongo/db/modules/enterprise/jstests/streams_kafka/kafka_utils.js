import {getPython3Binary} from "jstests/libs/python.js";

export class LocalKafkaCluster {
    constructor() {
        this.python_file = "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/kafka.py";
    }

    start(partitionCount) {
        // Stop any previously running containers to ensure we are running from a clean state.
        this.stop();
        // Start the kafka containers.
        let ret = runMongoProgram(getPython3Binary(),
                                  "-u",
                                  this.python_file,
                                  "-v",
                                  `--partitions=${[partitionCount]}`,
                                  "start");
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
    getConsumerGroupId(groupId) {
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
        let output = rawMongoProgramOutput().split("\n").find(
            (line) => line.includes(`"group": "${groupId}"`));

        output = output.substring(output.indexOf("["));
        jsTestLog(`Consumer group '${groupId}' state: ${output}`);

        // Return a partition keyed object.
        const obj = JSON.parse(output);
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

        let output = rawMongoProgramOutput().split("\n").find(
            (line) => line.includes(`"group": "${groupId}"`));
        output = output.substring(output.indexOf("["));
        jsTestLog(`Consumer group '${groupId}' members: ${output}`);
        return JSON.parse(output);
    }
}
