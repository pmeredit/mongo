import {getPython3Binary} from "jstests/libs/python.js";

export class LocalKafkaCluster {
    constructor() {
        this.python_file = "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/kafka.py";
    }

    start() {
        // Stop any previously running containers to ensure we are running from a clean state.
        this.stop();
        // Start the kafka containers.
        let ret = runMongoProgram(getPython3Binary(), "-u", this.python_file, "-v", "start");
        assert.eq(ret, 0, "Could not start Kafka containers.");
    }

    stop() {
        let ret = runMongoProgram(getPython3Binary(), "-u", this.python_file, "-v", "stop");
        assert.eq(ret, 0, "Could not stop Kafka containers.");
    }
}
