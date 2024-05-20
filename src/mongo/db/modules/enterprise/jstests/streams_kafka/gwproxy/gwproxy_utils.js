import {getPython3Binary} from "jstests/libs/python.js";

export class LocalGWProxyServer {
    constructor() {
        this.python_file = "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/gwproxy.py";
    }

    start(partitionCount) {
        // Stop any previously running containers to ensure we are running from a clean state.
        this.stop();
        // Start the gwproxy containers.
        let ret = runMongoProgram(getPython3Binary(), "-u", this.python_file, "start");
        assert.eq(ret, 0, "Could not start GWProxy container.");
    }

    stop() {
        let ret = runMongoProgram(getPython3Binary(), "-u", this.python_file, "stop");
        assert.eq(ret, 0, "Could not stop GWProxy containers.");
    }
}
