// StreamProcessor and Streams classes are used to make the
// client javascript look like the syntax spec.
// See example usage in smoketest.js.
class StreamProcessor {
    constructor(name, pipeline, connections) {
        this._name = name;
        this._pipeline = pipeline;
        this._connections = connections;
    }

    start() {
        let cmd = {
            streams_startStreamProcessor: '',
            name: this._name,
            pipeline: this._pipeline,
            connections: this._connections
        };
        return db.runCommand(cmd);
    }

    stop() {
        let cmd = {streams_stopStreamProcessor: '', name: this._name};
        return db.runCommand(cmd);
    }
}

class Streams {
    constructor(connections) {
        this._connections = connections;
    }

    createStreamProcessor(name, pipeline) {
        this[name] = new StreamProcessor(name, pipeline, this._connections);
    }
}