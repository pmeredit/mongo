// StreamProcessor and Streams classes are used to make the
// client javascript look like the syntax spec.
// See kafkaExample below for usage instructions.
export class StreamProcessor {
    constructor(name, pipeline, connectionRegistry) {
        this._name = name;
        this._pipeline = pipeline;
        this._connectionRegistry = connectionRegistry;
    }

    // Utilities to make test streams comamnds.
    makeStartCmd(options, processorId, tenantId) {
        return {
            streams_startStreamProcessor: '',
            name: this._name,
            pipeline: this._pipeline,
            connections: this._connectionRegistry,
            options: options,
            processorId: processorId,
            tenantId: tenantId
        };
    }

    // Start the streamProcessor.
    start(options, processorId, tenantId, assertWorked = true) {
        const result = db.runCommand(this.makeStartCmd(options, processorId, tenantId));
        if (assertWorked) {
            assert.commandWorked(result);
        }
        return result;
    }

    makeStopCmd() {
        return {streams_stopStreamProcessor: '', name: this._name};
    }

    // Stop the streamProcessor.
    stop(assertWorked = true) {
        const result = db.runCommand(this.makeStopCmd());
        if (assertWorked) {
            assert.commandWorked(result);
        }
        return result;
    }

    // Utility to sample a stream processor with a custom connection.
    runGetMoreSample(dbConn, maxLoops = 10) {
        let cmd = {
            streams_startStreamSample: '',
            name: this._name,
        };
        let result = dbConn.runCommand(cmd);
        assert.commandWorked(result);
        let cursorId = result["id"];

        for (let loop = 0; loop < maxLoops; loop++) {
            let cmd = {streams_getMoreStreamSample: cursorId, name: this._name};
            result = dbConn.runCommand(cmd);
            assert.commandWorked(result);

            if (result["cursor"]["id"] == 0) {
                print("Sample stopping: returned cursor ID 0 from server");
                break;
            }

            let batch = result["cursor"]["nextBatch"];
            if (batch.length == 0) {
                sleep(1000);
            } else {
                for (const doc of batch) {
                    print(tojson(doc));
                }
            }
        }
    }

    // Sample the streamProcessor.
    sample(maxLoops = 10) {
        this.runGetMoreSample(db, maxLoops);
    }

    // `stats` returns the stats corresponding to this stream processor.
    stats() {
        const res = db.runCommand({streams_getStats: '', name: this._name});
        assert.commandWorked(res);
        assert.eq(res["ok"], 1);
        return res;
    }
}

export class Streams {
    constructor(connectionRegistry) {
        this._connectionRegistry = connectionRegistry;
    }

    createStreamProcessor(name, pipeline) {
        const sp = new StreamProcessor(name, pipeline, this._connectionRegistry);
        this[name] = sp;
        return sp;
    }

    process(pipeline, maxLoops = 3) {
        let name = UUID().toString();
        this[name] = new StreamProcessor(name, pipeline, this._connectionRegistry);
        assert.commandWorked(this[name].start());
        this[name].sample(maxLoops);
        assert.commandWorked(this[name].stop());
    }
}

export let sp = new Streams([]);

export function kafkaExample(
    connectionName = "kafka1", inputTopic = "inputTopic", isTestKafka = false) {
    // First, we create some javascript objects that fake
    // some parts of the client shell and connection registry.
    let connectionRegistry = [{
        name: connectionName,
        type: 'kafka',
        options: {bootstrapServers: 'localhost:9092', isTestKafka: isTestKafka},
    }];
    sp = new Streams(connectionRegistry);

    // Below here, things should look mostly like the syntax spec.

    // Use the streams object to create a streamProcessor named myProcessor.
    sp.createStreamProcessor("myProcessor", [
        {
            $source: {
                connectionName: connectionName,
                topic: inputTopic,
                partitionCount: NumberInt(1),
                // Examples of optional fields below:
                // timeField: { $dateFromString: { dateString: '$myTsStr' }},
                // allowedLateness: { unit: 'second', size: 1 },
                // tsFieldOverride: '_myts',
            }
        },
        {$project: {id: 1, value: 1}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {$sort: {value: 1}},
                    {
                        $group: {
                            _id: "$id",
                            sum: {$sum: "$value"},
                            max: {$max: "$value"},
                            min: {$min: "$value"},
                            count: {$count: {}},
                            first: {$first: "$value"}
                        }
                    },
                    {$sort: {sum: 1}},
                    {$limit: 1}
                ]
            }
        },
        {$match: {_id: NumberInt(0)}},
        {$emit: {connectionName: "__testLog"}}
    ]);

    // Start the streamProcessor.
    let result = sp.myProcessor.start();
    assert.commandWorked(result);

    // Sample the streamProcessor.
    sp.myProcessor.sample();

    // Stop the streamProcessor.
    result = sp.myProcessor.stop();
    assert.commandWorked(result);
}
