// StreamProcessor and Streams classes are used to make the
// client javascript look like the syntax spec.
// See kafkaExample below for usage instructions.
class StreamProcessor {
    constructor(name, pipeline, connectionRegistry) {
        this._name = name;
        this._pipeline = pipeline;
        this._connectionRegistry = connectionRegistry;
    }

    // Start the streamProcessor.
    start(options) {
        let cmd = {
            streams_startStreamProcessor: '',
            name: this._name,
            pipeline: this._pipeline,
            connections: this._connectionRegistry,
            options: options
        };
        return db.runCommand(cmd);
    }

    // Stop the streamProcessor.
    stop() {
        let cmd = {streams_stopStreamProcessor: '', name: this._name};
        return db.runCommand(cmd);
    }

    // Sample the streamProcessor.
    // Please note, this client-side sample implementation is a hack for
    // demonstration purposes.
    sample(maxLoops = 100, maxEmptyBatches = 20) {
        let cmd = {
            streams_startStreamSample: '',
            name: this._name,
        };
        let result = db.runCommand(cmd);
        assert.commandWorked(result);
        let cursorId = result["id"];

        let consecutiveEmptyBatches = 0;
        let loop = 0;
        for (; loop < maxLoops; loop++) {
            let cmd = {streams_getMoreStreamSample: cursorId, name: this._name};
            result = db.runCommand(cmd);
            assert.commandWorked(result);

            if (result["cursor"]["id"] == 0) {
                print("Sample stopping: returned cursor ID 0 from server");
                break;
            }

            let batch = result["cursor"]["nextBatch"];
            if (batch.length == 0) {
                consecutiveEmptyBatches++;
                if (consecutiveEmptyBatches == maxEmptyBatches) {
                    print("Sample stopping: reached end of streamProcessor");
                    break;
                }
                sleep(500);
            } else {
                consecutiveEmptyBatches = 0;
                for (const doc of batch) {
                    print(tojson(doc));
                }
            }
        }

        if (loop == maxLoops) {
            print("Sample stopping: hit maxLoops");
        }
    }
}

class Streams {
    constructor(connectionRegistry) {
        this._connectionRegistry = connectionRegistry;
    }

    createStreamProcessor(name, pipeline) {
        this[name] = new StreamProcessor(name, pipeline, this._connectionRegistry);
    }
}

let sp = new Streams([]);

function kafkaExample(connectionName = "kafka1", inputTopic = "inputTopic", isTestKafka = false) {
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
