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
    start(options, processorId, tenantId) {
        let cmd = {
            streams_startStreamProcessor: '',
            name: this._name,
            pipeline: this._pipeline,
            connections: this._connectionRegistry,
            options: options,
            processorId: processorId,
            tenantId: tenantId
        };
        let result = db.runCommand(cmd);
        assert.commandWorked(result);
        return result;
    }

    // Stop the streamProcessor.
    stop() {
        let cmd = {streams_stopStreamProcessor: '', name: this._name};
        let result = db.runCommand(cmd);
        assert.commandWorked(result);
        return result;
    }

    // Sample the streamProcessor.
    sample(maxLoops = 10) {
        let cmd = {
            streams_startStreamSample: '',
            name: this._name,
        };
        let result = db.runCommand(cmd);
        assert.commandWorked(result);
        let cursorId = result["id"];

        for (let loop = 0; loop < maxLoops; loop++) {
            let cmd = {streams_getMoreStreamSample: cursorId, name: this._name};
            result = db.runCommand(cmd);
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
}

class Streams {
    constructor(connectionRegistry) {
        this._connectionRegistry = connectionRegistry;
    }

    createStreamProcessor(name, pipeline) {
        this[name] = new StreamProcessor(name, pipeline, this._connectionRegistry);
    }

    process(pipeline, maxLoops = 3) {
        let name = UUID().toString();
        this[name] = new StreamProcessor(name, pipeline, this._connectionRegistry);
        assert.commandWorked(this[name].start());
        this[name].sample(maxLoops);
        assert.commandWorked(this[name].stop());
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
