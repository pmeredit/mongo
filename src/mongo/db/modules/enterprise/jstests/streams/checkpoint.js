/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertErrorCode().
load('src/mongo/db/modules/enterprise/jstests/streams/fake_client.js');
load('src/mongo/db/modules/enterprise/jstests/streams/utils.js');

function generateInput(size) {
    let input = [];
    for (let i = 0; i < size; i++) {
        input.push({
            ts: new ISODate(),
            idx: i,
            a: 1,
        });
    }
    return input;
}

function uuidStr() {
    return UUID().toString().split('"')[1];
}

function removeProjections(doc) {
    delete doc._ts;
    delete doc._stream_meta;
}

function smokeTestCorrectness() {
    const input = generateInput(2000);
    const uri = 'mongodb://' + db.getMongo().host;
    const kafkaConnectionName = "kafka1";
    const kafkaBootstrapServers = "localhost:9092";
    const kafkaIsTest = true;
    const kafkaTopic = "topic1";
    const dbConnectionName = "db1";
    const dbName = "test";
    const dlqCollName = uuidStr();
    const outputCollName = uuidStr();
    const processorId = uuidStr();
    const tenantId = uuidStr();
    const outputColl = db.getSiblingDB(dbName)[outputCollName];
    const spName = uuidStr();
    const checkpointCollName = uuidStr();
    const checkpointColl = db.getSiblingDB(dbName)[checkpointCollName];
    const checkpointIntervalMs = NumberInt(0);
    const startOptions = {
        dlq: {connectionName: dbConnectionName, db: dbName, coll: dlqCollName},
        checkpointOptions: {
            storage: {
                uri: uri,
                db: dbName,
                coll: checkpointCollName,
            },
            debugOnlyIntervalMs: checkpointIntervalMs,
        },
    };
    const connectionRegistry = [
        {name: dbConnectionName, type: 'atlas', options: {uri: uri}},
        {
            name: kafkaConnectionName,
            type: 'kafka',
            options: {
                bootstrapServers: kafkaBootstrapServers,
                isTestKafka: kafkaIsTest,
            },
        }
    ];
    sp = new Streams(connectionRegistry);

    // Helper functions.
    let run = () => {
        sp.createStreamProcessor(spName, [
            {
                $source: {
                    connectionName: kafkaConnectionName,
                    topic: kafkaTopic,
                    partitionCount: NumberInt(1),
                    timeField: {
                        $toDate: "$ts",
                    }
                }
            },
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ]);
        sp[spName].start(startOptions, processorId, tenantId);
    };
    let getCheckpointIds = () => {
        return checkpointColl.find({_id: {$regex: "^checkpoint"}}).sort({_id: -1}).toArray();
    };
    let getResults = () => { return outputColl.find({}).sort({idx: 1}).toArray(); };
    let getStartOffsetFromCheckpoint = (checkpointId) => {
        let sourceState =
            checkpointColl
                .find({
                    _id: {$regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`}
                })
                .sort({_id: -1})
                .toArray();
        assert.eq(1, sourceState.length, "expected only 1 state document for $source");
        return sourceState[0]["state"]["partitions"][0]["offset"];
    };

    // Run the streamProcessor for the first time.
    run();
    // Insert the input.
    assert.commandWorked(db.runCommand({
        streams_testOnlyInsert: '',
        name: spName,
        documents: input,
    }));
    // Wait until the last doc in the input appears in the output collection.
    waitForDoc(outputColl, (doc) => doc.idx == input.length - 1, /* maxWaitSeconds */ 60);
    sp[spName].stop();
    // Verify the output matches the input.
    let results = getResults();
    assert.eq(results.length, input.length);
    for (let i = 0; i < results.length; i++) {
        assert.eq(input.length[i], removeProjections(results[i]));
    }
    // Get the checkpoint IDs from the run.
    let ids = getCheckpointIds();
    // Note: this minimum is an implementation detail subject to change.
    // KafkaConsumerOperator has a maximum docs per run, hardcoded to 500.
    // Since the checkpoint interval is 0, we should see at least see a checkpoint
    // after every batch of 500 docs.
    const maxDocsPerRun = 500;
    const expectedMinimumCheckpoints = input.length / maxDocsPerRun;
    assert.gte(ids.length,
               expectedMinimumCheckpoints,
               `expected at least ${expectedMinimumCheckpoints} checkpoints`);

    // Replay from each written checkpoint and verify the results are expected.
    let replaysWithData = 0;
    while (ids.length > 0) {
        const id = ids.shift();
        const startingOffset = getStartOffsetFromCheckpoint(id._id);
        const expectedOutputCount = input.length - startingOffset;

        // Clean the output.
        outputColl.deleteMany({});
        // Run the streamProcessor.
        run();
        // Insert the input.
        assert.commandWorked(db.runCommand({
            streams_testOnlyInsert: '',
            name: spName,
            documents: input,
        }));
        if (expectedOutputCount > 0) {
            // If we're expected to output anything from this checkpoint,
            // wait for the last document in the input.
            waitForDoc(outputColl, (doc) => doc.idx == input.length - 1, /* maxWaitSeconds */ 60);
            replaysWithData++;
        } else {
            // Else, just sleep 5 seconds, we will verify nothing is output after this.
            sleep(1000 * 5);
        }
        sp[spName].stop();
        // Verify the results.
        let results = getResults();
        // ex. if the checkpoint starts at offset 500, we verify the output
        // to contain input[500] to input[length - 1].
        assert.eq(results.length, expectedOutputCount);
        for (let i = 0; i < results.length; i++) {
            assert.eq(input.length[i + startingOffset], removeProjections(results[i]));
        }
        // This deletes the id we just verified, and any new
        // checkpoints this replay created.
        checkpointColl.deleteMany({_id: {$nin: ids.map(id => id._id), $not: /^operator/}});
    }
    assert.gt(replaysWithData, 0);
}

smokeTestCorrectness();
}());