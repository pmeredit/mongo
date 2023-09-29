/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

function badDBSourceStartError() {
    const badUri = "mongodb://127.0.0.1:9123";
    const dbConnectionName = "db1";
    const dbName = "test";
    const inputCollName = "testin";
    const spName = "sp1";
    const connectionRegistry = [{
        name: dbConnectionName,
        type: 'atlas',
        options: {
            uri: badUri,
        }
    }];
    // Calls streams_startStreamProcessor with a bad $source.
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: dbConnectionName,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$emit: {connectionName: "__testMemory"}}
        ],
        connections: connectionRegistry,
    });
    assert.commandFailed(result);
    assert.eq(8112613, result.code);
    assert(result.errmsg.startsWith("Error encountered while connecting to change stream $source"));
}

function badKafkaSourceStartError() {
    const spName = "sp2";
    const badUri = 'foohost:9092';
    const connectionName = "kafka1";
    const inputTopicName = "intopic";
    const connectionRegistry = [{
        name: connectionName,
        type: 'kafka',
        options: {bootstrapServers: badUri},
    }];
    // Calls streams_startStreamProcessor with a bad $source.
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {$source: {connectionName: connectionName, topic: inputTopicName}},
            {$emit: {connectionName: "__testMemory"}}
        ],
        connections: connectionRegistry,
    });
    assert.commandFailed(result);
    assert.eq(77175, result.code);
    assert.eq("Could not connect to the Kafka topic with kafka error code: -195.", result.errmsg);
}

// Create a streamProcessor with a bad $merge target. Verify the
// streamProcessor reports a meaningful error in listStreamProcessors.
function badDBMergeAsyncError() {
    // Chose a valid network address and port that does not have a running server on it.
    const goodUri = 'mongodb://' + db.getMongo().host;
    const goodConnection = "dbgood";
    const dbName = "test";
    const inputCollName = "testin";
    const inputColl = db.getSiblingDB(dbName)[inputCollName];
    const badUri = "mongodb://127.0.0.1:9123";
    const badConnection = "dbbad";
    const connectionRegistry = [
        {
            name: badConnection,
            type: 'atlas',
            options: {
                uri: badUri,
            }
        },
        {
            name: goodConnection,
            type: 'atlas',
            options: {
                uri: goodUri,
            }
        },
    ];
    const spName = "sp1";
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: goodConnection,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {
                $merge: {
                    into: {connectionName: badConnection, db: 'test', coll: "outputcoll"},
                }
            }
        ],
        connections: connectionRegistry,
    });
    assert.commandWorked(result);

    inputColl.insert({a: 1});
    assert.soon(() => {
        let result = db.runCommand({streams_listStreamProcessors: ''});
        let sp = result.streamProcessors.find((sp) => sp.name == spName);
        return sp.status == "error" && sp.error.code == 74780 &&
            sp.error.reason.includes("Error encountered in MergeOperator while writing to target");
    });

    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: '', name: spName}));
}

function badMongoDLQAsyncError() {
    const badUri = "mongodb://127.0.0.1:9123";
    const badConnection = "dbbad";
    const connectionRegistry = [
        {
            name: badConnection,
            type: 'atlas',
            options: {
                uri: badUri,
            }
        },
        {
            name: '__testMemory',
            type: 'in_memory',
            options: {},
        },
    ];
    const dlq = {
        connectionName: badConnection,
        db: 'test',
        coll: 'dlq',
    };
    const spName = "sp1";
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {$source: {connectionName: '__testMemory'}},
            {
                $validate: {
                    validator: {$expr: {$eq: ['$value', 1]}},
                    validationAction: 'dlq',
                }
            },
            {$emit: {connectionName: '__testMemory'}},
        ],
        connections: connectionRegistry,
        options: {dlq},
    });
    assert.commandWorked(result);

    assert.commandWorked(
        db.runCommand({streams_testOnlyInsert: '', name: spName, documents: [{value: 2}]}));

    assert.soon(() => {
        const result = db.runCommand({streams_listStreamProcessors: ''});
        const sp = result.streamProcessors.find((sp) => sp.name == spName);
        return sp.status == "error" && sp.error.code == 75382 &&
            sp.error.reason ===
            "Error encountered while writing to the DLQ with db: test, coll: dlq";
    });

    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: '', name: spName}));
}

function checkpointDbConnectionFailureError() {
    const goodUri = 'mongodb://' + db.getMongo().host;
    const goodConnection = "dbgood";
    const dbName = "test";
    const inputCollName = "testin";
    const badUri = "mongodb://127.0.0.1:9123";
    const connectionRegistry = [
        {
            name: goodConnection,
            type: 'atlas',
            options: {
                uri: goodUri,
            }
        },
    ];
    const spName = "sp1";
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: goodConnection,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {
                $merge: {
                    into: {connectionName: goodConnection, db: 'test', coll: "outputcoll"},
                }
            }
        ],
        connections: connectionRegistry,
        options: {checkpointOptions: {storage: {uri: badUri, db: "test", coll: "checkpointColl"}}},
        processorId: "processorId",
        tenantId: "tenantId"
    });
    assert.commandFailed(result);
    assert(result.errmsg.startsWith("Failure while reading from checkpoint storage."));
}

function badKafkaEmit() {
    const goodUri = 'mongodb://' + db.getMongo().host;
    const goodConnection = "dbgood";
    const dbName = "test";
    const inputCollName = "testin";
    const badUri = 'foohost:9092';
    const badKafkaName = "kafka1";
    const connectionRegistry = [
        {
            name: goodConnection,
            type: 'atlas',
            options: {
                uri: goodUri,
            }
        },
        {
            name: badKafkaName,
            type: 'kafka',
            options: {bootstrapServers: badUri},
        }
    ];

    // Validate start will return an error if we can't connect to Kafka.
    const spName = "sp1";
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: goodConnection,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$emit: {connectionName: badKafkaName, topic: "thisWontWork"}}
        ],
        connections: connectionRegistry,
    });
    assert.commandFailed(result);
    assert.eq("$emit to Kafka encountered error while connecting, kafka error code: -195",
              result.errmsg);
    assert.eq(8141700, result.code);

    // Try the same thing, with a dynamic topic name.
    result = db.runCommand({
        streams_startStreamProcessor: '',
        name: "sp2",
        pipeline: [
            {
                $source: {
                    connectionName: goodConnection,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {
                $emit: {
                    connectionName: badKafkaName,
                    topic: {$cond: {if: {$eq: ["$gid", 0]}, then: "t1", else: "t2"}}
                }
            }
        ],
        connections: connectionRegistry,
    });
    assert.commandFailed(result);
    assert.eq("$emit to Kafka encountered error while connecting, kafka error code: -195",
              result.errmsg);
    assert.eq(8141700, result.code);
}

badDBSourceStartError();
badKafkaSourceStartError();
badDBMergeAsyncError();
badMongoDLQAsyncError();
checkpointDbConnectionFailureError();
badKafkaEmit();
}());

// TODO(SERVER-80742): Write tests for the below.
// dbSourceDiesAfterSuccesfulStart()
// kafkaSourceDiesAfterSuccesfulStart()
// kafkaSourceStartBeforeTopicExists()
// dbSourceUnparseableUri()
// checkpointFailsAfterStart()
