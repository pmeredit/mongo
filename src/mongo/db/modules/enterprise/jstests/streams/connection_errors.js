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
    assert.eq(13053, result.code);
    let errmsg = `Failed to connect to change stream $source for db: ${dbName} and collection: ${inputCollName}: No suitable servers found (\`serverSelectionTryOnce\` set): [connection refused calling hello on ':']: generic server error`;
    assert.eq(errmsg, result.errmsg);
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
    assert.eq(
        "Could not connect to the Kafka topic with kafka error code: -195, message: Local: Broker transport failure.",
        result.errmsg);
}

// Create a streamProcessor with a bad $merge target. Verify the
// streamProcessor reports a meaningful error in listStreamProcessors.
function badMergeStartError() {
    // Chose a valid network address and port that does not have a running server on it.
    const goodUri = 'mongodb://' + db.getMongo().host;
    const goodConnection = "dbgood";
    const dbName = "test";
    const inputCollName = "testin";
    const badUri = "mongodb://127.0.0.1:9123";
    const badConnection = "dbbad";
    const outputCollName = "outputcoll";
    const outputDbName = "test";
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
                    into: {connectionName: badConnection, db: outputDbName, coll: outputCollName},
                }
            }
        ],
        connections: connectionRegistry,
    });
    assert.commandFailedWithCode(result, 13053);
    assert.eq(
        result.errmsg,
        "Failed to connect to $merge to test.outputcoll: No suitable servers found (`serverSelectionTryOnce` set): [connection refused calling hello on ':']: generic server error");
}

// Test a bad $merge state with on specified, should throw an error during start.
function badMerge_WithOn_StartError() {
    const goodUri = 'mongodb://' + db.getMongo().host;
    const goodConnection = "dbgood";
    const dbName = "test";
    const inputCollName = "testin";
    const outputCollName = "outputcoll";
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
                    into: {connectionName: badConnection, db: dbName, coll: outputCollName},
                    on: ['foo']
                },
            }
        ],
        connections: connectionRegistry,
    });
    assert.commandFailedWithCode(result, 13053);
    assert.eq(
        result.errmsg,
        "Failed to connect to $merge to test.outputcoll: No suitable servers found (`serverSelectionTryOnce` set): [connection refused calling hello on ':']: generic server error");
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
        return sp.status == "error" && sp.error.code == 13053 &&
            sp.error.reason ===
            "Failed to connect to DLQ at test.dlq: No suitable servers found (`serverSelectionTryOnce` set): [connection refused calling hello on ':']: generic server error";
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

// Validate listStreamProcessors reports a meaningful error for a $source fails sometime after
// start succeeds.
function changeSourceFailsAfterSuccesfulStart() {
    // Start the replset used as the $source for the streamProcessor.
    const rstSource = new ReplSetTest({
        name: "stream_sourcefails_test",
        nodes: 1,
        waitForKeys: false,
    });
    rstSource.startSet();
    rstSource.initiateWithAnyNodeAsPrimary(
        Object.extend(rstSource.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));
    const conn = rstSource.getPrimary();
    const dbName = "test";
    const dbSource = conn.getDB(dbName);
    const sourceUri = 'mongodb://' + dbSource.getMongo().host;
    const sourceConnection = "sourceDbThatWillGetKilled";
    const mergeConnection = "mergeDbThatStaysAlive";
    const inputCollName = "testinput";
    const outputCollName = "testoutput";
    const mergeUri = 'mongodb://' + db.getMongo().host;
    // Use the default replset for the $merge target and to actually
    // run the streamProcessor.
    const dbMerge = db;
    const connectionRegistry = [
        {
            name: sourceConnection,
            type: 'atlas',
            options: {
                uri: sourceUri,
            }
        },
        {
            name: mergeConnection,
            type: 'atlas',
            options: {
                uri: mergeUri,
            }
        }
    ];
    const inputColl = dbSource.getSiblingDB(dbName)[inputCollName];
    const outputColl = dbMerge.getSiblingDB(dbName)[outputCollName];
    outputColl.drop();

    // The start command should succeed.
    const spName = "sp1";
    assert.commandWorked(dbMerge.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: sourceConnection,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$merge: {into: {connectionName: mergeConnection, db: dbName, coll: outputCollName}}}
        ],
        connections: connectionRegistry,
    }));

    // Validate the $source and $merge are working.
    assert.eq(0, outputColl.count());
    inputColl.insert({a: 1});
    assert.soon(() => { return outputColl.count() == 1; });

    // Now kill the $source replset.
    rstSource.stopSet();

    // Verify the streamProcessor goes into an error state.
    assert.soon(() => {
        let result = dbMerge.runCommand({streams_listStreamProcessors: ''});
        let sp = result.streamProcessors.find((sp) => sp.name == spName);
        jsTestLog(sp);
        return sp.status == "error" &&
            sp.error.reason.includes(
                "Failed to connect to change stream $source for db: test and collection: testinput:");
    });

    // Stop the streamProcessor.
    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: '', name: spName}));
}

// Validate that a stream processor in an error state can be restarted by calling start.
function startFailedStreamProcessor() {
    const rstSource = new ReplSetTest({
        name: "stream_sourcefails_test",
        nodes: 1,
        waitForKeys: false,
    });
    rstSource.startSet();
    rstSource.initiateWithAnyNodeAsPrimary(
        Object.extend(rstSource.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));
    const conn = rstSource.getPrimary();
    const dbName = "test";
    const dbSource = conn.getDB(dbName);
    const sourceUri = 'mongodb://' + dbSource.getMongo().host;
    const sourceConnection = "sourceDbThatWillGetKilled";
    const mergeConnection = "mergeDbThatStaysAlive";
    const inputCollName = "testinput";
    const outputCollName = "testoutput";
    const mergeUri = 'mongodb://' + db.getMongo().host;
    // Use the default replset for the $merge target and to actually
    // run the streamProcessor.
    const dbMerge = db;
    const connectionRegistry = [
        {
            name: sourceConnection,
            type: 'atlas',
            options: {
                uri: sourceUri,
            }
        },
        {
            name: mergeConnection,
            type: 'atlas',
            options: {
                uri: mergeUri,
            }
        }
    ];
    const inputColl = dbSource.getSiblingDB(dbName)[inputCollName];
    const outputColl = dbMerge.getSiblingDB(dbName)[outputCollName];
    outputColl.drop();

    // The start command should succeed.
    const spName = "sp1";
    assert.commandWorked(dbMerge.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: sourceConnection,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$merge: {into: {connectionName: mergeConnection, db: dbName, coll: outputCollName}}}
        ],
        connections: connectionRegistry,
    }));

    // Validate the $source and $merge are working.
    assert.eq(0, outputColl.count());
    inputColl.insert({a: 1});
    assert.soon(() => { return outputColl.count() == 1; });

    // Now kill the $source replset.
    rstSource.stopSet();

    // Verify the streamProcessor goes into an error state.
    assert.soon(() => {
        let result = dbMerge.runCommand({streams_listStreamProcessors: ''});
        let sp = result.streamProcessors.find((sp) => sp.name == spName);
        jsTestLog(sp);
        return sp.status == "error" &&
            sp.error.reason.includes(
                "Failed to connect to change stream $source for db: test and collection: testinput:");
    });

    // Restart the $source replset.
    rstSource.startSet();
    rstSource.initiateWithAnyNodeAsPrimary(
        Object.extend(rstSource.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));

    // Issue the start command again for the same SP.
    let result = assert.commandWorked(dbMerge.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: sourceConnection,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$merge: {into: {connectionName: mergeConnection, db: dbName, coll: outputCollName}}}
        ],
        connections: connectionRegistry,
    }));
    assert.commandWorked(result);
    result = db.runCommand({streams_listStreamProcessors: ''});
    let sp = result.streamProcessors.find((sp) => sp.name == spName);
    assert.eq(sp.status, "running");

    // Stop the streamProcessor.
    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: '', name: spName}));

    // Stop the $source replset.
    rstSource.stopSet();
}

function unparseableMongocxxUri() {
    const expectedErrorCode = 1;  // InternalError
    const result = db.runCommand({
        "streams_startStreamProcessor": "Test roll back random",
        "name": "sp1",
        "pipeline": [
            {"$source": {"connectionName": "conn1", "db": "test", "coll": "testin"}},
            {"$merge": {"into": {"connectionName": "conn1", "db": "test", "coll": "testout"}}}
        ],
        "connections": [{name: "conn1", type: "atlas", options: {uri: ""}}],
    });
    assert.commandFailedWithCode(result, expectedErrorCode);
}

badDBSourceStartError();
badKafkaSourceStartError();
badMergeStartError();
badMerge_WithOn_StartError();
badMongoDLQAsyncError();
checkpointDbConnectionFailureError();
badKafkaEmit();
changeSourceFailsAfterSuccesfulStart();
startFailedStreamProcessor();
unparseableMongocxxUri();
}());

// TODO(SERVER-80742): Write tests for the below.
// badKafkaSourceAsyncError()
// badKafkaEmitAsyncError()
// kafkaSourceStartBeforeTopicExists()
// badDLQStartError();
// checkpointFailsAfterStart()
