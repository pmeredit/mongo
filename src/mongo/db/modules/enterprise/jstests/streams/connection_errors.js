/**
 * @tags: [
 *  featureFlagStreams,
 *  # TODO SERVER-93379 investigate why this test segfaults under TSAN.
 *  tsan_incompatible,
 * ]
 */
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    getStats,
    listStreamProcessors,
    makeRandomString,
    stopStreamProcessor,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {featureFlags: {}}
    });
    assert.commandFailed(result);
    assert.eq(ErrorCodes.StreamProcessorAtlasConnectionError, result.code);
    let errmsg = `Change stream $source ${dbName}.${inputCollName} failed: No suitable servers found (\`serverSelectionTryOnce\` set): [connection refused calling hello on ':']: generic server error`;
    assert.eq(errmsg, result.errmsg);
}

function badDBSourceStartErrorWithStartAtOperationTime() {
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
        pipeline: [
            {
                $source: {
                    connectionName: dbConnectionName,
                    db: dbName,
                    coll: inputCollName,
                    config: {startAtOperationTime: db.hello().$clusterTime.clusterTime}
                }
            },
            {$emit: {connectionName: "__testMemory"}}
        ],
        connections: connectionRegistry,
        options: {featureFlags: {}}
    });
    assert.commandFailed(result);
    assert.eq(ErrorCodes.StreamProcessorAtlasConnectionError, result.code);
    assert(result.errmsg.includes(`Change stream $source test.testin failed`));
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
        pipeline: [
            {$source: {connectionName: connectionName, topic: inputTopicName}},
            {$emit: {connectionName: "__testMemory"}}
        ],
        connections: connectionRegistry,
        options: {featureFlags: {}}
    });
    assert.commandFailed(result);
    assert.eq(ErrorCodes.StreamProcessorKafkaConnectionError, result.code);
    assert(result.errmsg.includes(
               "Could not connect to the Kafka topic: Local: Broker transport failure (-195)"),
           result.errmsg);
    assert(
        result.errmsg.includes(
            "foohost:9092/bootstrap: Failed to resolve 'foohost:9092': Temporary failure in name resolution") ||
            result.errmsg.includes("Failed to resolve 'foohost:9092': Name or service not known"),
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {featureFlags: {}}
    });
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorAtlasConnectionError);
    assert.eq(
        result.errmsg,
        "$merge to test.outputcoll failed: No suitable servers found (`serverSelectionTryOnce` set): [connection refused calling hello on ':']: generic server error");
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {featureFlags: {}}
    });
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorAtlasConnectionError);
    assert.eq(
        result.errmsg,
        "$merge to test.outputcoll failed: No suitable servers found (`serverSelectionTryOnce` set): [connection refused calling hello on ':']: generic server error");
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {dlq, featureFlags: {}},
    });
    assert.commandWorked(result);

    assert.soon(() => {
        const result = db.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
        const sp = result.streamProcessors.find((sp) => sp.name == spName);
        return sp.status == "error" &&
            sp.error.code == ErrorCodes.StreamProcessorAtlasConnectionError &&
            sp.error.retryable == true && sp.error.userError == false &&
            sp.error.reason ===
            "Dead letter queue test.dlq failed: No suitable servers found (`serverSelectionTryOnce` set): [connection refused calling hello on ':']: generic server error";
    });

    stopStreamProcessor(spName);
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {featureFlags: {}},
    });
    assert.commandFailed(result);
    jsTestLog(result);
    assert(result.errmsg.includes(
        "$emit to Kafka topic encountered error while connecting: Local: Broker transport failure (-195)"));
    assert(
        result.errmsg.includes(
            "foohost:9092/bootstrap: Failed to resolve 'foohost:9092': Temporary failure in name resolution") ||
            result.errmsg.includes("Failed to resolve 'foohost:9092': Name or service not known"),
        result.errmsg);
    assert.eq(ErrorCodes.StreamProcessorKafkaConnectionError, result.code);

    // Try the same thing, with a dynamic topic name.
    result = db.runCommand({
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: "sp2",
        processorId: "sp2",
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
        options: {featureFlags: {}},
    });
    assert.commandFailed(result);
    assert(result.errmsg.includes(
        "$emit to Kafka topic encountered error while connecting: Local: Broker transport failure (-195)"));
    assert(
        result.errmsg.includes(
            "foohost:9092/bootstrap: Failed to resolve 'foohost:9092': Temporary failure in name resolution") ||
            result.errmsg.includes("Failed to resolve 'foohost:9092': Name or service not known"),
        result.errmsg);
    assert.eq(ErrorCodes.StreamProcessorKafkaConnectionError, result.code);
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
    rstSource.initiate(
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {featureFlags: {}},
    }));

    // Validate the $source and $merge are working.
    assert.eq(0, outputColl.count());
    inputColl.insert({a: 1});
    assert.soon(() => { return outputColl.count() == 1; });

    // Now kill the $source replset.
    rstSource.stopSet();

    // Verify the streamProcessor goes into an error state.
    assert.soon(() => {
        let result =
            dbMerge.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
        let sp = result.streamProcessors.find((sp) => sp.name == spName);
        jsTestLog(sp);
        return sp.status == "error" &&
            sp.error.reason.includes("Change stream $source test.testinput failed");
    });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
}

function mergeFailsWithFullQueue() {
    // Start the replset used as the $merge target for the streamProcessor.
    const mongostream = db;
    const mergeTarget = new ReplSetTest({
        name: "streams_mergefails_test",
        nodes: 1,
        waitForKeys: false,
    });
    mergeTarget.startSet();
    mergeTarget.initiate(
        Object.extend(mergeTarget.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));
    const conn = mergeTarget.getPrimary();
    const dbName = "test";
    const dbMergeTarget = conn.getDB(dbName);
    const mergeTargetUri = 'mongodb://' + dbMergeTarget.getMongo().host;
    const writeConnection = "mergeTargetThatWillGetKilled";
    const outputCollName = "testoutput";
    // Use the default replset for the $merge target and to actually
    // run the streamProcessor.
    const connectionRegistry = [
        {
            name: writeConnection,
            type: 'atlas',
            options: {
                uri: mergeTargetUri,
            }
        },
        {
            name: '__testMemory',
            type: 'in_memory',
            options: {},
        }
    ];
    const outputColl = dbMergeTarget.getSiblingDB(dbName)[outputCollName];
    outputColl.drop();

    const spName = "sp1";

    // Each doc is about 2Kb, the unwind will multiple it by 100.
    // So each input doc leads to 200kB of data.
    // With 10 input docs that's 2MB.
    Random.setRandomSeed(42);
    let array = [];
    for (let i = 0; i < 100; i += 1) {
        array.push(i);
    }
    let doc = {arr: array, str: makeRandomString(2000)};
    let input = [];
    for (let i = 0; i < 20; i += 1) {
        input.push(doc);
    }

    // Start the stream processor.
    assert.commandWorked(mongostream.runCommand({
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
        pipeline: [
            {$source: {connectionName: "__testMemory"}},
            {$unwind: "$arr"},
            {$merge: {into: {connectionName: writeConnection, db: dbName, coll: outputCollName}}}
        ],
        connections: connectionRegistry,
        options: {
            featureFlags: {
                // 500kB
                maxQueueSizeBytes: NumberInt(500000)
            }
        }
    }));

    const batches = 3;
    for (let i = 0; i < batches; i += 1) {
        assert.commandWorked(db.runCommand({
            streams_testOnlyInsert: '',
            tenantId: TEST_TENANT_ID,
            name: spName,
            documents: input
        }));
    }
    // Now kill the $merge replset.
    mergeTarget.stopSet();

    // Verify the streamProcessor goes into an error state.
    assert.soon(() => {
        let result =
            mongostream.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
        let sp = result.streamProcessors.find((sp) => sp.name == spName);
        jsTestLog(getStats(spName));
        return sp.status == "error";
    });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
}

// Validate that a stream processor in an error state can be restarted by calling start.
function startFailedStreamProcessor() {
    const rstSource = new ReplSetTest({
        name: "stream_sourcefails_test",
        nodes: 1,
        waitForKeys: false,
    });
    rstSource.startSet();
    rstSource.initiate(
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
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {featureFlags: {}},
    }));

    // Validate the $source and $merge are working.
    assert.eq(0, outputColl.count());
    inputColl.insert({a: 1});
    assert.soon(() => { return outputColl.count() == 1; });

    // Now kill the $source replset.
    rstSource.stopSet();

    // Verify the streamProcessor goes into an error state.
    assert.soon(() => {
        let result =
            dbMerge.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
        let sp = result.streamProcessors.find((sp) => sp.name == spName);
        jsTestLog(sp);
        return sp.status == "error" &&
            sp.error.reason.includes("Change stream $source test.testinput failed");
    });

    // Restart the $source replset.
    rstSource.startSet();
    rstSource.initiate(
        Object.extend(rstSource.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));

    // Issue the start command again for the same SP.
    let result = assert.commandWorked(dbMerge.runCommand({
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
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
        options: {featureFlags: {}},
    }));
    assert.commandWorked(result);
    result = db.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
    let sp = result.streamProcessors.find((sp) => sp.name == spName);
    assert.eq(sp.status, "running");

    // Stop the streamProcessor.
    stopStreamProcessor(spName);

    // Stop the $source replset.
    rstSource.stopSet();
}

function unparseableMongocxxUri() {
    const expectedErrorCode = 1;  // InternalError
    const spName = "sp1";
    const result = db.runCommand({
        streams_startStreamProcessor: "Test roll back random",
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: spName,
        pipeline: [
            {"$source": {"connectionName": "conn1", "db": "test", "coll": "testin"}},
            {"$merge": {"into": {"connectionName": "conn1", "db": "test", "coll": "testout"}}}
        ],
        connections: [{name: "conn1", type: "atlas", options: {uri: ""}}],
        options: {featureFlags: {}},
    });
    assert.commandFailedWithCode(result, expectedErrorCode);
}

function mergeListIndexesAuthFailure() {
    const keyfile = 'jstests/libs/key1';
    let replTest = new ReplSetTest({nodes: 1, keyFile: keyfile, nodeOptions: {auth: ""}});
    replTest.startSet();
    replTest.initiate();
    let primary = replTest.getPrimary();
    const admin = primary.getDB('admin');
    const dbName = "test";

    // Create the admin user.
    assert.commandWorked(
        admin.runCommand({createUser: "admin", pwd: "password", roles: ["__system"]}));
    assert(admin.auth("admin", "password"));
    // Create a user with limited permissions.
    const limitedRole = "limitedRole";
    assert.commandWorked(admin.runCommand({
        createRole: limitedRole,
        privileges: [{resource: {db: "", collection: ""}, actions: ["find", "changeStream"]}],
        roles: []
    }));
    const userName = "readOnly";
    const password = "password";
    assert.commandWorked(
        admin.runCommand({createUser: userName, pwd: password, roles: [limitedRole]}));
    // Create a URI corresponding to the limited user.
    const readOnlyUri = `mongodb://${userName}:${password}@${admin.getMongo().host}`;
    const connectionName = "limited";
    const inputColl = "coll";
    const sp = new Streams(TEST_TENANT_ID, [
        {
            name: connectionName,
            type: 'atlas',
            options: {uri: readOnlyUri},
        },
    ]);

    // Start a stream processor that does not have the auth to listIndexes in MergeOperator.
    const spName = "nomergeauth";
    sp.createStreamProcessor(spName, [
        {$source: {connectionName: connectionName, db: dbName, coll: inputColl}},
        {
            $merge: {
                into: {
                    connectionName: connectionName,
                    db: dbName,
                    coll: "outcoll",
                }
            }
        },
    ]);
    let result = sp[spName].start(undefined /* options */, false /* assertWorked */);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorAtlasUnauthorizedError);
    assert(result.errmsg.includes(
        "$merge to test.outcoll failed: not authorized on test to execute command { listIndexes: \"outcoll\", cursor: {}, $db: \"test\""));

    replTest.stopSet();
}

function mergeUpdateFailure() {
    const keyfile = 'jstests/libs/key1';
    let replTest = new ReplSetTest({nodes: 1, keyFile: keyfile, nodeOptions: {auth: ""}});
    replTest.startSet();
    replTest.initiate();
    let primary = replTest.getPrimary();
    const admin = primary.getDB('admin');
    const dbName = "test";
    const targetDb = admin.getSiblingDB(dbName);

    // Create the admin user.
    assert.commandWorked(
        admin.runCommand({createUser: "admin", pwd: "password", roles: ["__system"]}));
    assert(admin.auth("admin", "password"));
    // Create a user with limited permissions.
    const limitedRole = "limitedRole";
    assert.commandWorked(admin.runCommand({
        createRole: limitedRole,
        privileges: [
            {resource: {db: "", collection: ""}, actions: ["find", "changeStream", "listIndexes"]}
        ],
        roles: []
    }));
    const userName = "readOnly";
    const password = "password";
    assert.commandWorked(
        admin.runCommand({createUser: userName, pwd: password, roles: [limitedRole]}));
    // Create a URI corresponding to the limited user.
    const readOnlyUri = `mongodb://${userName}:${password}@${admin.getMongo().host}`;
    const connectionName = "limited";
    const inputColl = "coll";
    const sp = new Streams(TEST_TENANT_ID, [
        {
            name: connectionName,
            type: 'atlas',
            options: {uri: readOnlyUri},
        },
    ]);

    // Start a stream processor that does not have the auth to listIndexes in MergeOperator.
    const spName = "nomergeauth";
    sp.createStreamProcessor(spName, [
        {$source: {connectionName: connectionName, db: dbName, coll: inputColl}},
        {
            $merge: {
                into: {
                    connectionName: connectionName,
                    db: dbName,
                    coll: "outcoll",
                }
            }
        },
    ]);
    // TODO(SERVER-88295): This start works, but later we can detect the lack of auth during start
    // and fail. When we change this test we should make another test that tests the async failure
    // path.
    sp[spName].start();

    targetDb[inputColl].insertOne({a: 1});
    assert.soon(() => {
        const result = sp.listStreamProcessors();
        const processor = result.streamProcessors.filter(p => p.name === spName)[0];
        return processor.status == "error" &&
            processor.error.code == ErrorCodes.StreamProcessorAtlasUnauthorizedError &&
            processor.error.reason.includes(
                "not authorized on test to execute command { update: \"outcoll\"");
    });

    sp[spName].stop();

    replTest.stopSet();
}

function timeseriesEmitUpdateFailure() {
    const keyfile = 'jstests/libs/key1';
    let replTest = new ReplSetTest({nodes: 1, keyFile: keyfile, nodeOptions: {auth: ""}});
    replTest.startSet();
    replTest.initiate();
    let primary = replTest.getPrimary();
    const admin = primary.getDB('admin');
    const dbName = "test";
    const targetDb = admin.getSiblingDB(dbName);

    // Create the admin user.
    assert.commandWorked(
        admin.runCommand({createUser: "admin", pwd: "password", roles: ["__system"]}));
    assert(admin.auth("admin", "password"));
    // Create a user with limited permissions.
    const limitedRole = "limitedRole";
    assert.commandWorked(admin.runCommand({
        createRole: limitedRole,
        privileges: [{
            resource: {db: "", collection: ""},
            actions: ["find", "changeStream", "listIndexes", "listCollections"]
        }],
        roles: []
    }));
    const userName = "readOnly";
    const password = "password";
    assert.commandWorked(
        admin.runCommand({createUser: userName, pwd: password, roles: [limitedRole]}));
    // Create a URI corresponding to the limited user.
    const readOnlyUri = `mongodb://${userName}:${password}@${admin.getMongo().host}`;
    const connectionName = "limited";
    const inputColl = "coll";
    const sp = new Streams(TEST_TENANT_ID, [
        {
            name: connectionName,
            type: 'atlas',
            options: {uri: readOnlyUri},
        },
    ]);

    const spName = "notimeseriesemitauth";
    const timeseriesColl = targetDb["timeseries_coll"];
    assert.commandWorked(targetDb.createCollection(
        "timeseries_coll", {timeseries: {timeField: '_ts', metaField: 'metaData'}}));

    sp.createStreamProcessor(spName, [
        {$source: {connectionName: connectionName, db: dbName, coll: inputColl}},
        {
            $emit: {
                connectionName: connectionName,
                db: dbName,
                coll: timeseriesColl.getName(),
                timeseries: {timeField: '_ts', metaField: 'metaData'}
            }
        },
    ]);
    // TODO(SERVER-88295): This start works, but later we can detect the lack of auth during start
    // and fail. When we change this test we should make another test that tests the async failure
    // path.
    sp[spName].start();

    targetDb[inputColl].insertOne({a: 1});
    assert.soon(() => {
        const result = sp.listStreamProcessors();
        const processor = result.streamProcessors.filter(p => p.name === spName)[0];
        jsTestLog(result);
        return processor.status == "error" &&
            processor.error.code == ErrorCodes.StreamProcessorAtlasUnauthorizedError &&
            processor.error.reason.includes(
                "not authorized on test to execute command { insert: \"timeseries_coll\"");
    });

    sp[spName].stop();

    replTest.stopSet();
}

badDBSourceStartError();
badDBSourceStartErrorWithStartAtOperationTime();
badKafkaSourceStartError();
badMergeStartError();
badMerge_WithOn_StartError();
badMongoDLQAsyncError();
badKafkaEmit();
changeSourceFailsAfterSuccesfulStart();
startFailedStreamProcessor();
unparseableMongocxxUri();
mergeListIndexesAuthFailure();
mergeUpdateFailure();
timeseriesEmitUpdateFailure();
mergeFailsWithFullQueue();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
}());

// TODO(SERVER-80742): Write tests for the below.
// badKafkaSourceAsyncError()
// badKafkaEmitAsyncError()
// kafkaSourceStartBeforeTopicExists()
// badDLQStartError();
// checkpointFailsAfterStart()
