/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {
    TEST_PROJECT_ID,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const uri = 'mongodb://' + db.getMongo().host;
const connectionRegistry = [
    {
        name: "conn1",
        type: 'sample_solar',
        options: {},
    },
    {name: "conn2", type: 'atlas', options: {uri: uri}},
    {name: "conn3", type: 'atlas', options: {uri: uri}},
    {name: "conn4", type: 'atlas', options: {uri: uri}},
    {name: "conn5", type: 'atlas', options: {uri: uri}},
    {name: "conn6", type: 'atlas', options: {uri: uri}},
];
const dbName = "parse_only";
const collName = "sink";
const foreignCollName = "foreign";
const dlqCollName = "dlq";

function test({pipeline, dlq, expectedConnectionNames}) {
    const command = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        projectId: TEST_PROJECT_ID,
        name: 'test_name',
        pipeline: pipeline,
        connections: connectionRegistry,
        options: {dlq, parseOnly: true, featureFlags: {}},
        processorId: 'test_id',
    };
    const result = assert.commandWorked(db.runCommand(command));
    assert.eq(result.connections, expectedConnectionNames, result.connections);
}

test({
    pipeline: [
        {
            $source: {
                connectionName: "conn1",
                timeField: {$toDate: "$timestamp"},
            }
        },
        {
            $lookup: {
                from: {
                    connectionName: "conn3",
                    db: dbName,
                    coll: foreignCollName,
                },
                localField: "a",
                foreignField: "b",
                as: "out"
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {
                        $lookup: {
                            from: {
                                connectionName: "conn4",
                                db: dbName,
                                coll: foreignCollName,
                            },
                            localField: "a",
                            foreignField: "b",
                            as: "out"
                        }
                    }
                ]
            }
        },
        {$merge: {into: {connectionName: "conn2", db: dbName, coll: collName}}}
    ],
    dlq: {
        connectionName: "conn6",
        db: dbName,
        coll: dlqCollName,
    },
    expectedConnectionNames: [
        { name: "conn1", stage: "$source" },
        { name: "conn3", stage: "$lookup" },
        { name: "conn4", stage: "$lookup" },
        { name: "conn2", stage: "$merge" },
        { name: "conn6" }
    ]
});

test({
    pipeline: [
        {$source: {documents: [{a: 1}]}},
    ],
    expectedConnectionNames: []
});

test({
    pipeline: [
        {
            $source: {
                connectionName: "conn1",
                timeField: {$toDate: "$timestamp"},
            }
        },
        {$merge: {into: {connectionName: "conn2", db: dbName, coll: collName}}}
    ],
    expectedConnectionNames: [
        {name: "conn1", stage: "$source"},
        {name: "conn2", stage: "$merge"},
    ]
});

test({
    pipeline: [
        {$source: {connectionName: "foo", topic: "t2"}},
        {$emit: {connectionName: "bar", topic: "t1"}}
    ],
    expectedConnectionNames: [
        {name: "foo", stage: "$source"},
        {name: "bar", stage: "$emit"},
    ]
});

test({
    pipeline: [
        {
            $source: {
                connectionName: "conn1",
                timeField: {$toDate: "$timestamp"},
            }
        },
        {$lookup: {localField: "a", foreignField: "b", as: "out", pipeline: []}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                pipeline: [{$lookup: {localField: "a", foreignField: "b", as: "out", pipeline: []}}]
            }
        },
        {$merge: {into: {connectionName: "conn2", db: dbName, coll: collName}}}
    ],
    dlq: {
        connectionName: "conn6",
        db: dbName,
        coll: dlqCollName,
    },
    expectedConnectionNames:
        [{name: "conn1", stage: "$source"}, {name: "conn2", stage: "$merge"}, {name: "conn6"}]
});

test({
    pipeline: [
        {
            $source: {
                connectionName: "conn1",
                timeField: {$toDate: "$timestamp"},
            }
        },
        {$lookup: {localField: "a", foreignField: "b", as: "out", pipeline: []}},
        {$unwind: "$foo"},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {$lookup: {localField: "a", foreignField: "b", as: "out", pipeline: []}},
                    {$unwind: "$bar"},
                ]
            }
        },
        {$merge: {into: {connectionName: "conn2", db: dbName, coll: collName}}}
    ],
    dlq: {
        connectionName: "conn6",
        db: dbName,
        coll: dlqCollName,
    },
    expectedConnectionNames:
        [{name: "conn1", stage: "$source"}, {name: "conn2", stage: "$merge"}, {name: "conn6"}]
});

function missingConnectionName() {
    const command = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        projectId: TEST_PROJECT_ID,
        name: 'test_name',
        pipeline: [
            {$source: {topic: "foo"}},
            {$merge: {into: {connectionName: "conn2", db: dbName, coll: collName}}}
        ],
        connections: connectionRegistry,
        options: {parseOnly: true, featureFlags: {}},
        processorId: 'test_id',
    };
    const result = db.runCommand(command);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorInvalidOptions);
    jsTestLog(result);
    assert.eq("StreamProcessorUserError", result.errorLabels[0]);
}

missingConnectionName();

function badSourceSpec() {
    const command = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        projectId: TEST_PROJECT_ID,
        name: 'test_name',
        pipeline: [
            {$source: "foo"},
            {$merge: {into: {connectionName: "conn2", db: dbName, coll: collName}}}
        ],
        connections: connectionRegistry,
        options: {parseOnly: true, featureFlags: {}},
        processorId: 'test_id',
    };
    const result = db.runCommand(command);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorInvalidOptions);
    jsTestLog(result);
    assert.eq("StreamProcessorUserError", result.errorLabels[0]);
    assert.eq("$source specification must be an object.", result.errmsg);
}

badSourceSpec();