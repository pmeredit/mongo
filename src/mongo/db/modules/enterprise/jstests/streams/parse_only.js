/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {TEST_TENANT_ID} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

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
        name: 'test_name',
        pipeline: pipeline,
        connections: connectionRegistry,
        options: {dlq, parseOnly: true, featureFlags: {}},
        processorId: 'test_id',
    };
    const result = assert.commandWorked(db.runCommand(command));
    assert.eq(result.connectionNames.sort(), expectedConnectionNames, result.connectionNames);
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
        {
            $hoppingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                hopSize: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {
                        $lookup: {
                            from: {
                                connectionName: "conn5",
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
    expectedConnectionNames: ["conn1", "conn2", "conn3", "conn4", "conn5", "conn6"]
});
