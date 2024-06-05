/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function documentsDataSourceWindowMerge() {
    const uri = 'mongodb://' + db.getMongo().host;
    const dbName = "test";
    const collName = "sp1";
    let connectionRegistry = [{name: "atlas_conn", type: 'atlas', options: {uri: uri}}];
    const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

    const documentGroups = [
        [
            {timestamp: "2024-01-01T00:00:00Z", group: 0, value: 0},
            {timestamp: "2024-01-01T00:00:01Z", group: 0, value: 1},
            {timestamp: "2024-01-01T00:00:02Z", group: 0, value: 2},
        ],
        [
            {timestamp: "2024-01-01T00:00:03Z", group: 1, value: 3},
            {timestamp: "2024-01-01T00:00:04Z", group: 1, value: 4},
            {timestamp: "2024-01-01T00:00:05Z", group: 1, value: 5},
        ],
    ];
    const testCases = [
        documentGroups[0].concat(documentGroups[1]),
        {$concatArrays: documentGroups},
    ];
    testCases.forEach((documents, i) => {
        const coll = db.getSiblingDB(dbName)[collName];
        coll.drop();
        sp.process([
            {
                $source: {
                    timeField: {$toDate: "$timestamp"},
                    tsFieldOverride: "__ts",
                    documents,
                }
            },
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(3), unit: "second"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [
                        {
                            $group: {
                                _id: "$group",
                                sum: {$sum: "$value"},
                            }
                        },
                    ]
                }
            },
            {$merge: {into: {connectionName: "atlas_conn", db: dbName, coll: collName}}}
        ]);

        const results = coll.find({}).toArray();
        assert.eq(results.length, 1, results);
        assert.docEq(results[0],
                     {
                         _id: 0,
                         _stream_meta: {
                             source: {
                                 type: "generated",
                             },
                             window: {
                                 start: ISODate("2024-01-01T00:00:00Z"),
                                 end: ISODate("2024-01-01T00:00:03Z"),
                             }
                         },
                         sum: 3
                     },
                     results);
    });
}

function documentsDataSourceInvalidExpr() {
    const uri = 'mongodb://' + db.getMongo().host;
    const dbName = "test";
    const collName = "sp1";
    let connectionRegistry = [{name: "atlas_conn", type: 'atlas', options: {uri: uri}}];
    const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

    const testCases = [
        [{$toUpper: "abc"}, ErrorCodes.StreamProcessorInvalidOptions],
        [{$concatArrays: [[{}], [0]]}, ErrorCodes.StreamProcessorInvalidOptions],
    ];
    testCases.forEach((testCase, i) => {
        const [documents, errorCode] = testCase;
        const processorName = `error${i}`;
        sp.createStreamProcessor(processorName, [
            {
                $source: {
                    timeField: {$toDate: "$timestamp"},
                    tsFieldName: "__ts",
                    documents,
                }
            },
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(3), unit: "second"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [
                        {
                            $group: {
                                _id: "$group",
                                sum: {$sum: "$value"},
                            }
                        },
                    ]
                }
            },
            {$merge: {into: {connectionName: "atlas_conn", db: dbName, coll: collName}}}
        ]);
        const coll = db.getSiblingDB(dbName)[collName];
        coll.drop();
        assert.commandFailedWithCode(
            sp[processorName].start(undefined /* options */, false /* assertWorked */), errorCode);
    });
}

documentsDataSourceWindowMerge();
documentsDataSourceInvalidExpr();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);