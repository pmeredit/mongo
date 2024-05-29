/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {
    listStreamProcessors,
    verifyDocsEqual
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const sp = getDefaultSp();
const inputColl = db.getSiblingDB(test.dbName)[test.inputCollName];

// Test $geoWithin and $geoIntersects expressions within a $match stage.
function smoketest_geoWithin_geoIntersects() {
    inputColl.drop();
    const geoIntersects = "geoIntersects";
    const geoIntersectsColl = db.getSiblingDB(test.dbName)[geoIntersects];
    geoIntersectsColl.drop();
    sp.createStreamProcessor(geoIntersects, [
        {
            $source:
                {connectionName: test.atlasConnection, db: test.dbName, coll: inputColl.getName()}
        },
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $match: {
                "coordinates": {
                    $geoIntersects: {
                        $geometry: {
                            type: "MultiPolygon",
                            coordinates: [[[
                                [-20.0, -70.0],
                                [-30.0, -70.0],
                                [-30.0, -50.0],
                                [-20.0, -50.0],
                                [-20.0, -70.0]
                            ]]]
                        }
                    }
                }
            }
        },
        {
            $merge: {
                into: {
                    connectionName: test.atlasConnection,
                    db: test.dbName,
                    coll: geoIntersectsColl.getName()
                }
            }
        },
    ]);
    sp[geoIntersects].start();
    const geoWithin = "geoWithin";
    const geoWithinColl = db.getSiblingDB(test.dbName)[geoIntersects];
    geoWithinColl.drop();
    sp.createStreamProcessor(geoWithin, [
        {
            $source:
                {connectionName: test.atlasConnection, db: test.dbName, coll: test.inputCollName}
        },
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $match: {
                "coordinates": {
                    $geoWithin: {
                        $geometry: {
                            type: "MultiPolygon",
                            coordinates: [[[
                                [-20.0, -70.0],
                                [-30.0, -70.0],
                                [-30.0, -50.0],
                                [-20.0, -50.0],
                                [-20.0, -70.0]
                            ]]]
                        }
                    }
                }
            }
        },
        {$merge: {into: {connectionName: test.atlasConnection, db: test.dbName, coll: geoWithin}}},
    ]);
    sp[geoWithin].start();

    // Insert some data.
    const input = [
        {
            _id: "doghouse",
            coordinates: [25.0, 60.0],
            extra: {breeds: ["terrier", "dachshund", "bulldog"]}
        },
        {
            _id: "bullpen",
            coordinates: [-25.0, -60.0],
            extra: {breeds: "Scottish Highland", feeling: "bullish"}
        },
        {_id: "volcano", coordinates: [-1111.0, 2222.0], extra: {breeds: "basalt", feeling: "hot"}}
    ];
    assert.commandWorked(inputColl.insertMany(input));

    // Verify the geoIntersects stream processor's results.
    const geoIntersectsExpectedResults = [input[1]];
    // Wait for the expected result to appear in the output.
    assert.soon(() => { return geoIntersectsColl.count() == geoIntersectsExpectedResults.length; });
    // Verify the expected results.
    verifyDocsEqual(geoIntersectsColl.find({}).toArray(), geoIntersectsExpectedResults);

    // Verify the geoWithin stream processor's results.
    const geoWithinExpectedResults = [input[1]];
    // Wait for the expected result to appear in the output.
    assert.soon(() => { return geoWithinColl.count() == geoWithinExpectedResults.length; });
    // Verify the expected results.
    verifyDocsEqual(geoWithinColl.find({}).toArray(), geoWithinExpectedResults);

    // Stop streamProcessors.
    sp[geoIntersects].stop();
    sp[geoWithin].stop();
}

// $near and $nearSphere stages are not supported in agg, only in find.
function smoketest_near_nearSphere_notSupportedInAgg() {
    inputColl.drop();
    try {
        inputColl.aggregate([{$nearSphere: [0, 0]}]);
        assert(false, "The above should have failed.");
    } catch (e) {
        assert(e.toString().includes("Unrecognized pipeline stage name: '$nearSphere'"));
    }

    try {
        inputColl.aggregate([{$near: [0, 0]}]);
        assert(false, "The above should have failed.");
    } catch (e) {
        assert(e.toString().includes("Unrecognized pipeline stage name: '$near'"));
    }
}

// Validate $geoNear is not supported in the main pipeline or inside window pipeline in Atlas stream
// processing.
function smoketest_geoNear_notSupportedInStreamProcessing() {
    inputColl.drop();
    const geoNearMainPipeline = "geoNearMainPipeline";
    sp.createStreamProcessor(geoNearMainPipeline, [
        {
            $source:
                {connectionName: test.atlasConnection, db: test.dbName, coll: test.inputCollName}
        },
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $geoNear: {
                minDistance: 1,
                spherical: true,
                distanceField: "distance",
                near: {type: "Point", coordinates: [0, 0]}
            }
        },
        {
            $merge: {
                into: {
                    connectionName: test.atlasConnection,
                    db: test.dbName,
                    coll: geoNearMainPipeline
                }
            }
        },
    ]);
    let result =
        sp[geoNearMainPipeline].start(undefined /* startOptions */, false /* assertWorked */);
    assert.commandFailedWithCode(result, 72);
    assert(result.errmsg.includes("Unsupported stage: $geoNear"));

    const geoNearWindow = "geoNearInWindows";
    sp.createStreamProcessor(geoNearWindow, [
        {
            $source:
                {connectionName: test.atlasConnection, db: test.dbName, coll: test.inputCollName}
        },
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(60)},
                pipeline: [{
                    $geoNear: {
                        minDistance: 1,
                        spherical: true,
                        distanceField: "distance",
                        near: {type: "Point", coordinates: [0, 0]}
                    }
                }],
            }
        },
        {
            $merge:
                {into: {connectionName: test.atlasConnection, db: test.dbName, coll: geoNearWindow}}
        },
    ]);
    result = sp[geoNearWindow].start(undefined /* startOptions */, false /* assertWorked */);
    assert.commandFailedWithCode(result, 72);
    assert(result.errmsg.includes("Unsupported stage: $geoNear"));
}

// Validate $geoNear works inside a $lookup.pipeline.
function smoketest_lookup_geoNear() {
    inputColl.drop();
    const fromColl = db.getSiblingDB(test.dbName)["fromColl"];
    fromColl.drop();
    const geoLookup = "geoLookup";
    const outputColl = db.getSiblingDB(test.dbName)[geoLookup];
    outputColl.drop();
    sp.createStreamProcessor(geoLookup, [
        {
            $source:
                {connectionName: test.atlasConnection, db: test.dbName, coll: inputColl.getName()}
        },
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $lookup: {
                pipeline: [
                    {
                        $geoNear: {near: [0, 0], distanceField: "distance", spherical: true}
                    }
                ],
                from: {
                    connectionName: test.atlasConnection,
                    db: test.dbName,
                    coll: fromColl.getName()
                },
                as: "c",
            },
        },
        {
            $merge:
                {into: {connectionName: test.atlasConnection, db: test.dbName, coll: geoLookup}}
        },
    ]);
    sp[geoLookup].start();

    // Create geospatial index for field 'geo' on 'from'.
    assert.commandWorked(fromColl.createIndex({geo: "2dsphere"}));

    // Insert one matching document in 'from'.
    assert.commandWorked(fromColl.insert({_id: 1, geo: [0, 0]}));
    assert.commandWorked(inputColl.insert({_id: 4, x: 4}));

    // Verify the expected results.
    const expectedResults = [{"_id": 4, "x": 4, "c": [{"_id": 1, "geo": [0, 0], "distance": 0}]}];
    assert.soon(() => { return outputColl.count() == expectedResults.length; });
    verifyDocsEqual(outputColl.find({}).toArray(), expectedResults);

    sp[geoLookup].stop();
}

smoketest_geoWithin_geoIntersects();
smoketest_near_nearSphere_notSupportedInAgg();
smoketest_geoNear_notSupportedInStreamProcessing();
smoketest_lookup_geoNear();
