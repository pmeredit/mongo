import {FixtureHelpers} from "jstests/libs/fixture_helpers.js";

// For sharded, recommend running with passthrough "sharded_collections_jscore_passthrough".
// For standalone, recommend running with passthrough "core".

const coll = db.docs;
coll.drop();

/**
 * This tests basic usages of the $betaMultiCursor stage.
 *
 * There's a fundamental question about how $multiCursor would handle reading from a local
 * collection. If one of the subpipelines reads from a local collection, should the other
 * subpipeline also be required to read from that collection? If so, how should they share the
 * storage snapshot and result stream source? I avoided implementing any mechanics of opening a
 * mongod cursor for $betaMultliCursor since it doubles the complexity and since $search handles
 * sourcing its data on its own.
 *
 * For that reason, this test only uses source stages that unfortunately have weird semantics when
 * run on a standalone vs. a sharded cluster. For that reason, this test is split into "sharded" or
 * "non-sharded" cases.
 */

// jsTestLog("BUERGER");
// const res = assert.commandWorked(db.runCommand({aggregate: coll.getName(), pipeline: [{$countNodes: {}}], cursor: {}}));
// jsTestLog(res);

if (FixtureHelpers.isMongos(db)) {
    // Test $betaMultiStream on a sharded cluster, assuming there are 2 shards (as in
    // sharded_collections_jscore_passthrough).
    {
        const res = assert.commandWorked(db.runCommand({
            aggregate: coll.getName(),
            pipeline: [
                {
                    $betaMultiStream: {
                        primary: [{$documents: [{a: 1}, {a: 2}, {a: 3}]}, {$sort: {a: 1}}],
                        secondary: [{$countNodes: {}}],
                        finishMethod: "setVar"
                    }
                },
                {$project: {a: "$a", meta: "$$SEARCH_META"}}
            ],
            cursor: {}
        }));

        // This only passes if there are exactly 2 shards in the configuration for $countNodes behavior.
        assert.eq(res.cursor.firstBatch, [
            {a: 1, meta: {count: 2}},
            {a: 1, meta: {count: 2}},
            {a: 2, meta: {count: 2}},
            {a: 2, meta: {count: 2}},
            {a: 3, meta: {count: 2}},
            {a: 3, meta: {count: 2}}
        ]);
    }
} else {
    // Test $betaMultiStream on a standalone.

    // Test $betaMultiStream with "cursor" finishMethod.
    {
        // For now, you must pass {batchSize: 0} when running with "cursor" finishMethod so that the
        // cursors are initialized before pulling any documents.
        const res = assert.commandWorked(db.runCommand({
            aggregate: coll.getName(),
            pipeline: [{
                $betaMultiStream: {
                    primary: [{$documents: [{a: 1}, {a: 2}, {a: 3}]}],
                    secondary: [{$countNodes: {}}],
                    finishMethod: "cursor"
                }
            }],
            cursor: {batchSize: 0}
        }));

        assert.eq(res.cursors.length, 2);

        const primaryCursor = res.cursors[0].cursor;
        const secondaryCursor = res.cursors[1].cursor;

        let primaryCursorGetMore =
            db.runCommand({getMore: primaryCursor.id, collection: coll.getName()});
        assert.eq(primaryCursorGetMore.cursor.nextBatch, [{a: 1}, {a: 2}, {a: 3}]);

        let secondaryCursorGetMore =
            db.runCommand({getMore: secondaryCursor.id, collection: coll.getName()});
        assert.eq(secondaryCursorGetMore.cursor.nextBatch, [{count: 1}]);
    }

    // Test $betaMultiStream with "setVar" finishMethod.
    {
        const res = assert.commandWorked(db.runCommand({
            aggregate: coll.getName(),
            pipeline: [
                {
                    $betaMultiStream: {
                        primary: [{$documents: [{a: 1}, {a: 2}, {a: 3}]}],
                        secondary: [{$countNodes: {}}],
                        finishMethod: "setVar"
                    }
                },
                {$project: {a: "$a", meta: "$$SEARCH_META"}}
            ],
            cursor: {}
        }));

        assert.eq(res.cursor.firstBatch, [
            {a: 1, meta: {count: 1}},
            {a: 2, meta: {count: 1}},
            {a: 3, meta: {count: 1}}
        ]);
    }
}
