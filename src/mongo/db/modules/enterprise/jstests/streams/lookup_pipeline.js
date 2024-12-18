/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    connectionName,
    dbName,
    dlqCollName,
    insertDocs,
    listStreamProcessors,
    logState,
    makeLookupPipeline,
    outCollName,
    runTest,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

(function() {
"use strict";

const foreignColl = db.getSiblingDB(dbName).foreign;
const outputColl = db.getSiblingDB(dbName)[outCollName];
const dlqColl = db.getSiblingDB(dbName)[dlqCollName];
const spName = "lookupPipelineTest";

(function testImplicitlyCorrelatedPipeline() {
    const inputDocs = [{k: 0}, {k: 2}];
    const foreignDocs = [{fk: 0}, {fk: 1}, {fk: 2}];

    runTest({
        spName: spName,
        // If there's 'localField' and 'foreignField' specified, then an implicit match stage is
        // added to the beginning of the pipeline.
        pipeline: makeLookupPipeline({
            $lookup: {
                from: {connectionName: connectionName, db: dbName, coll: foreignColl.getName()},
                localField: 'k',
                foreignField: 'fk',
                pipeline: [
                    {$project: {_id: 0, fk: 1, b: {$add: ["$fk", 10]}}},
                ],
                as: 'out'
            }
        }),
        setupAction: () => {
            outputColl.drop();
            dlqColl.drop();
            foreignColl.drop();
            foreignColl.insert(foreignDocs);
        },
        verifyActions: () => {
            insertDocs(spName, inputDocs);
            assert.soon(() => { return outputColl.find().itcount() == 2; }, logState(spName));
            assert.eq([{k: 0, out: [{fk: 0, b: 10}]}, {k: 2, out: [{fk: 2, b: 12}]}],
                      outputColl.find({}, {_id: 0, k: 1, out: 1}).toArray(), logState(spName));
        },
    });
})();

(function testImplicitlyCorrelatedPipelineWithLet() {
    const inputDocs = [{k: 0}, {k: 2}];
    const foreignDocs = [{fk: 0}, {fk: 1}, {fk: 2}];

    runTest({
        spName: spName,
        // The same as above, but with 'let' specified for the constant 10.
        pipeline: makeLookupPipeline({
            $lookup: {
                from: {connectionName: connectionName, db: dbName, coll: foreignColl.getName()},
                localField: 'k',
                foreignField: 'fk',
                let: {addend: 10},
                pipeline: [
                    {$project: {_id: 0, fk: 1, b: {$add: ["$fk", "$$addend"]}}},
                ],
                as: 'out'
            }
        }),
        setupAction: () => {
            outputColl.drop();
            dlqColl.drop();
            foreignColl.drop();
            foreignColl.insert(foreignDocs);
        },
        verifyActions: () => {
            insertDocs(spName, inputDocs);
            assert.soon(() => { return outputColl.find().itcount() == 2; }, logState(spName));
            assert.eq([{k: 0, out: [{fk: 0, b: 10}]}, {k: 2, out: [{fk: 2, b: 12}]}],
                      outputColl.find({}, {_id: 0, k: 1, out: 1}).toArray());
        },
    });
})();

(function testImplicitlyCorrelatedPipelineWithLetAndUnwind() {
    const inputDocs = [{k: 0}, {k: 2}];
    // Use a large number of docs so that DocumentSourceRemoteDbCursor needs to send a
    // GetMoreCommandRequest.
    let foreignDocs = Array.from({length: 5000}, (_, i) => ({fk: 0, a: i + 1}));
    foreignDocs = foreignDocs.concat([{fk: 1}, {fk: 2, a: 3}]);

    runTest({
        spName: spName,
        // The same as above, but with 'let' specified for the constant 10.
        pipeline: makeLookupPipeline({
            $lookup: {
                from: {connectionName: connectionName, db: dbName, coll: foreignColl.getName()},
                localField: 'k',
                foreignField: 'fk',
                let: {addend: 10},
                pipeline: [
                    {$project: {_id: 0, fk: 1, a: 1, b: {$add: ["$fk", "$$addend"]}}},
                ],
                as: 'out'
            }
        }, {
            $unwind: {path: "$out"}
        }),
        setupAction: () => {
            outputColl.drop();
            dlqColl.drop();
            foreignColl.drop();
            foreignColl.insert(foreignDocs);
        },
        verifyActions: () => {
            insertDocs(spName, inputDocs);
            assert.soon(() => { return outputColl.find().itcount() == 5001; }, logState(spName));
            assert.eq(
                Array.from({length: 5000}, (_, i) => ({k: 0, out: {fk: 0, a: i + 1, b: 10}})).concat(
                [{k: 2, out: {fk: 2, a: 3, b: 12}}]),
                outputColl.find({}, {_id: 0, k: 1, out: 1}).toArray());
        },
    });
})();

(function testPipelineWithLet() {
    const inputDocs = [{k: 1}, {k: 3}, {k: 4}];
    const foreignDocs = [{fk: 0}, {fk: 1}, {fk: 2}, {fk: 3}];

    runTest({
        spName: spName,
        // If there's no 'localField' and 'foreignField' specified, then the pipeline is not
        // implicitly correlated and the $match stage should be explicitly added to the inner
        // pipeline. And also the $match stage should use $expr to access let variables of input
        // document field(s).
        pipeline: makeLookupPipeline({
            $lookup: {
                from: {connectionName: connectionName, db: dbName, coll: foreignColl.getName()},
                let: {lk: "$k"},
                pipeline: [
                    {$match: {$expr: {$gte: ["$fk", "$$lk"]}}},
                    {$project: {_id: 0, fk: 1}},
                ],
                as: 'out'
            }
        }),
        setupAction: () => {
            outputColl.drop();
            dlqColl.drop();
            foreignColl.drop();
            foreignColl.insert(foreignDocs);
        },
        verifyActions: () => {
            insertDocs(spName, inputDocs);
            assert.soon(() => {
                return outputColl.find().itcount() == 3;
            }, logState(spName));
            assert.eq(
                [{k: 1, out: [{fk: 1}, {fk: 2}, {fk: 3}]}, {k: 3, out: [{fk: 3}]}, {k: 4, out: []}],
                outputColl.find({}, {_id: 0, k: 1, out: 1}).toArray());
        },
    });
})();

(function testPipelineWithLetUnwindAndMatch() {
    const inputDocs = [{k: 1}, {k: 3}, {k: 4}];
    const foreignDocs = [{fk: 0}, {fk: 1}, {fk: 2}, {fk: 3}];

    runTest({
        spName: spName,
        // If there's no 'localField' and 'foreignField' specified, then the pipeline is not
        // implicitly correlated and the $match stage should be explicitly added to the inner
        // pipeline. And also the $match stage should use $expr to access let variables of input
        // document field(s).
        pipeline: makeLookupPipeline({
            $lookup: {
                from: {connectionName: connectionName, db: dbName, coll: foreignColl.getName()},
                let: {lk: "$k"},
                pipeline: [
                    {$match: {$expr: {$gte: ["$fk", "$$lk"]}}},
                    {$project: {_id: 0, fk: 1}},
                ],
                as: 'out'
            }
        }, {
            $unwind: {path: "$out"}
        }, {
            $match: {"out.fk": {$gte: 2}}
        }),
        setupAction: () => {
            outputColl.drop();
            dlqColl.drop();
            foreignColl.drop();
            foreignColl.insert(foreignDocs);
        },
        verifyActions: () => {
            insertDocs(spName, inputDocs);
            assert.soon(() => {
                return outputColl.find().itcount() == 3;
            }, logState(spName));
            assert.eq(
                [{k: 1, out: {fk: 2}}, {k: 1, out:{fk: 3}}, {k: 3, out: {fk: 3}}],
                outputColl.find({}, {_id: 0, k: 1, out: 1}).toArray());
        },
    });
})();

(function testPipelineWithLetAndInnerLookup() {
    const inputDocs = [{k: 1}, {k: 2}];
    const foreignDocs = [{fk: 1, k2: 1}, {fk: 1, k2: 2}, {fk: 2, k2: 3}, {fk: 3, k2: 4}];
    const foreignColl2 = db.getSiblingDB(dbName).foreign2;
    const foreign2Docs = [{_id: 1, str: "abc"}, {_id: 2, str: "bcd"}, {_id: 3, str: "cde"}];

    runTest({
        spName: spName,
        pipeline: makeLookupPipeline({
            $lookup: {
                from: {connectionName: connectionName, db: dbName, coll: foreignColl.getName()},
                let: {lk: "$k"},
                pipeline: [
                    {$match: {$expr: {$eq: ["$fk", "$$lk"]}}},
                    {$project: {_id: 0, fk: 1, k2: 1}},
                    // There's an inner lookup stage in the inner pipeline of the $lookup stage. The
                    // inner pipeline is sent to the remote db server as is, so the 'from'
                    // collection should belong to the same remote server and remote db.
                    {$lookup: {from: foreignColl2.getName(), localField: "k2", foreignField: "_id",
                               as: "out2"}},
                    {$project: {_id: 0, fk: 1, k2: 1, out2: 1}},
                    {$unwind: {path: "$out2"}},
                    {$project: {fk: 1, k2: 1, str: "$out2.str"}},
                ],
                as: 'out'
            }
        },
        {$unwind: {path: "$out"}},
        // Removes the duplicated fields and fully flattens the output.
        {$project: {k: 1, k2: "$out.k2", str: "$out.str"}}),
        setupAction: () => {
            outputColl.drop();
            dlqColl.drop();
            foreignColl.drop();
            foreignColl.insert(foreignDocs);
            foreignColl2.drop();
            foreignColl2.insert(foreign2Docs);
        },
        verifyActions: () => {
            insertDocs(spName, inputDocs);
            assert.soon(() => {
                return outputColl.find().itcount() == 3;
            }, logState(spName));
            assert.eq(
                [
                    {k: 1, k2: 1, str: "abc"},
                    {k: 1, k2: 2, str: "bcd"},
                    {k: 2, k2: 3, str: "cde"},
                ],
                outputColl.find({}, {_id: 0, k: 1, k2: 1, str: 1}).toArray());
        },
    });
})();

(function testPipelineWithWindow() {
    // Input docs for the first window.
    const inputDocs1 = [{k: 1, a: 10}, {k: 1, a: 20}, {k: 2, a: 100}];
    // Input docs for the second window.
    const inputDocs2 = [{k: 3, a: 1000}];
    const foreignDocs = [{fk: 0, str: "abc"}, {fk: 1, str: "bcd"}, {fk: 2, str: "cde"}, {fk: 3}];

    runTest({
        spName: spName,
        pipeline: makeLookupPipeline({
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [
                    {
                        $group: {
                            _id: "$k",
                            sa: {$sum: "$a"}
                        }
                    },
                    {
                        $lookup: {
                            from: {
                                connectionName: connectionName,
                                db: dbName,
                                coll: foreignColl.getName()
                            },
                            let: {lk: "$_id"},
                            pipeline: [
                                {$match: {$expr: {$eq: ["$fk", "$$lk"]}}},
                                {$project: {_id: 0, fk: 1, str: 1}},
                            ],
                            as: 'out'
                        }
                    }
                ]
            }
        }),
        setupAction: () => {
            outputColl.drop();
            dlqColl.drop();
            foreignColl.drop();
            foreignColl.insert(foreignDocs);
        },
        verifyActions: () => {
            // Inserts docs for the first window.
            insertDocs(spName, inputDocs1);
            // Gives some time for the first window to close.
            sleep(2000);
            // Inserts docs for the second window which will actually closes the first window.
            insertDocs(spName, inputDocs2);

            assert.soon(() => {
                logState(spName);
                return outputColl.find().itcount() == 2;
            }, logState(spName));

            assert.eq(
                [
                    {_id: 1, sa: 30, out: [{fk: 1, str: "bcd"}]},
                    {_id: 2, sa: 100, out: [{fk: 2, str: "cde"}]},
                ],
                outputColl.find({}, {_stream_meta: 0}).sort({_id: 1}).toArray());
        },
    });
})();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
}());
