/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    insertDocs,
    listStreamProcessors,
    sanitizeDoc,
    stopStreamProcessor,
    TEST_PROJECT_ID,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

(function() {
"use strict";

const outColl = db.output_coll;
const dlqColl = db.dlq_coll;
const inputColl = db.input_coll;
const spName = "mergeTest";
const goodUri = 'mongodb://' + db.getMongo().host;
const badUri = "mongodb://badUri";

function startStreamProcessor(pipeline, uri = goodUri, validateSuccess = true) {
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        projectId: TEST_PROJECT_ID,
        name: spName,
        processorId: 'mergeTest1',
        pipeline: pipeline,
        connections: [
            {name: "db1", type: 'atlas', options: {uri: uri}},
            {name: '__testMemory', type: 'in_memory', options: {}},
        ],
        options:
            {dlq: {connectionName: "db1", db: "test", coll: dlqColl.getName()}, featureFlags: {}}
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    if (validateSuccess) {
        assert.commandWorked(result);
    }
    return result;
}

(function testKeepExistingInsertMode() {
    jsTestLog("Running testKeepExistingInsertMode");

    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                whenMatched: 'keepExisting',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // Insert 2 documents into the stream.
    insertDocs(spName, [{_id: 0, a: 0}, {_id: 1, a: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents (4 good, 4 bad that violate unique constraint on field a) into the
    // stream.
    insertDocs(spName, [
        {_id: 2, a: 2},
        {_id: 3, a: 1},
        {_id: 4, a: 4},
        {_id: 5, a: 4},
        {_id: 6, a: 6},
        {_id: 7, a: 6},
        {_id: 8, a: 8},
        {_id: 9, a: 8},
    ]);

    // Verify that the 4 documents are successfully inserted.
    assert.soon(() => { return outColl.find().itcount() == 6; });
    assert.sameMembers(
        [
            {_id: 0, a: 0},
            {_id: 1, a: 1},
            {_id: 2, a: 2},
            {_id: 4, a: 4},
            {_id: 6, a: 6},
            {_id: 8, a: 8}
        ],
        outColl.find().toArray().map((doc) => sanitizeDoc(doc)));

    assert.soon(() => { return dlqColl.find().itcount() == 4; });
    // Verify that the 4 documents that caused duplicate key errors are in the DLQ.
    let res = dlqColl.find({"errInfo.reason": /a_1 dup key/}).toArray();
    assert.eq(4, res.length, dlqColl.find().toArray());
    for (const dqlDoc of res) {
        assert.eq(dqlDoc.operatorName, "MergeOperator");
    }

    // Insert 3 more documents (2 good, 1 bad that violates unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'keepExisting' whenMatched mode
    // only the first one should get inserted.
    insertDocs(spName, [{_id: 10, a: 0}, {_id: 11, a: 11}, {_id: 11, a: 12}]);

    assert.soon(() => { return outColl.find().itcount() == 7; });
    assert.soon(() => { return dlqColl.find().itcount() == 5; });
    assert.eq([{_id: 11, a: 11}], outColl.find({_id: 11}).toArray().map((doc) => sanitizeDoc(doc)));

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testMergeInsertMode() {
    jsTestLog("Running testMergeInsertMode");

    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                whenMatched: 'merge',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // Insert 2 documents into the stream.
    insertDocs(spName, [{_id: 0, a: 0}, {_id: 1, a: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents (4 good, 4 bad that violate unique constraint on field a) into the
    // stream.
    insertDocs(spName, [
        {_id: 2, a: 2},
        {_id: 3, a: 1},
        {_id: 4, a: 4},
        {_id: 5, a: 4},
        {_id: 6, a: 6},
        {_id: 7, a: 6},
        {_id: 8, a: 8},
        {_id: 9, a: 8}
    ]);

    assert.soon(() => { return outColl.find().itcount() == 6; });
    assert.soon(() => { return dlqColl.find().itcount() == 4; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 4);

    // Insert 3 more documents (2 good, 1 bad that violates unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'merge' whenMatched mode the 2
    // documents the should get merged.
    insertDocs(spName, [{_id: 10, a: 0}, {_id: 11, a: 11, obj: {a: 1}}, {_id: 11, obj: {b: 1}}]);

    assert.soon(() => { return outColl.find().itcount() == 7; });
    assert.soon(() => { return dlqColl.find().itcount() == 5; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 5);
    assert.soon(() => {
        let results = outColl.find({_id: 11}).toArray().map((doc) => sanitizeDoc(doc));
        jsTestLog(tojson(results));
        let expected = [{_id: 11, a: 11, obj: {b: 1}}];
        return tojson(expected) === tojson(results);
    });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testMergeInsertModeWithOnFields() {
    jsTestLog("Running testMergeInsertModeWithOnFields");

    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                whenMatched: 'merge',
                whenNotMatched: 'insert',
                on: "_id"
            }
        }
    ]);

    // Insert 2 documents into the stream.
    insertDocs(spName, [{_id: 0, a: 0}, {_id: 1, a: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testReplaceInsertMode() {
    jsTestLog("Running testReplaceInsertMode");

    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // Insert 2 documents into the stream.
    insertDocs(spName, [{_id: 0, a: 0}, {_id: 1, a: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents (4 good, 4 bad that violate unique constraint on field a) into the
    // stream.
    insertDocs(spName, [
        {_id: 2, a: 2},
        {_id: 3, a: 1},
        {_id: 4, a: 4},
        {_id: 5, a: 4},
        {_id: 6, a: 6},
        {_id: 7, a: 6},
        {_id: 8, a: 8},
        {_id: 9, a: 8}
    ]);

    assert.soon(() => { return outColl.find().itcount() == 6; });
    assert.soon(() => { return dlqColl.find().itcount() == 4; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 4);

    // Insert 3 more documents (2 good, 1 bad that violates unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'replace' whenMatched mode the
    // second document should overwrite the first one.
    insertDocs(spName, [{_id: 10, a: 0}, {_id: 11, a: 11, obj: {a: 1}}, {_id: 11, obj: {b: 1}}]);

    assert.soon(() => { return outColl.find().itcount() == 7; });
    assert.soon(() => { return dlqColl.find().itcount() == 5; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 5);
    assert.eq([{_id: 11, obj: {b: 1}}],
              outColl.find({_id: 11}).toArray().map((doc) => sanitizeDoc(doc)));

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testReplaceDiscardMode() {
    jsTestLog("Running testReplaceDiscardMode");

    outColl.drop();
    dlqColl.drop();

    // Insert 2 documents into outColl.
    assert.commandWorked(outColl.insert([{_id: 0, a: 0}, {_id: 1, a: 1}]));

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'discard'
            }
        }
    ]);

    // Insert 3 documents into the stream, only 2 should get inserted.
    insertDocs(spName, [{_id: 0, a: 0}, {_id: 1, a: 1}, {_id: 2, a: 2}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });
    assert.soon(() => { return dlqColl.find().itcount() == 0; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents into the stream.
    insertDocs(spName, [
        {_id: 0, a: 2},
        {_id: 1, a: 2},
        {_id: 0, a: 1},
        {_id: 1, a: 3},
        {_id: 0, a: 3},
        {_id: 1, a: 2},
        {_id: 0, a: 4},
        {_id: 1, a: 2}
    ]);

    assert.soon(() => { return outColl.find().itcount() == 2; });
    assert.soon(() => { return dlqColl.find().itcount() == 4; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 4);
    assert.soon(() => {
        return tojson([{_id: 0, a: 4}]) ==
            tojson(outColl.find({_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{_id: 1, a: 2}]) ==
            tojson(outColl.find({_id: 1}).toArray().map((doc) => sanitizeDoc(doc)));
    });

    // Insert 3 more documents (2 good, 2 bad that violate unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'replace' whenMatched mode the
    // second document should overwrite the first one.
    insertDocs(
        spName,
        [{_id: 1, a: 4}, {_id: 0, a: 2}, {_id: 1, a: 11, obj: {a: 1}}, {_id: 1, obj: {b: 1}}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });
    assert.soon(() => { return dlqColl.find().itcount() == 6; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 6);
    assert.soon(() => {
        return tojson([{_id: 0, a: 4}]) ==
            tojson(outColl.find({_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{_id: 1, obj: {b: 1}}]) ==
            tojson(outColl.find({_id: 1}).toArray().map((doc) => sanitizeDoc(doc)));
    });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testReplaceDiscardModeWithOnFields() {
    jsTestLog("Running testReplaceDiscardModeWithOnFields");

    outColl.drop();
    dlqColl.drop();

    // Insert 2 documents into outColl.
    assert.commandWorked(outColl.insert([{x: 0, a: 0}, {x: 1, a: 1}]));
    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));
    assert.commandWorked(outColl.createIndex({x: 1}, {unique: true}));

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'discard',
                on: "x"
            }
        }
    ]);

    // Insert 4 documents (2 good, 2 bad) into the stream. Of the 2 bad documents,
    // - one contain array in the on field 'x' and is added to the dlq while creating the batch
    // - one violates unique constraint on field 'a' and is added to the dlq while the batch is
    //   getting flushed
    insertDocs(spName, [{x: 0, a: 0, b: 0}, {x: 1, a: 0}, {x: [1, 2], a: 0}, {x: 1, a: 1, b: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });
    assert.soon(() => {
        return tojson([{x: 0, a: 0, b: 0}]) ==
            tojson(outColl.find({a: 0}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{x: 1, a: 1, b: 1}]) ==
            tojson(outColl.find({a: 1}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 2; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 2);

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testDynamicTarget() {
    jsTestLog("Running testDynamicTarget");

    const outColl1 = db.getSiblingDB('cust1').outColl1;
    outColl1.drop();
    const outColl2 = db.getSiblingDB('cust2').outColl2;
    outColl2.drop();
    dlqColl.drop();

    // Start a stream processor with dynamic 'db' & 'coll' name expressions.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {
                    connectionName: 'db1',
                    db: '$customer.name',
                    coll: {$cond: {if: {$eq: ['$gid', 0]}, then: 'outColl1', else: 'outColl2'}}
                },
                whenMatched: 'keepExisting',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // Insert 2 documents into the stream. One document has gid: 0 and the other has gid: 1.
    insertDocs(spName, [
        {_id: 0, customer: {name: "cust1"}, a: 0, gid: 0},
        {_id: 1, customer: {name: "cust2"}, a: 1, gid: 1}
    ]);

    // One document should go to outColl1 and the other should go to outColl2.
    assert.soon(() => { return outColl1.find().itcount() == 1 && outColl2.find().itcount() == 1; });
    jsTestLog(tojson(outColl1.find().toArray().map((doc) => sanitizeDoc(doc))));
    jsTestLog(tojson(outColl2.find().toArray().map((doc) => sanitizeDoc(doc))));

    assert.commandWorked(outColl1.createIndex({a: 1}, {unique: true}));
    assert.commandWorked(outColl2.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents (5 good, 3 bad that violate unique constraint on field a) into the
    // stream.
    insertDocs(spName, [
        {_id: 2, customer: {name: "cust2"}, a: 2},          // missing != 0, goes to outColl2
        {_id: 3, customer: {name: "cust1"}, a: 0, gid: 0},  // dup key error for outColl1
        {_id: 4, customer: {name: "cust2"}, a: 4, gid: 1},  // goes to outColl2
        {_id: 5, customer: {name: "cust1"}, a: 5, gid: 0},  // goes to outColl1
        {_id: 6, customer: {name: "cust2"}, a: 6, gid: 1},  // goes to outColl2
        {_id: 7, customer: {name: "cust1"}, a: 5, gid: 0},  // dup key error for outColl1
        {_id: 8, customer: {name: "cust1"}, a: 8, gid: 0},  // goes to outColl1
        {_id: 9, customer: {name: "cust2"}, a: 4},  // missing != 0, dup key error for outColl2
    ]);

    // Verify that two more documents are inserted into outColl1.
    assert.soon(() => {
        jsTestLog(tojson(outColl1.find().toArray().map((doc) => sanitizeDoc(doc))));
        return outColl1.find().itcount() == 3;
    });
    assert.sameMembers(
        [
            {_id: 0, customer: {name: "cust1"}, a: 0, gid: 0},
            {_id: 5, customer: {name: "cust1"}, a: 5, gid: 0},
            {_id: 8, customer: {name: "cust1"}, a: 8, gid: 0},
        ],
        outColl1.find().toArray().map((doc) => sanitizeDoc(doc)));

    // Verify that three more documents are inserted into outColl2.
    assert.soon(() => {
        jsTestLog(tojson(outColl2.find().toArray().map((doc) => sanitizeDoc(doc))));
        return outColl2.find().itcount() == 4;
    });
    assert.sameMembers(
        [
            {_id: 1, customer: {name: "cust2"}, a: 1, gid: 1},
            {_id: 2, customer: {name: "cust2"}, a: 2},
            {_id: 4, customer: {name: "cust2"}, a: 4, gid: 1},
            {_id: 6, customer: {name: "cust2"}, a: 6, gid: 1},
        ],
        outColl2.find().toArray().map((doc) => sanitizeDoc(doc)));

    // Verify that the 3 documents that caused duplicate key errors are in the DLQ.
    assert.soon(() => {
        jsTestLog(tojson(dlqColl.find().toArray()));
        return dlqColl.find().itcount() == 3;
    });
    let res = dlqColl.find({"errInfo.reason": /a_1 dup key/}).toArray();
    assert.eq(3, res.length, `DLQ contents: ${tojson(dlqColl.find().toArray())}`);
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 3);

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testEvaluationFailure() {
    jsTestLog("Running testEvaluationFailure");

    const outColl1 = db.getSiblingDB('cust1').group_0;
    outColl1.drop();
    const outColl2 = db.getSiblingDB('cust2').group_1;
    outColl2.drop();
    dlqColl.drop();

    // Start a stream processor with dynamic 'db' & 'coll' name expressions.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {
                    connectionName: 'db1',
                    db: '$customer.name',
                    coll: {$concat: ['group_', {$toString: '$gid'}]}
                },
                whenMatched: 'keepExisting',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // Insert 2 documents into the stream. One document has gid: 0 and the other has no gid but
    // ggid. The collection name expression will fail to be evaluated for the second document.
    insertDocs(spName, [
        {_id: 0, customer: {name: "cust1"}, a: 0, gid: 0},
        {_id: 1, customer: {name: "cust2"}, a: 1, ggid: 1}
    ]);

    // One document should go to outColl1.
    assert.soon(() => { return outColl1.find().itcount() == 1; });
    jsTestLog(tojson(outColl1.find().toArray().map((doc) => sanitizeDoc(doc))));
    jsTestLog(tojson(outColl2.find().toArray().map((doc) => sanitizeDoc(doc))));

    assert.soon(() => { return dlqColl.find().itcount() == 1; });
    let res = dlqColl.find({"errInfo.reason": /evaluate target namespace/}).toArray();
    assert.eq(1, res.length, `DLQ contents: ${tojson(dlqColl.find().toArray())}`);
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 1);

    const spStatus = listStreamProcessors();
    assert.eq("running", spStatus.streamProcessors[0].status, tojson(spStatus));

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testManyDynamicCollections() {
    const testCases = [0, 100];

    testCases.forEach((numDynamicTargets) => {
        jsTestLog("Running testManyDynamicCollections " + numDynamicTargets);

        const nColls = numDynamicTargets;

        jsTestLog("The number of collections is " + nColls);

        let colls = [];
        for (let i = 0; i < nColls; i++) {
            const coll = db.getCollection("cust" + i);
            coll.drop();
            colls.push(coll);
        }
        dlqColl.drop();

        // Start a stream processor with dynamic 'coll' name expression.
        startStreamProcessor([
            {$source: {'connectionName': '__testMemory'}},
            {
                $merge: {
                    into: {
                        connectionName: 'db1',
                        db: "test",
                        coll: {$concat: ["cust", {$toString: "$a"}]},
                    },
                    whenMatched: 'keepExisting',
                    whenNotMatched: 'insert'
                }
            }
        ]);

        let docs = [];
        for (let i = 0; i < nColls; i++) {
            docs.push({_id: i, a: i});
        }
        // Insert all documents into the stream. Each documents has a different value for field 'a'
        // and so will go to a different collection.
        insertDocs(spName, docs);

        colls.forEach((coll) => {
            assert.soon(() => {
                const res = coll.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`coll ${coll.getName()} contents: ${tojson(res)}`);
                if (res.length != 1) {
                    return false;
                }

                // Documents should be routed by the value of field 'a'. Bails out early if we find
                // any mismatch.
                assert.eq(coll.getName(), "cust" + res[0].a);
                return true;
            });
        });

        // Stop the streamProcessor.
        stopStreamProcessor(spName);
    });
})();

// The max number of dynamic targets allowed is 1000.
const kMaxDynamicTargets = 1000;

(function testTooManyDynamicDbs() {
    jsTestLog("Running testTooManyDynamicDbs");

    for (let i = 0; i < kMaxDynamicTargets + 1; i++) {
        db.getSiblingDB("cust" + i).dropDatabase();
    }
    dlqColl.drop();

    // Start a stream processor with dynamic 'db' name expression.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {
                    connectionName: 'db1',
                    db: {$concat: ["cust", {$toString: "$a"}]},
                    coll: "coll"
                },
                whenMatched: 'keepExisting',
                whenNotMatched: 'insert'
            }
        }
    ]);

    let docs = [];
    for (let i = 0; i < kMaxDynamicTargets + 1; i++) {
        docs.push({_id: i, a: i});
    }
    // Insert all documents into the stream. Each documents has a different value for field 'a' and
    // so will go to a different database.
    insertDocs(spName, docs);

    // The sp should fail when the number of unique databases exceeds 'kMaxDynamicTargets'.
    assert.soon(() => {
        let result = listStreamProcessors();
        let sp = result.streamProcessors.find((sp) => sp.name == spName);
        jsTestLog(`${spName} status - \n${tojson(sp)}`);
        // 8143705 is the error code for "Too many unique databases". The error code is translated
        // by the executor to 75384.
        return sp.status == "error" &&
            sp.error.code == ErrorCodes.StreamProcessorTooManyOutputTargets &&
            sp.error.reason == "Too many unique databases: 1000";
    });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testBadUri() {
    jsTestLog("Running testBadUri");

    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    const result = startStreamProcessor(
        [
            {$source: {'connectionName': '__testMemory'}},
            {
                $merge: {
                    into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                    whenMatched: 'keepExisting',
                    whenNotMatched: 'insert'
                }
            }
        ],
        badUri,
        false /* validateSuccess */);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorAtlasConnectionError);
})();

// Tests $merge.on, including some cases that shouldn't work, when the specified on fields don't
// have unique indexes.
(function testMergeOnField() {
    // A test that validates a valid $merge stream processor starts as expected.
    let good = (setup, merge) => {
        outColl.drop();
        dlqColl.drop();
        if (setup != null) {
            setup();
        }
        startStreamProcessor([{$source: {'connectionName': '__testMemory'}}, merge]);
        stopStreamProcessor(spName);
    };

    // A test that validates a bad $merge fails to start with the expected error.
    let bad = (setup, merge) => {
        outColl.drop();
        dlqColl.drop();
        if (setup != null) {
            setup();
        }
        let result = startStreamProcessor([{$source: {'connectionName': '__testMemory'}}, merge],
                                          goodUri,
                                          false /* validateSuccess */);
        assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorInvalidOptions);
        jsTestLog(result);
        assert.eq(result.errorLabels[0], "StreamProcessorUserError");
        assert.eq(
            result.errmsg,
            "Error occured while validating $merge.on :: caused by :: Cannot find index to verify that join fields will be unique");
    };

    good(null /* setup */, {
        $merge: {
            into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
        }
    });
    good(null, {
        $merge: {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}, on: ["_id"]}
    });
    good(() => { db.createCollection("output_coll"); }, {
        $merge: {
            into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
        }
    });
    good(() => { db.createCollection("output_coll"); }, {
        $merge: {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}, on: ["_id"]}
    });
    good(
        () => {
            assert.commandWorked(outColl.createIndex({b: 1}, {unique: true}));
            assert.commandWorked(outColl.createIndex({a: 1, _id: 1}, {unique: true}));
        },
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()},
                on: ["_id", "a"]
            },
        },
    );

    bad(null,
        {$merge: {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}, on: ["a"]}});
    bad(null, {
        $merge:
            {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}, on: ["_id", "a"]}
    });
    bad(() => { assert.commandWorked(outColl.createIndex({b: 1}, {unique: true})); }, {
        $merge:
            {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}, on: ["_id", "a"]},
    });
})();

(function testLargeDocumentMerge() {
    jsTestLog("Running testLargeDocumentMerge");

    outColl.drop();
    dlqColl.drop();
    inputColl.drop();

    // Start a stream processor with dynamic 'db' name expression.
    startStreamProcessor([
        {
            $source: {
                connectionName: 'db1',
                db: 'test',
                coll: inputColl.getName(),
                timeField: '$ts',
                config: {fullDocument: 'required', fullDocumentOnly: true}
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(10), unit: 'second'},
                allowedLateness: NumberInt(0),
                pipeline: [
                    { $project: { docSize: 1, seed: 1, ts: 1, value: { $range: [ 0, "$docCount" ] } } },
                    { $unwind: "$value" },
                    { $project: { seed: 1, ts: 1, bigValue: { $range: [0, "$docSize"] }}},
                    { $project: { bigStr: { $reduce: { input: "$bigValue", initialValue: ".", in: {"$concat": [ "$$value", "$seed" ]}}}, ts: 1}},
                    { $group: { _id: "$_id", bigArr: {$push: "$bigStr"}, ts: {$max: "$ts"}}}
                ]
            }
        },
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}
            }
        }
    ]);

    assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

    // Seed of size 16KB.
    const seed = Array(16 * 1024).toString();
    // Accumulate size of ~1MB.
    inputColl.insert(
        {_id: 1, ts: ISODate("2024-03-01T01:00:00.000Z"), docCount: 8, docSize: 8, seed: seed});
    // Accumulate size of >16MB, this should result in dlq.
    inputColl.insert(
        {_id: 2, ts: ISODate("2024-03-01T01:00:00.000Z"), docCount: 16, docSize: 65, seed: seed});
    // Close the window.
    inputColl.insert(
        {_id: 4, ts: ISODate("2024-03-01T05:00:00.000Z"), docCount: 1, docSize: 1, seed: seed});

    assert.soon(() => {
        let result = dlqColl.find({}).toArray();
        return result.some(doc => { return doc.errInfo.reason.includes("BSONObjectTooLarge"); });
    });
    assert.soon(() => { return outColl.count() == 1; });
    assert.soon(() => { return dlqColl.find({"operatorName": "MergeOperator"}).itcount() == 1; });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testMergeLarge16MBDocument() {
    jsTestLog("Running testMergeLarge16MBDocument");

    inputColl.drop();
    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {'$source': {'connectionName': 'db1', 'db': 'test', 'coll': inputColl.getName()}},
        {"$replaceRoot": {"newRoot": "$fullDocument"}},
        {"$unset": ["_stream_meta", "ts", "_id"]},
        {$merge: {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}}}
    ]);

    // Leave space for timeField and _id.
    const seed = Array(16 * 1024 * 1024 - 44).toString();
    inputColl.insert({_id: 1, ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed});
    assert.eq(inputColl.count(), 1);
    assert.soon(() => { return outColl.find().itcount() == 1; });
    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testMergeLarge16MBDocumentDlq() {
    jsTestLog("Running testMergeLarge16MBDocumentDlq");

    inputColl.drop();
    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {'$source': {'connectionName': 'db1', 'db': 'test', 'coll': inputColl.getName()}},
        {"$replaceRoot": {"newRoot": "$fullDocument"}},
        {$merge: {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}}}
    ]);

    // Leave space for timeField and _id.
    const seed = Array(16 * 1024 * 1024 - 44).toString();
    inputColl.insert({_id: 1, ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed});
    assert.eq(inputColl.count(), 1);

    assert.soon(() => {
        let result = dlqColl.find({}).toArray();
        return result.some(doc => {
            return doc.errInfo.reason.includes("Document to upsert is larger than 16777216");
        });
    });
    assert.eq(dlqColl.count(), 1);
    assert.soon(() => { return dlqColl.find({"operatorName": "MergeOperator"}).itcount() == 1; });
    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

// This test validates the 12MB batch size by inserting batches of documents to input collection.
(function testMergeDocumentBatchBoundary() {
    jsTestLog("Running testMergeDocumentBatchBoundary");

    inputColl.drop();
    outColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {'$source': {'connectionName': 'db1', 'db': 'test', 'coll': inputColl.getName()}},
        {"$replaceRoot": {"newRoot": "$fullDocument"}},
        {"$unset": ["_stream_meta", "ts", "_id"]},
        {$merge: {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}}}
    ]);

    const seed4 = Array(4 * 1024 * 1024).toString();
    const seed3 = Array(3 * 1024 * 1024).toString();
    const seed8 = Array(8 * 1024 * 1024).toString();
    const seed12 = Array(12 * 1024 * 1024).toString();

    inputColl.insert([
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed4},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed3},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed4},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed8}
    ]);
    assert.eq(inputColl.count(), 4);
    assert.soon(() => { return outColl.find().itcount() == 4; });

    inputColl.insert([
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed8},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed4},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed4},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed8}
    ]);
    assert.eq(inputColl.count(), 8);
    assert.soon(() => { return outColl.count() == 8; });

    inputColl.insert([
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed8},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed12},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed3},
        {ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed12}
    ]);
    assert.eq(inputColl.count(), 12);
    assert.soon(() => { return outColl.count() == 12; });

    // Stop the streamProcessor.
    stopStreamProcessor(spName);
})();

(function testMergeBatchIndexForBadDocuments() {
    jsTestLog("Running testMergeBatchIndexForBadDocuments");

    inputColl.drop();
    outColl.drop();
    dlqColl.drop();

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));
    // Start a stream processor.
    startStreamProcessor([
        {
            $source: {
                connectionName: 'db1',
                db: 'test',
                coll: inputColl.getName(),
                timeField: '$ts',
                config: {fullDocument: 'required', fullDocumentOnly: true}
            }
        },
        {$merge: {into: {connectionName: 'db1', db: 'test', coll: outColl.getName()}, on: 'a'}}
    ]);

    // Insert a batch of 8 documents, where 2 documents will result in a DBException
    // during the batch creation and 1 documents will fail during the flush.
    inputColl.insert([
        {_id: 1, a: 2, ts: ISODate("2024-03-01T01:00:01.000Z")},
        {_id: 2, a: 3, ts: ISODate("2024-03-01T01:00:01.000Z")},
        {_id: 3, a: 2, ts: ISODate("2024-03-01T01:00:01.000Z")},
        {_id: 4, a: [3, 4], ts: ISODate("2024-03-01T01:00:01.000Z")},
        {_id: 5, a: 5, ts: ISODate("2024-03-01T01:00:01.000Z")},
        {_id: 6, a: 7, ts: ISODate("2024-03-01T01:00:01.000Z")},
        {_id: 7, a: [8], ts: ISODate("2024-03-01T01:00:01.000Z")},
        {_id: 8, a: 9, ts: ISODate("2024-03-01T01:00:01.000Z")},
    ]);

    assert.soon(() => { return outColl.find().itcount() == 5; });
    assert.soon(() => { return dlqColl.find().itcount() == 3; });

    const result = dlqColl.find().toArray();
    assert.eq(
        result
            .filter(doc => doc["errInfo"]["reason"].includes("$merge write error: 'on' field 'a'"))
            .length,
        2);
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 3);
    stopStreamProcessor(spName);
})();

// Cleanup the output collection and DLQ.
outColl.drop();
dlqColl.drop();
inputColl.drop();
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
}());
