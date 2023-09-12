/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {sanitizeDoc} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

(function() {
"use strict";

const outColl = db.output_coll;
const dlqColl = db.dlq_coll;

function startStreamProcessor(pipeline) {
    const uri = 'mongodb://' + db.getMongo().host;
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: 'tenant1',
        name: 'mergeTest',
        processorId: 'mergeTest1',
        pipeline: pipeline,
        connections: [
            {name: "db1", type: 'atlas', options: {uri: uri}},
            {name: '__testMemory', type: 'in_memory', options: {}},
        ],
        options: {dlq: {connectionName: "db1", db: "test", coll: dlqColl.getName()}}
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    assert.eq(result["ok"], 1);
}

function stopStreamProcessor() {
    let stopCmd = {
        streams_stopStreamProcessor: '',
        name: 'mergeTest',
    };
    let result = db.runCommand(stopCmd);
    assert.eq(result["ok"], 1);
}

function insertDocs(docs) {
    let insertCmd = {
        streams_testOnlyInsert: '',
        name: 'mergeTest',
        documents: docs,
    };

    let result = db.runCommand(insertCmd);
    jsTestLog(result);
    assert.eq(result["ok"], 1);
}

(function testKeepExistingInsertMode() {
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
    insertDocs([{_id: 0, a: 0}, {_id: 1, a: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents (4 good, 4 bad that violate unique constraint on field a) into the
    // stream.
    insertDocs([
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

    // Insert 3 more documents (2 good, 1 bad that violates unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'keepExisting' whenMatched mode
    // only the first one should get inserted.
    insertDocs([{_id: 10, a: 0}, {_id: 11, a: 11}, {_id: 11, a: 12}]);

    assert.soon(() => { return outColl.find().itcount() == 7; });
    assert.soon(() => { return dlqColl.find().itcount() == 5; });
    assert.eq([{_id: 11, a: 11}], outColl.find({_id: 11}).toArray().map(sanitizeDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testMergeInsertMode() {
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
    insertDocs([{_id: 0, a: 0}, {_id: 1, a: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents (4 good, 4 bad that violate unique constraint on field a) into the
    // stream.
    insertDocs([
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

    // Insert 3 more documents (2 good, 1 bad that violates unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'merge' whenMatched mode the 2
    // documents the should get merged.
    insertDocs([{_id: 10, a: 0}, {_id: 11, a: 11, obj: {a: 1}}, {_id: 11, obj: {b: 1}}]);

    assert.soon(() => { return outColl.find().itcount() == 7; });
    assert.soon(() => { return dlqColl.find().itcount() == 5; });
    assert.eq([{_id: 11, a: 11, obj: {b: 1}}], outColl.find({_id: 11}).toArray().map(sanitizeDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testReplaceInsertMode() {
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
    insertDocs([{_id: 0, a: 0}, {_id: 1, a: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents (4 good, 4 bad that violate unique constraint on field a) into the
    // stream.
    insertDocs([
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

    // Insert 3 more documents (2 good, 1 bad that violates unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'replace' whenMatched mode the
    // second document should overwrite the first one.
    insertDocs([{_id: 10, a: 0}, {_id: 11, a: 11, obj: {a: 1}}, {_id: 11, obj: {b: 1}}]);

    assert.soon(() => { return outColl.find().itcount() == 7; });
    assert.soon(() => { return dlqColl.find().itcount() == 5; });
    assert.eq([{_id: 11, obj: {b: 1}}], outColl.find({_id: 11}).toArray().map(sanitizeDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testReplaceDiscardMode() {
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
    insertDocs([{_id: 0, a: 0}, {_id: 1, a: 1}, {_id: 2, a: 2}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });
    assert.soon(() => { return dlqColl.find().itcount() == 0; });

    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

    // Insert 8 more documents into the stream.
    insertDocs([
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
    assert.soon(() => {
        return JSON.stringify([{_id: 0, a: 4}]) ==
            JSON.stringify(outColl.find({_id: 0}).toArray().map(sanitizeDoc));
    });
    assert.soon(() => {
        return JSON.stringify([{_id: 1, a: 2}]) ==
            JSON.stringify(outColl.find({_id: 1}).toArray().map(sanitizeDoc));
    });

    // Insert 3 more documents (2 good, 2 bad that violate unique constraint on field a) into the
    // stream. The 2 good documents have the same _id. Due to the 'replace' whenMatched mode the
    // second document should overwrite the first one.
    insertDocs(
        [{_id: 1, a: 4}, {_id: 0, a: 2}, {_id: 1, a: 11, obj: {a: 1}}, {_id: 1, obj: {b: 1}}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });
    assert.soon(() => { return dlqColl.find().itcount() == 6; });
    assert.soon(() => {
        return JSON.stringify([{_id: 0, a: 4}]) ==
            JSON.stringify(outColl.find({_id: 0}).toArray().map(sanitizeDoc));
    });
    assert.soon(() => {
        return JSON.stringify([{_id: 1, obj: {b: 1}}]) ==
            JSON.stringify(outColl.find({_id: 1}).toArray().map(sanitizeDoc));
    });

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testReplaceDiscardModeWithOnFields() {
    outColl.drop();
    dlqColl.drop();

    // Insert 2 documents into outColl.
    assert.commandWorked(outColl.insert([{x: 0, a: 0}, {x: 1, a: 1}]));
    assert.commandWorked(outColl.createIndex({a: 1}, {unique: true}));

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
    // - one does not contains the on field 'x' and is added to the dlq while creating the batch
    // - one violates unique constraint on field 'a' and is added to the dlq while the batch is
    //   getting flushed
    insertDocs([{x: 0, a: 0, b: 0}, {x: 1, a: 0}, {a: 0}, {x: 1, a: 1, b: 1}]);

    assert.soon(() => { return outColl.find().itcount() == 2; });
    assert.soon(() => {
        return JSON.stringify([{x: 0, a: 0, b: 0}]) ==
            JSON.stringify(outColl.find({a: 0}, {_id: 0}).toArray().map(sanitizeDoc));
    });
    assert.soon(() => {
        return JSON.stringify([{x: 1, a: 1, b: 1}]) ==
            JSON.stringify(outColl.find({a: 1}, {_id: 0}).toArray().map(sanitizeDoc));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 2; });

    // Stop the streamProcessor.
    stopStreamProcessor();
})();
}());
