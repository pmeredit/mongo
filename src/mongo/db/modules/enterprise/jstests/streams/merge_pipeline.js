/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    connectionName,
    dbName,
    insertDocs,
    listStreamProcessors,
    logState,
    outCollName,
    runTest,
    sanitizeDoc,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

(function() {
"use strict";

const outColl = db.getSiblingDB(dbName)[outCollName];
const spName = "mergePipelineTest";

/**
 * Makes a merge pipeline with the given 'mergePipeline' and 'vars'. The 'vars' are optional and
 * are used to specify let variables for the merge pipeline. The input source is the test-only
 * in-memory source. The output is the 'outColl' collection. 'whenNotMatched' is 'insert'. 'on'
 * field is not specified and so it defaults to '_id'.
 */
function makeMergePipeline({vars, mergePipeline}) {
    let mergeSpec = {
        $merge: {
            into: {connectionName: connectionName, db: dbName, coll: outColl.getName()},
            whenMatched: mergePipeline,
            whenNotMatched: 'insert'
        }
    };
    if (vars) {
        mergeSpec["$merge"]["let"] = vars;
    }

    const pipeline = [
        {$source: {connectionName: "__testMemory"}},
        mergeSpec,
    ];
    return pipeline;
}

(function testMergeSetPipelineInsertMode() {
    const docs = [{_id: 0, a: 0}, {_id: 1, a: 1}];
    runTest({
        spName: spName,
        // Adds 'x' field by accessing the matched document's 'a' field.
        pipeline: makeMergePipeline({mergePipeline: [{$set: {x: {$add: ["$a", 10]}}}]}),
        iteration: 2,
        verifyActions: [
            () => {
                // Inserts 2 documents into the stream. Each document has '_id' field. The 'outColl'
                // is empty and so the 2 documents should get inserted into the 'outColl'.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find().itcount() == 2; }, logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`1st result -\n${tojson(results)}`);
                assert.sameMembers(docs, results);
            },
            () => {
                // Inserts the same documents again. This time, two documents should be matched and
                // updated by the pipeline.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find({x: {$gte: 10}}).itcount() == 2; },
                            logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`2nd result -\n${tojson(results)}`);
                assert.sameMembers([{_id: 0, a: 0, x: 10}, {_id: 1, a: 1, x: 11}], results);
            }
        ]
    });
})();

(function testMergeSetAndUnsetPipelineInsertMode() {
    const docs = [{_id: 0, a: 0, b: 10}, {_id: 1, a: 1, b: 11}];
    runTest({
        spName: spName,
        // Adds 'x' field and removes 'b' field.
        pipeline:
            makeMergePipeline({mergePipeline: [{$set: {x: {$add: ["$a", 10]}}}, {$unset: "b"}]}),
        iteration: 2,
        verifyActions: [
            () => {
                // Inserts 2 documents into the stream. Each document has '_id' field. The 'outColl'
                // is empty and so the 2 documents should get inserted into the 'outColl'.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find().itcount() == 2; }, logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`1st result -\n${tojson(results)}`);
                assert.sameMembers(docs, results);
            },
            () => {
                // Inserts the same documents again. This time, two documents should be matched and
                // updated by the pipeline.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find({x: {$gte: 10}}).itcount() == 2; },
                            logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`2nd result -\n${tojson(results)}`);
                assert.sameMembers([{_id: 0, a: 0, x: 10}, {_id: 1, a: 1, x: 11}], results);
            }
        ]
    });
})();

(function testMergeImplicitVarPipelineInsertMode() {
    const docs = [{_id: 0, a: 0}, {_id: 1, a: 1}];
    let firstRawResults;
    runTest({
        spName: spName,
        // Uses the implicit variable '$$new' to update the '_ts' field.
        pipeline:
            makeMergePipeline({mergePipeline: [{$set: {x: {$add: ["$a", 10]}, _ts: "$$new._ts"}}]}),
        iteration: 2,
        verifyActions: [
            () => {
                // Inserts 2 documents into the stream. Each document has '_id' field. The 'outColl'
                // is empty and so the 2 documents should get inserted into the 'outColl'.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find().itcount() == 2; }, logState(spName));

                firstRawResults = outColl.find().toArray();
                jsTestLog(`1st raw result -\n${tojson(firstRawResults)}`);
                let results = firstRawResults.map((doc) => sanitizeDoc(doc));
                jsTestLog(`1st result -\n${tojson(results)}`);
                assert.sameMembers(docs, results);
            },
            () => {
                // Inserts the same documents again. This time, two documents should be matched and
                // updated by the pipeline.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find({x: {$gte: 10}}).itcount() == 2; },
                            logState(spName));

                let rawResults = outColl.find().toArray();
                jsTestLog(`2nd raw result -\n${tojson(rawResults)}`);
                let results = rawResults.map((doc) => sanitizeDoc(doc));
                jsTestLog(`2nd result -\n${tojson(results)}`);
                assert.sameMembers([{_id: 0, a: 0, x: 10}, {_id: 1, a: 1, x: 11}], results);

                for (let i = 0; i < 2; i++) {
                    assert.neq(firstRawResults[i]._ts, rawResults[i]._ts, tojson(rawResults));
                }
            }
        ]
    });
})();

(function testMergeExplicitVarPipelineInsertMode() {
    const docs = [{_id: 0, a: 0}, {_id: 1, a: 1}];
    runTest({
        spName: spName,
        // Uses the explicit variable '$$addendum' to add the 'x' field.
        pipeline: makeMergePipeline(
            {vars: {addendum: 10}, mergePipeline: [{$set: {x: {$add: ["$a", "$$addendum"]}}}]}),
        iteration: 2,
        verifyActions: [
            () => {
                // Inserts 2 documents into the stream. Each document has '_id' field. The 'outColl'
                // is empty and so the 2 documents should get inserted into the 'outColl'.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find().itcount() == 2; }, logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`1st result -\n${tojson(results)}`);
                assert.sameMembers(docs, results);
            },
            () => {
                // Inserts the same documents again. This time, two documents should be matched and
                // updated by the pipeline.
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find({x: {$gte: 10}}).itcount() == 2; },
                            logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`2nd result -\n${tojson(results)}`);
                assert.sameMembers([{_id: 0, a: 0, x: 10}, {_id: 1, a: 1, x: 11}], results);
            }
        ]
    });
})();

(function testMergeReplaceWithPipelineInsertMode() {
    runTest({
        spName: spName,
        // Uses $replaceWith to replace the document with the new document.
        pipeline: makeMergePipeline({mergePipeline: [{$replaceWith: "$$new"}]}),
        iteration: 2,
        verifyActions: [
            () => {
                // Inserts 2 documents into the stream. Each document has '_id' field. The 'outColl'
                // is empty and so the 2 documents should get inserted into the 'outColl'.
                const docs = [{_id: 0, a: 0}, {_id: 1, a: 1}];
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find().itcount() == 2; }, logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`1st result -\n${tojson(results)}`);
                assert.sameMembers(docs, results);
            },
            () => {
                // Inserts different documents but with the same '_id's. This time, two documents
                // should be matched and replaced by the new documents.
                const docs = [{_id: 0, a: 10, b: 0}, {_id: 1, a: 11, b: 1}];
                insertDocs(spName, docs);

                assert.soon(() => { return outColl.find({a: {$gte: 10}}).itcount() == 2; },
                            logState(spName));

                let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
                jsTestLog(`2nd result -\n${tojson(results)}`);
                assert.sameMembers(docs, results);
            }
        ]
    });
})();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
}());
