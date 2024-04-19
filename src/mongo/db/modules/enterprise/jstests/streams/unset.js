import {
    dbName,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "unsetTest";

const unsetFunc = function(docs, replaceWithString, expectedResults) {
    const pipeline = [{$project: replaceWithString}];
    runStreamProcessorOperatorTest({
        pipeline: [{$unset: replaceWithString}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState());
            let results =
                outColl.find().toArray().map((doc) => sanitizeDoc(doc, ['_ts', '_stream_meta']));
            assert.eq(expectedResults, results);
        }
    });
};

const docs = [{_id: 0, a: 10}, {_id: 1, a: {b: 20, c: 30, 0: 40}}, {_id: 2, a: [{b: 50, c: 60}]}];
const expected = [{_id: 0}, {_id: 1}, {_id: 2}];
unsetFunc(docs, "a", expected);
unsetFunc(docs, ["a"], expected);
// unset with non-existent field
unsetFunc(docs, ["a", "b"], expected);

// unset with dotted field path.
unsetFunc(docs, "a.b", [{_id: 0, a: 10}, {_id: 1, a: {0: 40, c: 30}}, {_id: 2, a: [{c: 60}]}]);

// Numeric field paths in aggregation represent field name only and not array offset.
unsetFunc(
    docs, ["a.0"], [{_id: 0, a: 10}, {_id: 1, a: {b: 20, c: 30}}, {_id: 2, a: [{b: 50, c: 60}]}]);

// multiple unsets
unsetFunc(docs, ["a.0", "a.b"], [{_id: 0, a: 10}, {_id: 1, a: {c: 30}}, {_id: 2, a: [{c: 60}]}]);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);