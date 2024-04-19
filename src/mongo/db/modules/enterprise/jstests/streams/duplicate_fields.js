import {sequentialIds} from "jstests/query_golden/libs/example_data.js";
import {
    dbName,
    dlqCollName,
    generate16MBDoc,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "mergeOperatorTest";

// This function is trying to test adding another field to a large document would cause it to DLQ
// but is blocked by STREAMS-733
const simpleMergeFunc = function testMergeFunc(docs, expectedResults) {
    const docsWithIds = sequentialIds(docs);
    runStreamProcessorOperatorTest({
        pipeline: [{$set: {b: "$a0"}}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docsWithIds);
            assert.soon(() => {
                return outColl.find().itcount() >= expectedResults.length ||
                    db.getSiblingDB(dbName)[dlqCollName].find().itcount() +
                        outColl.find().itcount() ==
                    expectedResults.length;
            }, logState());
            var fieldNames = ['_ts', '_stream_meta', '_id'];
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc, fieldNames));
        }
    });
};

var testDoc = {};
const tmp = 1;
testDoc["a" + tmp] = 1;
simpleMergeFunc([testDoc], [{a: 1, b: 1}]);

// verify that > 16MB doc produces a DLQ
const doc = generate16MBDoc();
simpleMergeFunc([doc], [doc]);
assert.eq(db.getSiblingDB(dbName)[dlqCollName].find().itcount(), 1);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);