import {
    dbName,
    generate16MBDoc,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "replaceWithOperatorTest";
const coll = db.project_coll;
const replaceWithFunc = function(docs, replaceWithString, expectedResults) {
    const pipeline = [{$project: replaceWithString}];
    runStreamProcessorOperatorTest({
        pipeline: [{$unwind: "$comments"}, {$replaceWith: replaceWithString}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map(
                (doc) => sanitizeDoc(doc, ['_ts', '_stream_meta', '_id']));
            assert.eq(expectedResults, results);
        }
    });
};

const docs = [
    {_id: 0, comments: [{user_id: "x", comment: "foo"}, {user_id: "y", comment: "bar"}]},
    {_id: 1, comments: [{user_id: "y", comment: "bar again"}]}
];

const expectedResults = [
    {"comment": "foo", "user_id": "x"},
    {"comment": "bar", "user_id": "y"},
    {"comment": "bar again", "user_id": "y"}
];

replaceWithFunc(docs, "$comments", expectedResults);

// tests are from documentation onsite for $replaceWith
const replaceWithFuncNoUnwind = function testReplaceWithNoUnwind(
    docs, replaceWithString, expectedResults) {
    const pipeline = [{$project: replaceWithString}];
    runStreamProcessorOperatorTest({
        pipeline: [{$replaceWith: replaceWithString}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map(
                (doc) => sanitizeDoc(doc, ['_ts', '_stream_meta', '_id']));
            assert.eq(expectedResults, results);
        }
    });
};

const docs2 = [
    {"_id": 1, "name": {"first": "John", "last": "Backus"}},
    {"_id": 2, "name": {"first": "John", "last": "McCarthy"}},
    {"_id": 3, "name": {"first": "Grace", "last": "Hopper"}},
];

const expectedResults2 = [
    {"first": "John", "last": "Backus"},
    {"first": "John", "last": "McCarthy"},
    {"first": "Grace", "last": "Hopper"}
];

replaceWithFuncNoUnwind(docs2, "$name", expectedResults2);

const docs3 = [
    {"_id": 1, "name": {"first": "John", "last": "Backus"}},
    {"_id": 2, "name": {"first": "John", "last": "McCarthy"}},
    {"_id": 3, "name": {"first": "Grace", "last": "Hopper"}},
    {"_id": 4, "firstname": "Ole-Johan", "lastname": "Dahl"},
];
replaceWithFuncNoUnwind(
    docs3, "$name", expectedResults2);  // one document without name will be dlqed.

const docs4 = [
    {"_id": 1, "name": "Arlene", "age": 34, "pets": {"dogs": 2, "cats": 1}},
    {"_id": 2, "name": "Sam", "age": 41, "pets": {"cats": 1, "fish": 3}},
    {"_id": 3, "name": "Maria", "age": 25}
];

const expectedResults4 = [
    {"birds": 0, "cats": 1, "dogs": 2, "fish": 0},
    {"birds": 0, "cats": 1, "dogs": 0, "fish": 3},
    {"birds": 0, "cats": 0, "dogs": 0, "fish": 0}
];

replaceWithFuncNoUnwind(
    docs4, {$mergeObjects: [{dogs: 0, cats: 0, birds: 0, fish: 0}, "$pets"]}, expectedResults4);
const doc = {
    _id: 1,
    a: generate16MBDoc()
};

// tests are from documentation onsite for $replaceWith
const replaceWithFuncProject = function testReplaceWithProject(
    docs, replaceWithString, expectedResults) {
    const pipeline = [{$project: replaceWithString}];
    runStreamProcessorOperatorTest({
        pipeline: [{$replaceWith: replaceWithString}, {$project: {_id: 1}}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map(
                (doc) => sanitizeDoc(doc, ['_ts', '_stream_meta', '_id']));
            assert.eq(expectedResults, results);
        }
    });
};

// large document replaceWith test
replaceWithFuncProject([doc], "$a", []);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);