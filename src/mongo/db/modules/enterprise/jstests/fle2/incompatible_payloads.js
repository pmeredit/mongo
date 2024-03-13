/**
 * Test v1 payload formats are rejected by the server running the v2 protocol.
 *
 * @tags: [
 * requires_fcv_80
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {
                "path": "rating",
                "bsonType": "int",
                "queries":
                    {"queryType": "range", "min": NumberInt(1), "max": NumberInt(2), "sparsity": 1}
            },
            {"path": "unindexed", "bsonType": "string"}
        ]
    }
}));

const edb = client.getDB();

const collInfos = edb.getCollectionInfos({name: "basic"});
assert.eq(collInfos.length, 1);
assert(collInfos[0].options.encryptedFields !== undefined);
const encryptionInfo = {
    schema: {"basic.basic": collInfos[0].options.encryptedFields}
};

const v1InsertUpdatePayload = BinData(
    6,
    "BG0BAAAFZAAgAAAAAHnDBV5FStmAkk8tgPxNo2hTxbb33wGYzQJ8YMqELGH6BXMAIAAAAADbfVEMJuTZJPz3Qq/dfv" +
        "F9uQSMpodTxLYoZi/j5cKYAAVjACAAAAAA8I4AQMHmlL3LCPWvvebUiBFE5t8w0Byjk2diEm1GKWUFcABQAAAAAIoZ" +
        "VhWUr7d4jzicsyssyBqwIrDnPP2zFQFY1PFv85X2ym+9wJWl1+qXv5UAu9sJMypUpBKB7K3UFrd3tf6wtJO01i2ftU" +
        "XPzMdMDy1uAJX0BXUAEAAAAASlB971JulEr4Acrs+WGJVlEHQAAgAAAAV2AEkAAAAApQfe9SbpRK+AHK7PlhiVZUr8" +
        "ZW7uHLde1kfqFaIsLYdRqoKKvautxfb8p+mhvbDOSKWsY6sSJWHZ1j3o4G2ci2KwEst0K5RRrwVlACAAAAAAiWO9wO" +
        "baCq1eH6pdvNtXax5UJdQPyO7L4R8xxxKOTFwA");
const v1UnindexedPayload = BinData(
    6,
    "Bj98HBPgqEwVonjMXp+EbLwCtuK2EXpjVcrl/2/6sSgtXE2Ubyq4KkU6XMCBDeJ2q6HQNtgEKHvTNqsp7rcxZDgpNH61qI9V2dE=");
const v1FindEqualityPayload = BinData(
    6,
    "BbEAAAAFZAAgAAAAACoHme5RnctV9kJcBlLhuRkmFoUCR2EMWTS/NwTcRALUBXMAIAAAAAAHkawU6xGYztV3h30Q1A" +
        "BdEY7o+rmyZIfB2ng8838u4AVjACAAAAAAFHEmCfQWVcRgKnL+Y7u/u9/5dQyaQSLSbeGp4auL000FZQAgAAAAALVE" +
        "SvKWp41m2canTKfnm4rmoRMwMPEcyj9YuAkVDksCEmNtAAQAAAAAAAAAAA==");
const v1FindRangePayload = BinData(
    6,
    "CvEAAAADcGF5bG9hZADBAAAABGcAhQAAAAMwAH0AAAAFZAAgAAAAAM2AuByUc3KeCcIBhbbGTQ+yjEwcLtinSfhhwX" +
        "C+GGCQBXMAIAAAAAD1NAGKC+euJxrgEXD7o+pRN068yJFssTxSmbhK6mAyAAVjACAAAAAAMXr0229Ozfm4b/MEiH4p" +
        "o0OxyPpDrzKCUvudiZjM1esAAAVlACAAAAAAqyE4josUK9EsXiPJFSfpB+Q+8JpoRwXpJHGUZcbqHfsSY20ABAAAAA" +
        "AAAAAAEHBheWxvYWRJZAAAAAAAEGZpcnN0T3BlcmF0b3IAAQAAAAA=");

// ---------------------------------------
// Insert Tests
// - with unencrypted client, insert a canned v1 insert update payload and
//   a canned v1 unindexed value
// ---------------------------------------
//
// Test v1 indexed insert
//
jsTestLog("Testing v1 indexed insert fails");
let res = dbTest.runCommand({
    "insert": "basic",
    documents: [{
        "_id": 1,
        "first": v1InsertUpdatePayload,
    }],
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7291901);

//
// Test v1 unindexed insert
//
jsTestLog("Testing v1 unindexed insert fails");
res = dbTest.runCommand({
    "insert": "basic",
    documents: [{
        "_id": 1,
        "unindexed": v1UnindexedPayload,
    }],
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7413901);

// ---------------------------------------
// Find/Count/Aggregate Tests
// - with unencrypted client, query using canned v1 indexed/unindexed payloads
// ---------------------------------------
const queryTests = [
    {
        title: "Test v1 equality indexed value in find query",
        filter: {first: v1FindEqualityPayload},
        result: 7292602
    },
    {
        title: "Test v1 range indexed value in find query",
        filter: {rating: {$gt: v1FindRangePayload}},
        result: 7292602
    },
    {
        title: "Test v1 unindexed value in find query",
        filter: {unindexed: v1UnindexedPayload},
        result: 7292602
    }
];

// Test finds
for (const test of queryTests) {
    jsTestLog(test.title);
    res = dbTest.runCommand(
        {find: "basic", filter: test.filter, encryptionInformation: encryptionInfo});
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));
}

// Test counts
for (const test of queryTests) {
    jsTestLog(test.title.replace('find', 'count'));
    res = dbTest.runCommand(
        {count: "basic", query: test.filter, encryptionInformation: encryptionInfo});
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));
}

// Test aggregates
for (const test of queryTests) {
    jsTestLog(test.title.replace('find', 'aggregate'));
    res = dbTest.runCommand({
        aggregate: "basic",
        pipeline: [{$match: test.filter}],
        cursor: {},
        encryptionInformation: encryptionInfo
    });
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));
}

// ---------------------------------------
// Delete Tests
// - with unencrypted client, query using canned v1 indexed/unindexed payloads
// ---------------------------------------
for (const test of queryTests) {
    jsTestLog(test.title.replace('find', 'delete'));
    res = dbTest.runCommand({
        delete: "basic",
        deletes: [{q: test.filter, limit: 1}],
        encryptionInformation: encryptionInfo
    });
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));
}

// ---------------------------------------
// Update Tests
// - with unencrypted client, query using canned v1 indexed/unindexed payloads
//   and update using canned v1 insert update payload or v1 unindexed value
// ---------------------------------------
const updateTests = [
    {
        title: "Test v1 indexed value in update $set",
        opEntry: {q: {last: "pe"}, u: {$set: {first: v1InsertUpdatePayload}}},
        result: 7291901
    },
    {
        title: "Test v1 indexed value in replacement-style update",
        opEntry: {q: {last: "pe"}, u: {first: v1InsertUpdatePayload}},
        result: 7291901
    },
    {
        title: "Test v1 unindexed value in update $set",
        opEntry: {q: {last: "pe"}, u: {$set: {unindexed: v1UnindexedPayload}}},
        result: 7413901
    },
    {
        title: "Test v1 unindexed value in replacement-style update",
        opEntry: {q: {last: "pe"}, u: {first: v1UnindexedPayload}},
        result: 7413901
    },
    {
        title: "Test v1 equality encrypted value in update query",
        opEntry: {q: {first: v1FindEqualityPayload}, u: {$set: {last: "pe"}}},
        result: 7292602
    },
    {
        title: "Test v1 range encrypted value in update query",
        opEntry: {q: {rating: {$gt: v1FindRangePayload}}, u: {last: "pe"}},
        result: 7292602
    },
    {
        title: "Test v1 unindexed value in update query (equality)",
        opEntry: {q: {unindexed: v1UnindexedPayload}, u: {$set: {last: "pe"}}},
        result: 7292602
    },
    {
        title: "Test v1 unindexed value in update query (range)",
        opEntry: {q: {rating: {$lt: v1UnindexedPayload}}, u: {last: "pe"}},
        result: 7292602
    }
];

for (const test of updateTests) {
    jsTestLog(test.title);
    res = dbTest.runCommand(
        {update: "basic", updates: [test.opEntry], encryptionInformation: encryptionInfo});
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));
}

// ---------------------------------------
// Upsert Tests
// ---------------------------------------
for (const test of updateTests) {
    test.opEntry.upsert = true;
    jsTestLog(test.title.replace('update', 'upsert'));
    res = dbTest.runCommand(
        {update: "basic", updates: [test.opEntry], encryptionInformation: encryptionInfo});
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));
    delete test.opEntry.upsert;
}

// ---------------------------------------
// FindAndModify Tests
// ---------------------------------------
for (const test of updateTests) {
    jsTestLog(test.title.replace('update', 'findAndModify'));
    res = dbTest.runCommand({
        findAndModify: "basic",
        query: test.opEntry.q,
        update: test.opEntry.u,
        encryptionInformation: encryptionInfo
    });
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));

    // upsert variant
    res = dbTest.runCommand({
        findAndModify: "basic",
        query: test.opEntry.q,
        update: test.opEntry.u,
        upsert: true,
        encryptionInformation: encryptionInfo
    });
    assert.commandFailedWithCode(
        res, test.result, "Failed on upsert variant of test: " + tojson(test));
}

// ---------------------------------------
// FindAndModify Remove Tests
// ---------------------------------------
for (const test of queryTests) {
    jsTestLog(test.title.replace('find', 'findAndModify remove'));
    res = dbTest.runCommand({
        findAndModify: "basic",
        query: test.filter,
        remove: true,
        encryptionInformation: encryptionInfo
    });
    assert.commandFailedWithCode(res, test.result, "Failed on test: " + tojson(test));
}
