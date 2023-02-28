/**
 * Test v1 payload formats are rejected by the server running the v2 protocol.
 *
 * @tags: [
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

// TODO: SERVER-73303 remove when v2 is enabled by default
if (!isFLE2ProtocolVersion2Enabled()) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is not enabled");
    return;
}

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
                "queries": {
                    "queryType": "rangePreview",
                    "min": NumberInt(1),
                    "max": NumberInt(2),
                    "sparsity": 1
                }
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

//
// Test v1 indexed insert
//
// with unencrypted client, try to insert a canned v1 insert update payload
jsTestLog("Testing v1 indexed insert fails");
let v1Payload = BinData(
    6,
    "BG0BAAAFZAAgAAAAAHnDBV5FStmAkk8tgPxNo2hTxbb33wGYzQJ8YMqELGH6BXMAIAAAAADbfVEMJuTZJPz3Qq/dfv" +
        "F9uQSMpodTxLYoZi/j5cKYAAVjACAAAAAA8I4AQMHmlL3LCPWvvebUiBFE5t8w0Byjk2diEm1GKWUFcABQAAAAAIoZ" +
        "VhWUr7d4jzicsyssyBqwIrDnPP2zFQFY1PFv85X2ym+9wJWl1+qXv5UAu9sJMypUpBKB7K3UFrd3tf6wtJO01i2ftU" +
        "XPzMdMDy1uAJX0BXUAEAAAAASlB971JulEr4Acrs+WGJVlEHQAAgAAAAV2AEkAAAAApQfe9SbpRK+AHK7PlhiVZUr8" +
        "ZW7uHLde1kfqFaIsLYdRqoKKvautxfb8p+mhvbDOSKWsY6sSJWHZ1j3o4G2ci2KwEst0K5RRrwVlACAAAAAAiWO9wO" +
        "baCq1eH6pdvNtXax5UJdQPyO7L4R8xxxKOTFwA");

let res = dbTest.runCommand({
    "insert": "basic",
    documents: [{
        "_id": 1,
        "first": v1Payload,
    }],
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7291901);

//
// Test v1 unindexed insert
//
jsTestLog("Testing v1 unindexed insert fails");
v1Payload = BinData(
    6,
    "Bj98HBPgqEwVonjMXp+EbLwCtuK2EXpjVcrl/2/6sSgtXE2Ubyq4KkU6XMCBDeJ2q6HQNtgEKHvTNqsp7rcxZDgpNH61qI9V2dE=");
res = dbTest.runCommand({
    "insert": "basic",
    documents: [{
        "_id": 1,
        "unindexed": v1Payload,
    }],
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7413901);

//
// Test v1 find equality
//
jsTestLog("Testing v1 equality encrypted find fails");
v1Payload = BinData(
    6,
    "BbEAAAAFZAAgAAAAACoHme5RnctV9kJcBlLhuRkmFoUCR2EMWTS/NwTcRALUBXMAIAAAAAAHkawU6xGYztV3h30Q1A" +
        "BdEY7o+rmyZIfB2ng8838u4AVjACAAAAAAFHEmCfQWVcRgKnL+Y7u/u9/5dQyaQSLSbeGp4auL000FZQAgAAAAALVE" +
        "SvKWp41m2canTKfnm4rmoRMwMPEcyj9YuAkVDksCEmNtAAQAAAAAAAAAAA==");
res = dbTest.runCommand(
    {find: "basic", filter: {first: v1Payload}, encryptionInformation: encryptionInfo});
assert.commandFailedWithCode(res, 7292602);

//
// Test v1 count equality
//
jsTestLog("Testing v1 equality encrypted count fails");
res = dbTest.runCommand(
    {count: "basic", query: {first: v1Payload}, encryptionInformation: encryptionInfo});
assert.commandFailedWithCode(res, 7292602);

//
// Test v1 aggregate equality $match
//
jsTestLog("Testing v1 equality encrypted $match fails");
res = dbTest.runCommand({
    aggregate: "basic",
    pipeline: [{$match: {first: v1Payload}}],
    cursor: {},
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7292602);

//
// Test v1 delete equality
//
jsTestLog("Testing v1 equality encrypted delete fails");
res = dbTest.runCommand({
    delete: "basic",
    deletes: [{q: {first: v1Payload}, limit: 1}],
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7292602);

//
// Test v1 find range $gt 1
//
jsTestLog("Testing v1 range encrypted find fails");
v1Payload = BinData(
    6,
    "CvEAAAADcGF5bG9hZADBAAAABGcAhQAAAAMwAH0AAAAFZAAgAAAAAM2AuByUc3KeCcIBhbbGTQ+yjEwcLtinSfhhwX" +
        "C+GGCQBXMAIAAAAAD1NAGKC+euJxrgEXD7o+pRN068yJFssTxSmbhK6mAyAAVjACAAAAAAMXr0229Ozfm4b/MEiH4p" +
        "o0OxyPpDrzKCUvudiZjM1esAAAVlACAAAAAAqyE4josUK9EsXiPJFSfpB+Q+8JpoRwXpJHGUZcbqHfsSY20ABAAAAA" +
        "AAAAAAEHBheWxvYWRJZAAAAAAAEGZpcnN0T3BlcmF0b3IAAQAAAAA=");
res = dbTest.runCommand(
    {find: "basic", filter: {rating: {$gt: v1Payload}}, encryptionInformation: encryptionInfo});
assert.commandFailedWithCode(res, 7292602);

//
// Test v1 count range
//
jsTestLog("Testing v1 range encrypted count fails");
res = dbTest.runCommand(
    {count: "basic", query: {rating: {$lte: v1Payload}}, encryptionInformation: encryptionInfo});
assert.commandFailedWithCode(res, 7292602);

//
// Test v1 aggregate range $match
//
jsTestLog("Testing v1 range encrypted $match fails");
res = dbTest.runCommand({
    aggregate: "basic",
    pipeline: [{$match: {rating: {$lt: v1Payload}}}],
    cursor: {},
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7292602);

//
// Test v1 delete range $match
//
jsTestLog("Testing v1 range encrypted delete fails");
res = dbTest.runCommand({
    delete: "basic",
    deletes: [{q: {rating: {$gte: v1Payload}}, limit: 1}],
    encryptionInformation: encryptionInfo
});
assert.commandFailedWithCode(res, 7292602);
}());
