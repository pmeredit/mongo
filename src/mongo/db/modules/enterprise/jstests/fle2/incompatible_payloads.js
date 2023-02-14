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
        ]
    }
}));

const edb = client.getDB();

const collInfos = edb.getCollectionInfos({name: "basic"});
assert.eq(collInfos.length, 1);
assert(collInfos[0].options.encryptedFields !== undefined);

// with unencrypted client, try to insert a canned v1 insert update payload
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
    encryptionInformation: {schema: {"basic.basic": collInfos[0].options.encryptedFields}}
});
assert.commandFailedWithCode(res, 7291901);
}());
