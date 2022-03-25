/**
 * Negative test for distinct command.
 *
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

if (!isFLE2Enabled()) {
    return;
}

const dbName = 'basic_distinct';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);
const edb = client.getDB();

// Negative: encryptionInformation is not a valid field for distinct.
assert.commandFailedWithCode(
    dbTest.basic.runCommand({distinct: edb.basic.getName(), key: "key", encryptionInformation: {}}),
    40415);
}());
