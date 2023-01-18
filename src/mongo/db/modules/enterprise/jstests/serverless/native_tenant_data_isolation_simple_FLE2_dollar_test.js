/* Test basic db operations in multitenancy using $tenant for FLE2.
 *
 * @tags: [
 * requires_non_retryable_writes,
 * assumes_unsharded_collection,
 * ]
 */
(function() {
"use strict";
load("jstests/fle2/libs/encrypted_client_util.js");  // For EncryptedClient
load('jstests/aggregation/extras/utils.js');         // For arrayEq()
load("jstests/libs/feature_flag_util.js");           // for isEnabled

const rst =
    new ReplSetTest({nodes: 3, nodeOptions: {auth: '', setParameter: {multitenancySupport: true}}});

const kDbName = "myDb";
const kCollName = "myColl";
const kTenantId = ObjectId();

rst.startSet({keyFile: 'jstests/libs/key1'});
rst.initiate();
const primary = rst.getPrimary();

// create root user on primary in order to use $tenant
const userName = "admin";
const adminPwd = "pwd";
const adminDB = primary.getDB("admin");
adminDB.createUser({user: userName, pwd: adminPwd, roles: ["root"]});
assert(adminDB.auth(userName, adminPwd));
let dbTest = primary.getDB(kDbName);

let client = new EncryptedClient(primary, kDbName, userName, adminPwd);

jsTest.log(`Creating FLE collection ${kCollName} for tenant ${kTenantId}`);
assert.commandWorked(client.createEncryptionCollection(kCollName, {
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "middle", "bsonType": "string"},
            {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
        ]
    },
    '$tenant': kTenantId
}));

let edb = client.getDB();

jsTest.log(`Testing FLE listCollection for tenant ${kTenantId}`);
{
    // There is myColl, esc (Encrypted State Collection), ecoc(Encrypted Compaction Collection),
    // ecc(Encrypted Cache Collection) that are created for that tenant as part of FLE when the
    // `createEncryptionCollection` was called. They are part of the State Collections which
    // allow a specific user to query on encrypted fields while minimizing the security leakage.
    // They are regular user collections with names.
    let colls = assert.commandWorked(
        edb.runCommand({listCollections: 1, nameOnly: true, '$tenant': kTenantId}));
    assert.eq(4, colls.cursor.firstBatch.length, tojson(colls.cursor.firstBatch));

    colls = assert.commandWorked(edb.runCommand({listCollections: 1, nameOnly: true}));
    assert.eq(0, colls.cursor.firstBatch.length, tojson(colls.cursor.firstBatch));

    // we cannot see collections for another tenant.
    colls = assert.commandWorked(
        edb.runCommand({listCollections: 1, nameOnly: true, '$tenant': ObjectId()}));
    assert.eq(0, colls.cursor.firstBatch.length, tojson(colls.cursor.firstBatch));
}

jsTest.log(`Testing FLE insert for tenant ${kTenantId}`);
{
    let res = assert.commandWorked(edb.runCommand({
        "insert": kCollName,
        documents: [{
            "_id": 1,
            "first": "dwayne",
            "middle": "elizondo mountain dew herbert",
            "aka": "president camacho"
        }],
        '$tenant': kTenantId
    }));
    assert.eq(res.n, 1);
    client.assertWriteCommandReplyFields(res);
    client.assertEncryptedCollectionCounts(kCollName, 1, 2, 0, 2, kTenantId);

    // Verify it is encrypted with an unencrypted client
    let rawDoc = assert.commandWorked(dbTest.runCommand({find: kCollName, '$tenant': kTenantId}))
                     .cursor.firstBatch[0];
    print(tojson(rawDoc));
    assertIsIndexedEncryptedField(rawDoc["first"]);
    assertIsUnindexedEncryptedField(rawDoc["middle"]);
    assertIsIndexedEncryptedField(rawDoc["aka"]);
    assert(rawDoc[kSafeContentField] !== undefined);

    // Verify we decrypt it clean with an encrypted client.
    const doc = assert.commandWorked(edb.runCommand({find: kCollName, '$tenant': kTenantId}))
                    .cursor.firstBatch[0];
    print(tojson(doc));
    assert.eq(doc["first"], "dwayne");
    assert.eq(doc["middle"], "elizondo mountain dew herbert");
    assert.eq(doc["aka"], "president camacho");
    assert(doc[kSafeContentField] !== undefined);

    client.assertOneEncryptedDocumentFields(kCollName, {}, {"first": "dwayne"}, kTenantId);

    assert.commandWorked(edb.runCommand(
        {"insert": kCollName, documents: [{"last": "camacho"}], '$tenant': kTenantId}));

    rawDoc = assert
                 .commandWorked(dbTest.runCommand(
                     {find: kCollName, filter: {"last": "camacho"}, '$tenant': kTenantId}))
                 .cursor.firstBatch[0];

    print(tojson(rawDoc));
    assert.eq(rawDoc["last"], "camacho");
    assert(rawDoc[kSafeContentField] === undefined);

    client.assertEncryptedCollectionCounts(kCollName, 2, 2, 0, 2, kTenantId);

    // Trigger a duplicate key exception and validate the response
    res = assert.commandFailed(edb.runCommand(
        {"insert": kCollName, documents: [{"_id": 1, "first": "camacho"}], '$tenant': kTenantId}));
    print(tojson(res));

    assert.eq(res.n, 0);
    client.assertWriteCommandReplyFields(res);

    // Inserting a document with encrypted data at a path that is marked for encryption, throws an
    // error.
    assert.throwsWithCode(() => edb.basic.runCommand({
        "insert": kCollName,
        documents: [{"first": BinData(6, "data")}],
        '$tenant': kTenantId
    }),
                          31041);
}

jsTest.log(`Testing FLE delete with tenant ${kTenantId}`);
{
    // Insert a new document for tenantid
    let res = assert.commandWorked(edb.runCommand({
        "insert": kCollName,
        documents: [{"_id": 2, "first": "leroy", "middle": "jenkins", "aka": "the runner"}],
        '$tenant': kTenantId
    }));
    assert.eq(res.n, 1);
    client.assertWriteCommandReplyFields(res);

    // Delete a document fails with no tenantid
    res = assert.commandWorked(
        edb.runCommand({delete: kCollName, deletes: [{"q": {"first": "leroy"}, limit: 1}]}));
    assert.eq(res.n, 0);

    // Delete a document fails with a different tenantid
    const kDifferentTenantId = ObjectId();
    res = assert.commandWorked(edb.runCommand({
        delete: kCollName,
        deletes: [{"q": {"first": "leroy"}, limit: 1}],
        '$tenant': kDifferentTenantId
    }));
    assert.eq(res.n, 0);

    // Delete a document succeeds with tenantid
    res = assert.commandWorked(edb.runCommand(
        {delete: kCollName, deletes: [{"q": {"first": "leroy"}, limit: 1}], '$tenant': kTenantId}));
    assert.eq(res.n, 1);
    print("deleted=" + tojson(res));
    client.assertWriteCommandReplyFields(res);
}

jsTest.log(`Testing FLE update and findAndModify with tenant ${kTenantId}`);
{
    // Insert a new document for tenantid
    let res = assert.commandWorked(edb.runCommand({
        "insert": kCollName,
        documents: [{"_id": 3, "first": "leroy", "middle": "jenkins", "aka": "runner"}],
        '$tenant': kTenantId
    }));
    assert.eq(res.n, 1);
    client.assertWriteCommandReplyFields(res);

    const updateCmd = {
        update: kCollName,
        updates: [{q: {"first": "leroy"}, u: {$set: {"middle": "notJenkins"}}}],
        '$tenant': kTenantId
    };
    res = assert.commandWorked(edb.runCommand(updateCmd));
    assert.eq(res.n, 1, tojson(res));
    assert.eq(res.nModified, 1);

    res = assert.commandWorked(edb.runCommand({
        findAndModify: kCollName,
        query: {"first": "leroy"},
        update: {$set: {"aka": "the runner"}},
        '$tenant': kTenantId
    }));
    assert.eq(res.lastErrorObject.n, 1);
    assert.eq(res.lastErrorObject.updatedExisting, true);
    assert.eq(res.value.first, "leroy");
    assert.eq(res.value.middle, "notJenkins");
    assert.eq(res.value.aka, "runner");

    // finds the updated document and checks all the fields
    res = assert.commandWorked(
        edb.runCommand({find: kCollName, filter: {_id: 3}, '$tenant': kTenantId}));
    assert.eq(res.cursor.firstBatch[0].first, "leroy");
    assert.eq(res.cursor.firstBatch[0].middle, "notJenkins");
    assert.eq(res.cursor.firstBatch[0].aka, "the runner");

    // fails to update something that does not exist.
    res = assert.commandWorked(edb.runCommand({
        findAndModify: kCollName,
        query: {"first": "notExistingName"},
        update: {$set: {"last": "notJenkins"}},
        '$tenant': kTenantId
    }));
    assert.eq(res.lastErrorObject.n, 0);
    assert.eq(res.lastErrorObject.updatedExisting, false);
}

rst.stopSet();
})();
