/* Test basic db operations in multitenancy using $tenant for FLE2.
 *
 * @tags: [
 * requires_non_retryable_writes,
 * assumes_unsharded_collection,
 * requires_fcv_70
 * ]
 */
import {
    assertIsIndexedEncryptedField,
    assertIsUnindexedEncryptedField,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";

const rst =
    new ReplSetTest({nodes: 3, nodeOptions: {auth: '', setParameter: {multitenancySupport: true}}});

const kDbName = "myDb";
const kCollName = "myColl";
const kTenantId = ObjectId();
const kOtherTenantId = ObjectId();

rst.startSet({keyFile: 'jstests/libs/key1'});
rst.initiate();
const primary = rst.getPrimary();

// create root user on primary in order to use $tenant
const userName = "admin";
const adminPwd = "pwd";
const adminDB = primary.getDB("admin");
adminDB.createUser({user: userName, pwd: adminPwd, roles: ["root"]});
assert(adminDB.auth(userName, adminPwd));
const featureFlagRequireTenantId = FeatureFlagUtil.isEnabled(adminDB, "RequireTenantID");
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
    // that are created for that tenant as part of FLE when the
    // `createEncryptionCollection` was called. They are part of the State Collections which
    // allow a specific user to query on encrypted fields while minimizing the security leakage.
    // They are regular user collections with names.
    let fle2CollectionCount = 3;

    let colls = assert.commandWorked(
        edb.runCommand({listCollections: 1, nameOnly: true, '$tenant': kTenantId}));
    assert.eq(fle2CollectionCount, colls.cursor.firstBatch.length, tojson(colls.cursor.firstBatch));

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
    client.assertEncryptedCollectionCounts(kCollName, 1, 2, 2, kTenantId);

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

    client.assertEncryptedCollectionCounts(kCollName, 2, 2, 2, kTenantId);

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
    res = edb.runCommand({delete: kCollName, deletes: [{"q": {"first": "leroy"}, limit: 1}]});
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

jsTest.log(`Testing FLE aggregation for tenant ${kTenantId}`);
{
    // Test that getMore only works on a tenant's own cursor
    const cmdRes = assert.commandWorked(edb.runCommand(
        {find: kCollName, projection: {_id: 1}, batchSize: 1, '$tenant': kTenantId}));
    assert.eq(cmdRes.cursor.firstBatch.length, 1, tojson(cmdRes.cursor.firstBatch));
    assert.commandWorked(
        edb.runCommand({getMore: cmdRes.cursor.id, collection: kCollName, '$tenant': kTenantId}));

    const cmdRes2 = assert.commandWorked(edb.runCommand(
        {find: kCollName, projection: {_id: 1}, batchSize: 1, '$tenant': kTenantId}));
    assert.commandFailedWithCode(
        edb.runCommand(
            {getMore: cmdRes2.cursor.id, collection: kCollName, '$tenant': kOtherTenantId}),
        ErrorCodes.Unauthorized);

    // Test that aggregate only finds a tenant's own document.
    const aggRes = assert.commandWorked(edb.runCommand({
        aggregate: kCollName,
        pipeline: [{$match: {"first": "dwayne"}}, {$project: {_id: 1}}],
        cursor: {},
        '$tenant': kTenantId
    }));
    assert.eq(1, aggRes.cursor.firstBatch.length, tojson(aggRes.cursor.firstBatch));
    assert.eq({_id: 1}, aggRes.cursor.firstBatch[0]);

    // Test that explain works correctly.
    // TODO SERVER-78904: Renable to test explain fix
    // const kTenantExplainRes = assert.commandWorked(edb.runCommand(
    //     {explain: {find: kCollName}, verbosity: 'executionStats', '$tenant': kTenantId}));
    // assert.eq(3, kTenantExplainRes.executionStats.nReturned, tojson(kTenantExplainRes));

    const prefixedDbName = kTenantId + '_' + edb.getName();
    const targetDb = featureFlagRequireTenantId ? edb.getName() : prefixedDbName;

    // Get catalog without specifying target collection (collectionless).
    let result = adminDB.runCommand(
        {aggregate: 1, pipeline: [{$listCatalog: {}}], cursor: {}, '$tenant': kTenantId});
    let resultArray = result.cursor.firstBatch;

    // Check that the resulting array of catalog entries contains our target databases and
    // namespaces.
    assert(resultArray.some((entry) => (entry.db === targetDb) && (entry.name === kCollName)));
}

jsTest.log(`Testing FLE transaction for tenant ${kTenantId}`);
{
    const kOtherCollName = "otherDb";
    assert.commandWorked(client.createEncryptionCollection(kOtherCollName, {
        encryptedFields: {
            "fields": [
                {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            ]
        },
        '$tenant': kTenantId
    }));
    const session = edb.getMongo().startSession({causalConsistency: false});

    // Verify we can insert two documents in a txn
    session.startTransaction();
    let res = assert.commandWorked(edb.runCommand(
        {"insert": kOtherCollName, documents: [{"first": "mark"}], '$tenant': kTenantId}));
    assert.eq(res.n, 1);
    client.assertWriteCommandReplyFields(res);

    res = assert.commandWorked(edb.runCommand(
        {"insert": kOtherCollName, documents: [{"first": "john"}], '$tenant': kTenantId}));
    assert.eq(res.n, 1);
    client.assertWriteCommandReplyFields(res);

    session.commitTransaction();
    client.assertEncryptedCollectionCounts(kOtherCollName, 2, 2, 2, kTenantId);

    // Verify we insert two documents in a txn but abort it
    session.startTransaction();

    res = assert.commandWorked(edb.runCommand({
        "insert": kCollName,
        documents: [{"first": "jacques", "middle": "phil", "aka": "jp"}],
        '$tenant': kTenantId
    }));
    assert.eq(res.n, 1);
    client.assertWriteCommandReplyFields(res);

    res = assert.commandWorked(edb.runCommand({
        "insert": kCollName,
        documents: [{"first": "zack", "middle": "bryan", "aka": "zb"}],
        '$tenant': kTenantId
    }));

    assert.commandWorked(session.abortTransaction_forTesting());

    client.assertEncryptedCollectionCounts(kOtherCollName, 2, 2, 2, kTenantId);
}

jsTest.log(`Testing FLE renameCollection collection for tenant ${kTenantId}`);
{
    const fromName = kDbName + "." + kCollName;
    const toName = fromName + "_renamed";
    assert.commandWorked(adminDB.runCommand(
        {renameCollection: fromName, to: toName, dropTarget: false, '$tenant': kTenantId}));

    // Verify the the renamed collection by findAndModify existing documents.
    const res = assert.commandWorked(edb.runCommand({
        findAndModify: kCollName + "_renamed",
        query: {_id: 1},
        update: {$set: {"middle": "johnson"}},
        '$tenant': kTenantId
    }));
    assert.eq(res.lastErrorObject.n, 1);
    assert.eq(res.lastErrorObject.updatedExisting, true);
    assert.eq(res.value.first, "dwayne");
    assert.eq(res.value.middle, "elizondo mountain dew herbert");
    assert.eq(res.value.aka, "president camacho");

    // This collection should not be accessed with a different tenant.
    assert.commandFailedWithCode(
        adminDB.runCommand(
            {renameCollection: toName, to: fromName, dropTarget: true, '$tenant': kOtherTenantId}),
        ErrorCodes.NamespaceNotFound);

    // Reset the collection to be used below
    assert.commandWorked(adminDB.runCommand(
        {renameCollection: toName, to: fromName, dropTarget: false, '$tenant': kTenantId}));
}

jsTest.log(`Testing FLE drop collection for tenant ${kTenantId}`);
{
    // Another tenant shouldn't be able to drop the collection or database.
    assert.commandWorked(edb.runCommand({drop: kCollName, '$tenant': kOtherTenantId}));
    const collsAfterDropCollectionByOtherTenant = assert.commandWorked(edb.runCommand(
        {listCollections: 1, nameOnly: true, filter: {name: kCollName}, '$tenant': kTenantId}));
    assert.eq(1,
              collsAfterDropCollectionByOtherTenant.cursor.firstBatch.length,
              tojson(collsAfterDropCollectionByOtherTenant.cursor.firstBatch));

    assert.commandWorked(edb.runCommand({dropDatabase: 1, '$tenant': kOtherTenantId}));
    const collsAfterDropDbByOtherTenant = assert.commandWorked(edb.runCommand(
        {listCollections: 1, nameOnly: true, filter: {name: kCollName}, '$tenant': kTenantId}));
    assert.eq(1,
              collsAfterDropDbByOtherTenant.cursor.firstBatch.length,
              tojson(collsAfterDropDbByOtherTenant.cursor.firstBatch));

    // Drop the tenant collection.
    assert.commandWorked(edb.runCommand({drop: kCollName, '$tenant': kTenantId}));
    const collsAfterDropCollection = assert.commandWorked(edb.runCommand(
        {listCollections: 1, nameOnly: true, filter: {name: kCollName}, '$tenant': kTenantId}));
    assert.eq(0,
              collsAfterDropCollection.cursor.firstBatch.length,
              tojson(collsAfterDropCollection.cursor.firstBatch));

    // Now, drop the database using the original tenantId.
    assert.commandWorked(edb.runCommand({dropDatabase: 1, '$tenant': kTenantId}));
    const collsAfterDropDb = assert.commandWorked(edb.runCommand(
        {listCollections: 1, nameOnly: true, filter: {name: kCollName}, '$tenant': kTenantId}));
    assert.eq(
        0, collsAfterDropDb.cursor.firstBatch.length, tojson(collsAfterDropDb.cursor.firstBatch));
}

rst.stopSet();
