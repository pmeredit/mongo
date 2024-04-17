/**
 * Tests upserting documents with an encrypted predicate.
 *
 * @tags: [
 *   requires_fcv_70,
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_unsharded_collection,
 *   # TODO SERVER-89286 re-enable this test
 *   assumes_balancer_off,
 * ]
 */

const dbName = jsTestName();
const collName = jsTestName();
const kms = {
    key: BinData(
        0,
        "/tu9jUCBqZdwCelwE/EAm/4WqdxrSMi04B8e9uAV+m30rI1J2nhKZZtQjdvsSCwuI4erR6IEcEK+5eGUAODv43NDNIR9QheT2edWFewUfHKsl9cnzTc86meIzOmYl6dr")
};

var testdb = db.getSiblingDB(dbName);
testdb.dropDatabase();

const csfleOpts = {
    kmsProviders: {
        local: kms,
    },
    keyVaultNamespace: dbName + ".keystore",
    schemaMap: {},
};

var shell = Mongo(db.getMongo().host.toString(), csfleOpts);
var kv = shell.getKeyVault();

const schema = {
    "fields": [
        {
            "path": "foo",
            "keyId": kv.createKey("local", "ignored"),
            "bsonType": "string",
            "queries": {"queryType": "equality"}
        },
    ]
};

var edb = shell.getDB(jsTestName());
edb.createCollection(collName, {encryptedFields: schema});

var ecoll = edb.getCollection(collName);
ecoll.createIndex({__safeContent__: 1});

ecoll.insertOne({foo: "foovalue"});

assert.commandWorked(ecoll.updateOne({$and: [{foo: "foovalue"}, {bar: "barvalue"}]},
                                     {$set: {foo: "other_foovalue", bar: "other_barvalue"}},
                                     {upsert: true}));

const doc = ecoll.find({foo: "other_foovalue"}, {_id: 0, __safeContent__: 0}).toArray()[0];
assert.eq(doc.foo, "other_foovalue", tojson(doc));
assert.eq(doc.bar, "other_barvalue", tojson(doc));
