/**
 * Tests upserting documents with an encrypted predicate.
 *
 * @tags: [
 *   requires_fcv_70,
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_unsharded_collection,
 * ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = jsTestName();
const collName = jsTestName();
const kms = {
    key: BinData(
        0,
        "/tu9jUCBqZdwCelwE/EAm/4WqdxrSMi04B8e9uAV+m30rI1J2nhKZZtQjdvsSCwuI4erR6IEcEK+5eGUAODv43NDNIR9QheT2edWFewUfHKsl9cnzTc86meIzOmYl6dr")
};

db.getSiblingDB(dbName).dropDatabase();

const csfleOpts = {
    kmsProviders: {
        local: kms,
    },
    keyVaultNamespace: dbName + ".keystore",
    schemaMap: {},
};

const mongo = db.getMongo();
const shell = Mongo(mongo.host.toString(), csfleOpts);
const kv = shell.getKeyVault();

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

const client = new EncryptedClient(mongo, dbName);
assert.commandWorked(client.createEncryptionCollection(collName, {encryptedFields: schema}));

const ecoll = client.getDB().getCollection(collName);
ecoll.einsertOne({foo: "foovalue"});

assert.commandWorked(ecoll.eupdateOne({$and: [{foo: "foovalue"}, {bar: "barvalue"}]},
                                      {$set: {foo: "other_foovalue", bar: "other_barvalue"}},
                                      {upsert: true}));

const doc = ecoll.efindOne({foo: "other_foovalue"}, {_id: 0, __safeContent__: 0});
assert.eq(doc.foo, "other_foovalue", tojson(doc));
assert.eq(doc.bar, "other_barvalue", tojson(doc));
