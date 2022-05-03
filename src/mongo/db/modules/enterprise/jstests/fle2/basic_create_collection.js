// Ensures that a new collection cannot have both encryptedFields
// and jsonSchema with encrypt.

/**
 * @tags: [
 * requires_fcv_60
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

(function() {
'use strict';

let dbName = 'create_collection_basic';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const sampleJSONSchema = {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            properties: {
                name: {bsonType: "string", description: "must be a string"},
                ssn: {encrypt: {bsonType: "int", algorithm: kRandomAlgo, keyId: [UUID()]}}
            }
        }
    }
};

const sampleEncryptedFields = {
    encryptedFields: {
        "fields": [
            {
                "path": "ssn",
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
                "bsonType": "int",
                "queries": {"queryType": "equality"},
            },
        ]
    }
};

const mergedOptions = Object.assign({}, sampleJSONSchema, sampleEncryptedFields);

let client = new EncryptedClient(db.getMongo(), dbName);

const codeFailedInQueryAnalysis = (cb) => {
    try {
        cb();
        return false;
    } catch (e) {
        return e.message.indexOf("Client Side Field Level Encryption Error") !== -1;
    }
};

client.createBasicEncryptionCollection = function(coll, options, failure, qaFailure) {
    if (failure != null) {
        assert.commandFailedWithCode(this._edb.createCollection(coll, options), failure);
        return;
    }
    assert.commandWorked(this._edb.createCollection(coll, options));
};

assert.commandWorked(client.createEncryptionCollection("enc_fields", sampleEncryptedFields));
client.createBasicEncryptionCollection("json_schema", sampleJSONSchema);
assert(codeFailedInQueryAnalysis(
    () => client.createBasicEncryptionCollection("merged", mergedOptions, 224)));

// Test collmod
const collmodPayload = Object.assign({}, {collMod: "enc_fields"}, sampleJSONSchema);

assert(codeFailedInQueryAnalysis(() => client.getDB().runCommand(collmodPayload), 224));

// Test that bsontype needs to be specified if queries is specified, and that bsontype
// does not need to be specified if queries is not specified.

// Queries specified, bsonType not specified. Should error.
const encFieldsBad = {
    encryptedFields: {
        "fields": [
            {
                "path": "ssn",
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
                "queries": {"queryType": "equality"},
            },
        ]
    }
};

// Queries unspecified, bsonType specified. Should be fine.
const encFieldsGoodA = {
    encryptedFields: {
        "fields": [
            {
                "path": "ssn",
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
                "bsonType": "int",
            },
        ]
    }
};

// Neither specified. Should be fine.
const encFieldsGoodB = {
    encryptedFields: {
        "fields": [
            {
                "path": "ssn",
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            },
        ]
    }
};

assert.commandWorked(client.createEncryptionCollection("enc_fields_good_a", encFieldsGoodA));
assert.commandWorked(client.createEncryptionCollection("enc_fields_good_b", encFieldsGoodB));
client.createBasicEncryptionCollection("enc_fields_bad", encFieldsBad, 6412601);
}());
