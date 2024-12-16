// Ensures that a new collection cannot have both encryptedFields
// and jsonSchema with encrypt.

/**
 * @tags: [
 * requires_fcv_80
 * ]
 */
import {
    codeFailsInQueryAnalysisWithError,
    EncryptedClient
} from "jstests/fle2/libs/encrypted_client_util.js";
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {kRandomAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

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

client.createBasicEncryptionCollection = function(coll, options, failure, qaFailure) {
    client.runEncryptionOperation(() => {
        if (failure != null) {
            assert.commandFailedWithCode(this._db.createCollection(coll, options), failure);
            return;
        }
        assert.commandWorked(this._db.createCollection(coll, options));
    });
};

assert.commandWorked(client.createEncryptionCollection("enc_fields", sampleEncryptedFields));
client.createBasicEncryptionCollection("json_schema", sampleJSONSchema);

assert(codeFailsInQueryAnalysisWithError(
    () => client.createBasicEncryptionCollection("merged", mergedOptions, 224)));

// Test collmod
const collmodPayload = Object.assign({}, {collMod: "enc_fields"}, sampleJSONSchema);

client.runEncryptionOperation(() => {
    assert(codeFailsInQueryAnalysisWithError(() => client.getDB().runCommand(collmodPayload)));
});

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
assert.throwsWithCode(
    () => client.createBasicEncryptionCollection("enc_fields_bad", encFieldsBad, 6412601), 6412601);

// Double fields with range index cannot have min/max.
const encFieldsRangeDouble = {
    encryptedFields: {
        "fields": [{
            "path": "height",
            "bsonType": "double",
            "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "queries": {"queryType": "range", "sparsity": 1}
        }]
    }
};

const encFieldsRangeDoubleWithPrecision = {
    encryptedFields: {
        "fields": [{
            "path": "height",
            "bsonType": "double",
            "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "queries":
                {"queryType": "range", "sparsity": 1, "min": 0.0, "max": 10.0, "precision": 2}
        }]
    }
};

assert.commandWorked(
    client.createEncryptionCollection("enc_fields_rng_good_c", encFieldsRangeDouble));
assert.commandWorked(
    client.createEncryptionCollection("enc_fields_rng_good_c2", encFieldsRangeDoubleWithPrecision));

// Double fields with range index cannot have min/max.
const encFieldsRangeDecimal = {
    encryptedFields: {
        "fields": [{
            "path": "height",
            "bsonType": "decimal",
            "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "queries": {"queryType": "range", "sparsity": 1}
        }]
    }
};

const encFieldsRangeDecimalWithPrecision = {
    encryptedFields: {
        "fields": [{
            "path": "height",
            "bsonType": "decimal",
            "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "queries": {
                "queryType": "range",
                "sparsity": 1,
                "min": NumberDecimal(0.0),
                "max": NumberDecimal(10.0),
                "precision": 2,
            }
        }]
    }
};

assert.commandWorked(
    client.createEncryptionCollection("enc_fields_rng_good_d", encFieldsRangeDecimal));
assert.commandWorked(client.createEncryptionCollection("enc_fields_rng_good_d2",
                                                       encFieldsRangeDecimalWithPrecision));

const encFieldsRangeDecimalWithBadPrecision = {
    encryptedFields: {
        "fields": [{
            "path": "height",
            "bsonType": "decimal",
            "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "queries": {
                "queryType": "range",
                "sparsity": 1,
                "min": NumberDecimal(0.0),
                "max": NumberDecimal(10.123),
                "precision": 2,
            }
        }]
    }
};

assert.commandFailedWithCode(
    db.createCollection("enc_fields_rng_bad_d", encFieldsRangeDecimalWithBadPrecision), 6966808);

const encFieldsRangeTypesUnbounded = {
    encryptedFields: {
        "fields": [
            {
                "path": "a",
                "bsonType": "date",
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
                "queries": {"queryType": "range"}
            },
            {
                "path": "b",
                "bsonType": "long",
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befaea"),
                "queries": {"queryType": "range"}
            },
            {
                "path": "c",
                "bsonType": "int",
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befaeb"),
                "queries": {"queryType": "range"}
            }
        ]
    }
};
assert.commandWorked(
    client.createEncryptionCollection("enc_fields_rng_unbounded", encFieldsRangeTypesUnbounded));

if (FeatureFlagUtil.isPresentAndDisabled(db.getMongo(), 'QETextSearchPreview')) {
    // runs if featureFlagQETextSearchPreview is disabled
    // TODO: SERVER-94397 remove test once feature flag is gone
    const qtype = {
        queryType: "substringPreview",
        contention: NumberLong(1),
        strMaxLength: NumberLong(2),
        strMinQueryLength: NumberLong(2),
        strMaxQueryLength: NumberLong(2),
        caseSensitive: true,
        diacriticSensitive: true,
    };
    const badSchema = {
        encryptedFields: {
            fields: [{
                path: "ssn",
                bsonType: "string",
                keyId: UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
                queries: [qtype]
            }]
        }
    };
    assert.commandFailedWithCode(db.createCollection("basic_text", badSchema), 9783415);
    qtype.queryType = "suffixPreview";
    assert.commandFailedWithCode(db.createCollection("basic_text", badSchema), 9783416);
    qtype.queryType = "prefixPreview";
    assert.commandFailedWithCode(db.createCollection("basic_text", badSchema), 9783417);
    badSchema.encryptedFields.fields[0].queries =
        [{queryType: "equality"}, {queryType: "equality"}];
    assert.commandFailedWithCode(db.createCollection("basic_text", badSchema), 9783414);
}