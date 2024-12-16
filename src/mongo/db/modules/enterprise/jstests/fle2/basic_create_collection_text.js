/**
 * Test create collection with text search indexed encrypted fields works
 *
 * @tags: [
 *   featureFlagQETextSearchPreview,
 * ]
 */
import {
    codeFailsInQueryAnalysisWithError,
    EncryptedClient
} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic_create_collection_text';
db.getSiblingDB(dbName).dropDatabase();
const client = new EncryptedClient(db.getMongo(), dbName);

const prefixSuffixField = {
    path: "ssn",
    bsonType: "string",
    queries: [
        {
            queryType: "suffixPreview",
            contention: NumberLong(1),
            strMinQueryLength: NumberLong(3),
            strMaxQueryLength: NumberLong(30),
            caseSensitive: true,
            diacriticSensitive: true,
        },
        {
            queryType: "prefixPreview",
            contention: NumberLong(1),
            strMinQueryLength: NumberLong(2),
            strMaxQueryLength: NumberLong(20),
            caseSensitive: true,
            diacriticSensitive: true,
        },
    ]
};

const validSchema = {
    encryptedFields: {
        "fields": [
            {
                path: "firstName",
                bsonType: "string",
                queries: {
                    queryType: "substringPreview",
                    contention: NumberLong(1),
                    strMaxLength: NumberLong(100),
                    strMinQueryLength: NumberLong(1),
                    strMaxQueryLength: NumberLong(100),
                    caseSensitive: false,
                    diacriticSensitive: false,
                }
            },
            {
                path: "lastName",
                bsonType: "string",
                queries: {
                    queryType: "prefixPreview",
                    contention: NumberLong(1),
                    strMinQueryLength: NumberLong(2),
                    strMaxQueryLength: NumberLong(20),
                    caseSensitive: false,
                    diacriticSensitive: true,
                }
            },
            {
                path: "location",
                bsonType: "string",
                queries: {
                    queryType: "suffixPreview",
                    contention: NumberLong(1),
                    strMinQueryLength: NumberLong(2),
                    strMaxQueryLength: NumberLong(20),
                    caseSensitive: true,
                    diacriticSensitive: false,
                }
            },
            prefixSuffixField
        ]
    }
};

const schemaWithTooManyQueryTypes = {
    encryptedFields: {
        "fields": [
            {
                path: "ssn",
                bsonType: "string",
                queries: [
                    {
                        queryType: "suffixPreview",
                        contention: NumberLong(1),
                        strMinQueryLength: NumberLong(2),
                        strMaxQueryLength: NumberLong(20),
                        caseSensitive: true,
                        diacriticSensitive: true,
                    },
                    {
                        queryType: "prefixPreview",
                        contention: NumberLong(1),
                        strMinQueryLength: NumberLong(2),
                        strMaxQueryLength: NumberLong(20),
                        caseSensitive: true,
                        diacriticSensitive: true,
                    },
                    {
                        queryType: "suffixPreview",
                        contention: NumberLong(1),
                        strMinQueryLength: NumberLong(2),
                        strMaxQueryLength: NumberLong(20),
                        caseSensitive: true,
                        diacriticSensitive: true,
                    },
                ]
            },
        ]
    }
};

const schemaWithMixedTextAndNonTextQueryTypes = {
    encryptedFields: {
        "fields": [
            {
                path: "ssn",
                bsonType: "string",
                queries: [
                    {
                        queryType: "prefixPreview",
                        contention: NumberLong(1),
                        strMinQueryLength: NumberLong(2),
                        strMaxQueryLength: NumberLong(20),
                        caseSensitive: true,
                        diacriticSensitive: true,
                    },
                    {
                        queryType: "equality",
                        contention: NumberLong(1),
                    },
                ]
            },
        ]
    }
};

const schemaWithEmptyQueriesList = {
    encryptedFields: {
        "fields": [
            {path: "ssn", bsonType: "string", queries: []},
        ]
    }
};

const schemaWithMissingRequiredField = {
    encryptedFields: {
        "fields": [{
            path: "ssn",
            bsonType: "string",
            queries: [
                {
                    queryType: "prefixPreview",
                    contention: NumberLong(1),
                    strMinQueryLength: NumberLong(2),
                    caseSensitive: true,
                    diacriticSensitive: true,
                },
            ]
        }]
    }
};

jsTestLog("Test createCollection with various text search query types works");
assert.commandWorked(client.createEncryptionCollection("basic", validSchema));

jsTestLog("Test too many elements in QueryTypes list");
assert(codeFailsInQueryAnalysisWithError(
    () => client.createEncryptionCollection("bad_coll", schemaWithTooManyQueryTypes),
    "The number of query types for an encrypted field cannot exceed two"));

jsTestLog("Test non-text QueryTypes appearing in a list of multiple QueryTypes");
assert(codeFailsInQueryAnalysisWithError(
    () => client.createEncryptionCollection("bad_coll", schemaWithMixedTextAndNonTextQueryTypes),
    "Multiple query types may only include the suffixPreview and prefixPreview query types"));

jsTestLog("Test empty queries");
assert(codeFailsInQueryAnalysisWithError(
    () => client.createEncryptionCollection("bad_coll", schemaWithEmptyQueriesList),
    "At least one query type should be specified per field"));

jsTestLog("Test missing required field");
assert(codeFailsInQueryAnalysisWithError(
    () => client.createEncryptionCollection("bad_coll", schemaWithMissingRequiredField),
    "strMaxQueryLength parameter is required"));

jsTestLog("Test inconsistent case folding parameter in prefix+suffix");
prefixSuffixField.queries[0].caseSensitive = !prefixSuffixField.queries[1].caseSensitive;
assert(codeFailsInQueryAnalysisWithError(
    () => client.createEncryptionCollection("bad_coll", validSchema),
    "caseSensitive parameter must be the same"));
prefixSuffixField.queries[0].caseSensitive = prefixSuffixField.queries[1].caseSensitive;

jsTestLog("Test inconsistent diacritic folding parameter in prefix+suffix");
prefixSuffixField.queries[0].diacriticSensitive = !prefixSuffixField.queries[1].diacriticSensitive;
assert(codeFailsInQueryAnalysisWithError(
    () => client.createEncryptionCollection("bad_coll", validSchema),
    "diacriticSensitive parameter must be the same"));
prefixSuffixField.queries[0].diacriticSensitive = prefixSuffixField.queries[1].diacriticSensitive;

jsTestLog("Test inconsistent contention parameter in prefix+suffix");
prefixSuffixField.queries[0].contention += 1;
assert(codeFailsInQueryAnalysisWithError(
    () => client.createEncryptionCollection("bad_coll", validSchema),
    "contention parameter must be the same"));
prefixSuffixField.queries[0].contention -= 1;