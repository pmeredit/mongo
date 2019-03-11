/**
 * Verify that elements with an insert command are correctly marked for encryption.
 */

(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();

    const encryptDoc = {
        encrypt: {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", keyId: [UUID(), UUID()]}
    };
    const testCases = [
        // Test that a top level encrypt is translated.
        {
          schema: {type: "object", properties: {foo: encryptDoc}},
          docs: [{foo: "bar"}, {foo: "bar"}, {foo: {field: "isADoc"}}],
          encryptedPaths: ["foo"],
          notEncryptedPaths: []
        },
        // Test that only the correct fields are translated.
        {
          schema: {
              type: "object",
              properties: {
                  foo: encryptDoc,
                  bar: {type: "object", properties: {baz: encryptDoc, boo: {type: "string"}}}
              }
          },
          docs: [
              {foo: "bar"},
              {stuff: "baz"},
              {foo: "bin", no: "bar", bar: {baz: "stuff", boo: "plaintext"}}
          ],
          encryptedPaths: ["foo", "bar.baz"],
          notEncryptedPaths: ["bar.boo", "stuff", "no"]
        },
        // Test that a JSONPointer keyId is accepted.
        {
          schema: {
              type: "object",
              properties: {
                  foo: {
                      encrypt:
                          {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", keyId: "/key"}
                  }
              }
          },
          docs: [{foo: "bar", "key": "string"}],
          encryptedPaths: ["foo"],
          notEncryptedPaths: []
        }
    ];

    const extractField = function(doc, fieldName) {
        // Find the field.
        const fieldNames = fieldName.split(".");
        let curField = doc;
        for (let field of fieldNames) {
            if (typeof curField === "undefined") {
                return;
            }
            curField = curField[field];
        }
        return curField;
    };

    const testDb = conn.getDB("test");
    let insertCommand = {
        insert: "test.foo",
        documents: [],
        jsonSchema: {},
    };
    for (let test of testCases) {
        insertCommand["jsonSchema"] = test["schema"];
        insertCommand["documents"] = test["docs"];
        const result = assert.commandWorked(testDb.runCommand(insertCommand));
        for (let encryptedDoc of result["result"]["documents"]) {
            // For each field that should be encrypted. Some documents may not contain all of the
            // fields.
            for (let encrypt of test.encryptedPaths) {
                const curField = extractField(encryptedDoc, encrypt);
                if (typeof curField !== "undefined") {
                    assert(curField instanceof BinData,
                           tojson(test) + " Failed doc: " + tojson(encryptedDoc));
                }
            }
            // For each field that should not be encrypted. Some documents may not contain all of
            // the fields.
            for (let noEncrypt of test.notEncryptedPaths) {
                const curField = extractField(encryptedDoc, noEncrypt);
                if (typeof curField !== "undefined") {
                    assert(!(curField instanceof BinData),
                           tojson(test) + " Failed doc: " + tojson(encryptedDoc));
                }
            }
        }
    }

    // Make sure passthrough fields are present.
    const passThroughFields = {
        writeConcern: {w: "majority", wtimeout: 5000},
        "$audit": "auditString",
        "$client": "clientString",
        "$configServerState": 2,
        "allowImplicitCollectionCreation": true,
        "$oplogQueryData": false,
        "$readPreference": 7,
        "$replData": {data: "here"},
        "$clusterTime": "now",
        "maxTimeMS": 500,
        "readConcern": 1,
        "databaseVersion": 2.2,
        "shardVersion": 4.6,
        "tracking_info": {"found": "it"},
        "txnNumber": 7,
        "autocommit": false,
        "coordinator": false,
        "startTransaction": "now",
        // These two fields are added to the result if not set in the command. When we test them,
        // it is to test that the default values are overwritten.
        "ordered": false,
        "bypassDocumentValidation": true,
        // Excluded because errors with invalid arguments.
        // "$queryOptions": {limit: 1},
        // "stmtId": Number(783),
        // "lsid": 4,
        // Excluded because field not recognized on insert.
        // "$gleStats": {"yes": 7, "no": 4},
        // "operationTime": 10,
        // Excluded because not allowed on OP_QUERY requests.
        // "$db": "test"
    };

    for (let field of Object.keys(passThroughFields)) {
        let insertCommand = {
            insert: "test.foo",
            documents: [{"foo": "bar"}],
            jsonSchema: {type: "object", properties: {bar: encryptDoc}},
        };
        insertCommand[field] = passThroughFields[field];
        let expectedResult = {
            insert: "test.foo",
            documents: [{"foo": "bar"}],
            bypassDocumentValidation: false,
            ordered: true,
        };
        expectedResult[field] = passThroughFields[field];
        const result = assert.commandWorked(testDb.runCommand(insertCommand));
        // The shell runs commands inside sessions, and therefore will append the
        // logical session ID under the hood. We ignore it by deleting it from the object
        // containing the response.
        delete result["result"]["lsid"];
        assert.eq(Object.keys(result["result"]).length, Object.keys(expectedResult).length);
        for (let key of Object.keys(expectedResult)) {
            assert.eq(expectedResult[key], result["result"][key]);
        }
    }

    mongocryptd.stop();
}());
