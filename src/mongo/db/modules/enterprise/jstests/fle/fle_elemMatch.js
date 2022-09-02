/*
 * Arrays are not supported in query analysis currently. This test verifies that fields within
 * embedded documents are not treated as top-level fields by query analysis.
 */
(function() {
'use strict';
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");
const mongocryptd = new MongoCryptD();
mongocryptd.start();
const dbName = jsTestName();
const testDB = mongocryptd.getConnection().getDB(dbName);
const uuid = UUID();
let schema = generateSchema({
    name: {
        encrypt: {
            bsonType: 'string',
            algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
            keyId: [uuid]
        }
    }
},
                            dbName + ".coll");

let cmd = Object.assign({
    "find": "coll",
    "filter": {
        "name": "encrypted",
        "bars": {
            "$elemMatch": {"name": "unencrypted"},
        }
    }
},
                        schema);
let res = assert.commandWorked(testDB.runCommand(cmd));

assert(res.result.filter.$and[0].name.$eq instanceof BinData, tojson(res));
assert.eq(res.result.filter.$and[1].bars.$elemMatch.name.$eq, "unencrypted", tojson(res));

mongocryptd.stop();
}());
