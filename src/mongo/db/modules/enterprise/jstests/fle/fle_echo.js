/**
 * Validate that mongocryptd/mongocsfle correctly echoes passthrough fields that aren't needed for
 * query analysis.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {kDeterministicAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();

mongocryptd.start();

const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");

const unencryptedSchema = generateSchema({foo: {type: "string"}}, "test.foo");
const fooEncryptedSchema = generateSchema({
    foo: {
        encrypt: {
            algorithm: kDeterministicAlgo,
            keyId: [UUID("4edee966-03cc-4525-bfa8-de8acd6746fa")],
            bsonType: "long"
        }
    }
},
                                          "test.foo");

let cmds = [
    {cmdName: "find", cmd: {find: "foo", filter: {_id: 1}}, isReadOnly: true},
    {cmdName: "count", cmd: {count: "foo", query: {_id: 1}}, isReadOnly: true},
    {
        cmdName: "findAndModify",
        cmd: {findAndModify: "foo", query: {foo: NumberLong(1)}, update: {$inc: {score: 1.0}}}
    },
    // old name
    {
        cmdName: "findAndModify",
        cmd: {findandmodify: "foo", query: {foo: NumberLong(1)}, update: {$inc: {score: 1.0}}}
    },
    {
        cmdName: "aggregate",
        cmd: {aggregate: "foo", pipeline: [{$match: {foo: NumberLong(1)}}], cursor: {}}
    },
    {cmdName: "insert", cmd: {insert: "foo", documents: [{foo: NumberLong(1)}]}},
    {
        cmdName: "update",
        cmd: {update: "foo", updates: [{q: {foo: NumberLong(1)}, u: {"$set": {a: 2}}}]}
    },
    {cmdName: "delete", cmd: {delete: "foo", deletes: [{q: {foo: NumberLong(1)}, limit: 1}]}}
];

// Distinct is only supported with FLE 1.
if (!fle2Enabled()) {
    cmds.push({
        cmdName: "distinct",
        cmd: {distinct: "foo", query: {_id: 1}, key: "_id"},
        isReadOnly: true
    });
}

cmds.forEach(element => {
    // Make sure no json schema fails
    assert.commandFailed(testDB.runCommand(element.cmd));

    // NOTE: This mutates element so it now has jsonSchema
    Object.extend(element.cmd, unencryptedSchema);

    // Make sure json schema works
    const ret1 = assert.commandWorked(testDB.runCommand(element.cmd));
    assert.eq(ret1.hasEncryptionPlaceholders, false, ret1);
    assert.eq(ret1.schemaRequiresEncryption, false, ret1);

    // Test that generic "passthrough" command arguments are correctly echoed back from
    // mongocryptd.
    const passthroughFields = {
        "writeConcern": {w: "majority", wtimeout: 5000},
        "$audit": {
            $impersonatedUser: {"user": "testUser", "db": "test"},
            $impersonatedRoles: [{"role": "readWrite", "db": "test"}]
        },
        "$client": {},
        "$configServerState": {},
        "allowImplicitCollectionCreation": true,
        "$oplogQueryData": NumberInt(0),
        "$readPreference": {"mode": "primary"},
        "$replData": {data: "here"},
        "$clusterTime": {
            clusterTime: Timestamp(1, 1),
            signature: {hash: BinData(0, "AAAAAAAAAAAAAAAAAAAAAAAAAAA="), keyId: NumberLong(0)}
        },
        "maxTimeMS": 500,
        "readConcern": {level: "majority"},
        "databaseVersion": {
            uuid: new UUID("ebee4d32-ef8b-40cb-b75e-b45fbe4042dc"),
            timestamp: new Timestamp(1691525961, 12),
            lastMod: NumberInt(5),
        },
        "shardVersion": {e: ObjectId(), t: Timestamp(1, 2), v: Timestamp(1, 1)},
        "tracking_info": {"found": "it"},
        "txnNumber": NumberLong(7),
        "autocommit": false,
        "coordinator": false,
        "startTransaction": true,
        "lsid": {id: BinData(4, "QlLfPHTySm6tqfuV+EOsVA==")},
    };

    // Switch to the schema containing encrypted fields.
    Object.assign(element.cmd, fooEncryptedSchema);

    // Merge the passthrough fields with the current command object.
    Object.assign(element.cmd, passthroughFields);

    const stmtId = NumberInt(1);

    // stmtId is not automatically passed through for commands that do not use it.
    if (!element.isReadOnly) {
        element.cmd.stmtId = stmtId;
    }

    const passthroughResult = assert.commandWorked(testDB.runCommand(element.cmd));

    // Verify that each of the passthrough fields is included in the result.
    for (let field in passthroughFields) {
        assert.eq(passthroughResult.result[field], passthroughFields[field], passthroughResult);
    }

    if (!element.isReadOnly) {
        assert.eq(passthroughResult.result["stmtId"], stmtId, passthroughResult);
    }

    // Verify that the 'schemaRequiresEncryption' bit is correctly set.
    assert.eq(passthroughResult.schemaRequiresEncryption, true, passthroughResult);

    // The '$db' field is always removed by cryptd.
    assert(!passthroughResult.hasOwnProperty("$db"));

    // Commands name should hold the collection name
    assert.eq(passthroughResult.result[element.cmdName], "foo", passthroughResult);
});

mongocryptd.stop();
