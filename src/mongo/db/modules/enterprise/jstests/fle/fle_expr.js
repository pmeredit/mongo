/**
 * Tests mongocryptd handling $expr in Match Expressions.
 */

(function() {
'use strict';
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const uuid = UUID();

// Note that in the following schema, two fields share the same keyId for FLE1, but this isn't
// supportedin FLE2.
let schema = generateSchema({
    ssn: {
        encrypt: {
            algorithm: kDeterministicAlgo,
            keyId: [fle2Enabled() ? UUID() : uuid],
            bsonType: "long"
        }
    },
    'user.account': {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}},
    friendSsn: {
        encrypt: {
            algorithm: kDeterministicAlgo,
            keyId: [fle2Enabled() ? UUID() : uuid],
            bsonType: "long"
        }
    },
    incompatableEncryptionField:
        {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}}
},
                            "test.test");

function getValueAtPath(obj, path) {
    const parts = path.split('.');
    if (parts.length === 0) {
        return obj;
    }
    if (parts.length === 1) {
        return obj[path];
    }

    const firstPart = parts[0];
    return getValueAtPath(obj[parts[0]], parts.slice(1).join('.'));
}

function assertEncryptedFieldAtPath(res, path) {
    assert(getValueAtPath(res.result.filter, path) instanceof BinData, tojson(res));
    assert.eq(true, res.hasEncryptionPlaceholders);
}

let res = null;
let command = Object.assign({
    find: "test",
    filter: {'$expr': {'$eq': ['$ssn', '$friendSsn']}},
},
                            schema);

// Two encrypted fields can be compared if their encryption metadata matches. This is forbidden in
// FLE 2.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6334105);
} else {
    res = assert.commandWorked(testDB.runCommand(command));
    assert.eq(false, res.hasEncryptionPlaceholders);
}

command.filter = {
    '$expr': {'$eq': ['$ssn', '$incompatableEncryptionField']}
};
assert.commandFailedWithCode(testDB.runCommand(command), [31100, 6334105]);

// Constants are marked for encryption in comparisons to encrypted fields.
command.filter = {
    '$expr': {'$eq': ['$ssn', NumberLong(123456789)]}
};
res = assert.commandWorked(testDB.runCommand(command));
assertEncryptedFieldAtPath(res, '$expr.$eq.1.$const');

// Constants are encrypted in $cond expressions nested with an equality to an encrypted field
// path.
command.filter = {
    '$expr': {
        '$eq': [
            '$ssn',
            {
                '$cond': {
                    'if': {'$gte': ['$score', 25]},
                    'then': NumberLong(123456),
                    'else': '$friendSsn'

                }
            }
        ]
    }

};
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6334105);
} else {
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$cond.1.$const');
}

// Non-equality comparison with an unencrypted fields passes.
command.filter = {
    '$expr': {'$gte': ['$score', 25]}
};
res = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, res.hasEncryptionPlaceholders);

// Invalid operations within $expr on encrypted fields correctly fail.
command.filter = {
    '$expr': {'$gte': ['$ssn', 25]}
};
assert.commandFailedWithCode(testDB.runCommand(command), [31110, 6331102]);

// Inequality between an encrypted field and a constant marks the constant for encryption.
command.filter = {
    '$expr': {'$ne': ['$ssn', NumberLong(123)]}
};
res = assert.commandWorked(testDB.runCommand(command));
assertEncryptedFieldAtPath(res, '$expr.$ne.1.$const');

// Comparing an encrypted field to an unencrypted field fails.
command.filter = {
    '$expr': {
        '$eq': ['$ssn', '$score']

    }
};
assert.commandFailedWithCode(testDB.runCommand(command), [31099, 6334105]);

// Encryption placeholders within $and expressions.
command.filter = {
    '$expr':
        {'$and': [{'$eq': ['$ssn', NumberLong(123)]}, {'$ne': ['$friendSsn', NumberLong(456)]}]}
};
res = assert.commandWorked(testDB.runCommand(command));
assertEncryptedFieldAtPath(res, '$expr.$and.0.$eq.1.$const');
assertEncryptedFieldAtPath(res, '$expr.$and.1.$ne.1.$const');

// $concat not supported when comparing to encrypted fields.
command.filter = {
    '$expr': {'$eq': ['$ssn', {'$concat': ['hi', 'world']}]}
};
assert.commandFailedWithCode(testDB.runCommand(command), [31117, 6334105]);

command.filter = {
    '$expr': {'$eq': ['$summary', {'$concat': ['one', ' ', 'two']}]}
};
res = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, res.hasEncryptionPlaceholders);

command.filter = {
    '$expr': {
        '$switch': {
            branches: [
                {'case': {'$eq': ['$ssn', NumberLong(123)]}, 'then': 'hello'},
                {'case': {'$eq': ['$ssn', NumberLong(456)]}, 'then': 'another answer'}
            ],
            'default': 'default'
        }
    }
};
res = assert.commandWorked(testDB.runCommand(command));
assertEncryptedFieldAtPath(res, '$expr.$switch.branches.0.case.$eq.1.$const');
assertEncryptedFieldAtPath(res, '$expr.$switch.branches.1.case.$eq.1.$const');

// $ifNull properly marks constants for encryption.
command.filter = {
    $expr: {$eq: ['$ssn', {$ifNull: ['$ssn', NumberLong(123456)]}]}
};
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6334105);
} else {
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.1.$const');
}

// Make sure that even non-sensical but syntactically correct $ifNulls correctly mark literals
// for encryption.
command.filter = {
    $expr: {$eq: ['$ssn', {$ifNull: [NumberLong(123456), NumberLong(19234)]}]}
};
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6334105);
} else {
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.0.$const');
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.1.$const');
}

// Literals should be marked even when the encrypted field is inside of $ifNull.
command.filter = {
    $expr: {$eq: [NumberLong(1234), {$ifNull: ['$ssn', NumberLong(1234)]}]}
};
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.0.$const');
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.1.$const');
}

// Conflicts in encryption between fields should result in an error.
command.filter = {
    $expr: {$eq: ['$foo', {$ifNull: ['$ssn', '$foo2']}]}
};
res = assert.commandFailedWithCode(testDB.runCommand(command), [31098, 6331102]);

command.filter = {
    $expr: {$eq: ['$ssn', NumberLong(123)]}
};
res = assert.commandWorked(testDB.runCommand(command));
assertEncryptedFieldAtPath(res, '$expr.$eq.1.$const');

// Referencing a prefix of an encrypted field in a comparison should fail.
command.filter = {
    $expr: {$eq: ['$user', {account: "123"}]}
};
assert.commandFailedWithCode(testDB.runCommand(command), [31131, 6334105]);

// Comparing an encryted field to an object should fail.
command.filter = {
    $expr: {$eq: ['$user.account', {obj: 123}]}
};
assert.commandFailedWithCode(testDB.runCommand(command), [31117, 6334105]);

// $expr which does not reference an encrypted field is allowed.
command.filter = {
    $expr: {$eq: ['$foo', "123"]}
};
res = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, res.hasEncryptionPlaceholders);

// Comparing a randomly encrypted node to a constant should result in an error.
schema = generateSchema({
    ssn: {
        encrypt: {algorithm: kRandomAlgo, keyId: [fle2Enabled() ? UUID() : uuid], bsonType: "long"}
    },
    friendSsn: {
        encrypt: {algorithm: kRandomAlgo, keyId: [fle2Enabled() ? UUID() : uuid], bsonType: "long"}
    },
},
                        "test.test");
command.filter = {
    $expr: {$eq: ['$ssn', NumberLong(123)]}
};
assert.commandFailedWithCode(testDB.runCommand(Object.assign(command, schema)), 31158);
command.filter = {
    $expr: {$eq: ['$ssn', '$friendSsn']}
};
assert.commandFailedWithCode(testDB.runCommand(Object.assign(command, schema)), [31158, 6334105]);

mongocryptd.stop();
})();
