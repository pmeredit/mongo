/**
 * Tests mongocryptd handling $expr in Match Expressions.
 */

(function() {
    'use strict';
    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const uuid = UUID();

    const sampleSchema = {
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [uuid],
                    bsonType: "long"
                }
            },
            user: {
                type: "object",
                properties: {
                    account: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [UUID()],
                            bsonType: "string"
                        }
                    },

                }
            },
            friendSsn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [uuid],
                    bsonType: "long"
                }
            },
            incompatableEncryptionField: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [UUID()],
                    bsonType: "long"

                }
            },
            score: {bsonType: "long"},
            summary: {bsonType: "string"}
        }
    };

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
    let command = {
        find: "test",
        filter: {'$expr': {'$eq': ['$ssn', '$friendSsn']}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    };
    // Two encrypted fields can be compared if their encryption metadata matches.
    res = assert.commandWorked(testDB.runCommand(command));
    assert.eq(false, res.hasEncryptionPlaceholders);

    command.filter = {'$expr': {'$eq': ['$ssn', '$incompatableEncryptionField']}};
    assert.commandFailedWithCode(testDB.runCommand(command), 31100);

    // Constants are marked for encryption in comparisons to encrypted fields.
    command.filter = {'$expr': {'$eq': ['$ssn', NumberLong(123456789)]}};
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
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$cond.1.$const');

    // Non-equality comparison with an unencrypted fields passes.
    command.filter = {'$expr': {'$gte': ['$score', 25]}};
    res = assert.commandWorked(testDB.runCommand(command));
    assert.eq(false, res.hasEncryptionPlaceholders);

    // Invalid operations within $expr on encrypted fields correctly fail.
    command.filter = {'$expr': {'$gte': ['$ssn', 25]}};
    assert.commandFailedWithCode(testDB.runCommand(command), 31110);

    // Inequality between an encrypted field and a constant marks the constant for encryption.
    command.filter = {'$expr': {'$ne': ['$ssn', NumberLong(123)]}};
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$ne.1.$const');

    // Comparing an encrypted field to an unencrypted field fails.
    command.filter = {
        '$expr': {
            '$eq': ['$ssn', '$score']

        }
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31099);

    // Encryption placeholders within $and expressions.
    command.filter = {
        '$expr': {
            '$and':
                [{'$eq': ['$ssn', NumberLong(123)]}, {'$ne': ['$friendSsn', NumberLong(456)]}]
        }
    };
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$and.0.$eq.1.$const');
    assertEncryptedFieldAtPath(res, '$expr.$and.1.$ne.1.$const');

    // $concat not supported when comparing to encrypted fields.
    command.filter = {'$expr': {'$eq': ['$ssn', {'$concat': ['hi', 'world']}]}};
    assert.commandFailedWithCode(testDB.runCommand(command), 31117);

    command.filter = {'$expr': {'$eq': ['$summary', {'$concat': ['one', ' ', 'two']}]}};
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
    command.filter = {$expr: {$eq: ['$ssn', {$ifNull: ['$ssn', NumberLong(123456)]}]}};
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.1.$const');

    // Make sure that even non-sensical but syntactically correct $ifNulls correctly mark literals
    // for encryption.
    command.filter = {$expr: {$eq: ['$ssn', {$ifNull: [NumberLong(123456), NumberLong(19234)]}]}};
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.0.$const');
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.1.$const');

    // Literals should be marked even when the encrypted field is inside of $ifNull.
    command.filter = {$expr: {$eq: [NumberLong(1234), {$ifNull: ['$ssn', NumberLong(1234)]}]}};
    res = assert.commandWorked(testDB.runCommand(command));
    assertEncryptedFieldAtPath(res, '$expr.$eq.0.$const');
    assertEncryptedFieldAtPath(res, '$expr.$eq.1.$ifNull.1.$const');

    // Conflicts in encryption between fields should result in an error.
    command.filter = {$expr: {$eq: ['$foo', {$ifNull: ['$ssn', '$foo2']}]}};
    res = assert.commandFailedWithCode(testDB.runCommand(command), 31098);

    mongocryptd.stop();
})();
