/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

function smokeTest() {
    const uri = 'mongodb://' + db.getMongo().host;
    const dbConnectionName = "db1";
    const dbName = "test";
    const inputCollName = "testin";
    const inputColl = db.getSiblingDB(dbName)[inputCollName];
    const outputCollName = "testout";
    const outputColl = db.getSiblingDB(dbName)[outputCollName];
    const spName = "sp1";
    const connectionRegistry = [{name: dbConnectionName, type: 'atlas', options: {uri: uri}}];
    outputColl.drop();

    // Calls streams_startStreamProcessor with validateOnly: true.
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: dbConnectionName,
                    db: dbName,
                    coll: outputCollName,
                    allowedLateness: {size: NumberInt(0), unit: "second"}
                }
            },
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ],
        connections: connectionRegistry,
        options: {validateOnly: true},
    });
    assert.commandWorked(result);

    // Insert some documents into the "input column".
    inputColl.insert([
        {id: 0, value: 1},
        {id: 1, value: 1},
        {id: 2, value: 1},
        {id: 3, value: 1},
    ]);
    // Validate the streamProcessor was not actually started.
    // Validate nothing shows up in listStreamProcessors.
    result = db.runCommand({streams_listStreamProcessors: ''});
    assert.commandWorked(result);
    assert.eq(result["streamProcessors"].length, 0, result);
    // Wait 5 seconds and verify nothing is in the output even though we sent some input.
    sleep(3000);
    assert.eq(0, outputColl.find({}).toArray().length);

    // Calls streams_startStreamProcessor with { validateOnly: true } with a few
    // invalid requests. Verify the command fails.
    let invalidCmdRequests = [
        {
            streams_startStreamProcessor: '',
            name: spName,
            pipeline: [
                {
                    $source: {
                        connectionName: dbConnectionName,
                        db: dbName,
                        coll: outputCollName,
                        allowedLateness: {size: NumberInt(0), unit: "second"},
                        // Invalid field name.
                        foo: 1
                    }
                },
                {
                    $merge:
                        {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}
                }
            ],
            connections: connectionRegistry,
            options: {validateOnly: true},
        },
        {
            streams_startStreamProcessor: '',
            name: spName,
            pipeline: [
                {
                    $source: {
                        connectionName: dbConnectionName,
                        db: dbName,
                        coll: outputCollName,
                    }
                },
                // $group not supported at the top level, only inside windows.
                {$group: {_id: null, out: {$count: {}}}},
                {
                    $merge:
                        {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}
                }
            ],
            connections: connectionRegistry,
            options: {validateOnly: true},
        },
        {
            streams_startStreamProcessor: '',
            name: spName,
            pipeline: [
                {
                    $source: {
                        connectionName: "thisDbDoesNotExist",
                        db: dbName,
                        coll: outputCollName,
                    }
                },
                {
                    $merge:
                        {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}
                }
            ],
            connections: connectionRegistry,
            options: {validateOnly: true},
        },
        {
            streams_startStreamProcessor: '',
            name: spName,
            pipeline: [
                {
                    $source: {
                        connectionName: dbConnectionName,
                        db: dbName,
                        coll: outputCollName,
                    }
                },
                {
                    $merge:
                        {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}
                }
            ],
            connections: connectionRegistry,
            options: {
                validateOnly: true,
                dlq: {
                    connectionName: "thisDlqDoesntExist",
                    db: dbName,
                    coll: outputCollName,
                }
            },
        }
    ];

    for (let cmd of invalidCmdRequests) {
        assert.commandFailed(db.runCommand(cmd));
    }
}

smokeTest();
}());