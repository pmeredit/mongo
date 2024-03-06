/**
 * Test which verifies that a stream flavored mongod properly handles a SIGTERM
 * request by calling stop on all active streamProcessors.
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
function sendSigterm(rst) {
    const originalPrimaryConn = rst.getPrimary();
    const SIGTERM = 15;
    rst.restart(originalPrimaryConn, {}, SIGTERM);
    rst.awaitNodesAgreeOnPrimary();
}

// rst runs the streamProcessor.
const rst = new ReplSetTest({
    name: "stream_sigterm_rst",
    nodes: 1,
    waitForKeys: false,
});
// rst2 is used for the checkpoint storage.
const rst2 = new ReplSetTest({
    name: "stream_sigterm_rst2",
    nodes: 1,
    waitForKeys: false,
});
// Setup the replsets.
rst.startSet();
rst2.startSet();
rst.initiateWithAnyNodeAsPrimary(
    Object.extend(rst.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));
rst2.initiateWithAnyNodeAsPrimary(
    Object.extend(rst2.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));
const conn = rst.getPrimary();
const dbName = 'test';
const db = conn.getDB(dbName);
const conn2 = rst2.getPrimary();
const db2 = conn2.getDB(dbName);
const uri = 'mongodb://' + db2.getMongo().host;
const checkpointCollName = UUID().toString().split('"')[1];
const checkpointColl = db2.getSiblingDB(dbName)[checkpointCollName];

// Start 3 streamProcessors.
const numProcessors = 3;
for (let i = 0; i < numProcessors; ++i) {
    let name = "sp" + i;
    let cmd = {
        streams_startStreamProcessor: '',
        name: name,
        pipeline: [
            {$source: {connectionName: "__testMemory"}},
            {$emit: {connectionName: "__testMemory"}}
        ],
        connections: [],
        processorId: name,
        tenantId: name,
        options: {
            checkpointOptions: {
                storage: {
                    uri: uri,
                    db: dbName,
                    coll: checkpointCollName,
                },
                debugOnlyIntervalMs: 99999999 /* long interval */
            },
        }
    };
    assert.commandWorked(db.runCommand(cmd));
}

// Send a SIGTERM to the replset running the streamProcessor.
sendSigterm(rst);

// Validate there are two checkpoints for each streamProcssor, one for the
// start and one from the shutdown.
let checkpointIds = checkpointColl.find({_id: {$regex: "^checkpoint"}}).sort({_id: -1}).toArray();
jsTestLog(checkpointIds);
assert.eq(numProcessors * 2, checkpointIds.length);

// Stop the replsets.
rst.stopSet();
rst2.stopSet();