/**
 * Test which verifies that a stream flavored mongod properly handles a SIGTERM
 * request by calling stop on all active streamProcessors.
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {TestHelper} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {listStreamProcessors} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

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
rst.initiate(Object.extend(rst.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));
rst2.initiate(Object.extend(rst2.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));
const conn = rst.getPrimary();
const dbName = 'test';
const db = conn.getDB(dbName);
const conn2 = rst2.getPrimary();
const db2 = conn2.getDB(dbName);
const uri = 'mongodb://' + db2.getMongo().host;
const checkpointCollName = UUID().toString().split('"')[1];
const checkpointColl = db2.getSiblingDB(dbName)[checkpointCollName];
const kafkaName = "testKafka";
const inputTopicName = "topic1";
const memoryConnectionName = "__testMemory";
const connectionRegistry = [
    {
        name: kafkaName,
        type: 'kafka',
        options: {bootstrapServers: "localhost:9092", isTestKafka: true},
    },
    {name: memoryConnectionName, type: 'in_memory', options: {}},
];

// Start 3 streamProcessors.
const numProcessors = 3;
const checkpointTestHelpers = [];
for (let i = 0; i < numProcessors; ++i) {
    const helper = new TestHelper([] /* input */,
                                  [] /* middlePipeline */,
                                  null /* interval */,
                                  "kafka",
                                  true /* useNewCheckpointing */,
                                  true /* useRestoredExecutionPlan */,
                                  null /* writeDir */,
                                  null /* restoreDir */,
                                  db /* dbForTest */,
                                  db2 /* targetSourceMergeDb */);
    helper.run();
    checkpointTestHelpers.push(helper);
}

// Send a SIGTERM to the replset running the streamProcessor.
sendSigterm(rst);

// Validate there are two checkpoints for each streamProcssor, one for the
// start and one from the shutdown.
let checkpointIds = [];
for (const helper of checkpointTestHelpers) {
    const ids = helper.getCheckpointIds();
    for (const id of ids) {
        checkpointIds.push(id);
    }
}
jsTestLog(checkpointIds);
assert.eq(numProcessors * 2, checkpointIds.length);

// Stop the replsets.
rst.stopSet();
rst2.stopSet();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);