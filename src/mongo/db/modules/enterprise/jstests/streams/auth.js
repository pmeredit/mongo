/**
 * Test which verifies that stream processors can authenticate using x509 certficates when
 * connecting to a MongoDB server. At the time of writing, this means running a change stream
 * $source or a $merge stage.
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const CA_CERT = 'jstests/libs/ca.pem';
const SERVER_CERT = 'jstests/libs/server.pem';
const CLIENT_CERT = 'jstests/libs/client.pem';
const CLIENT_CERT_MISMATCHED_DN = 'jstests/libs/client_email.pem';

const rst = new ReplSetTest({
    name: "stream_auth_rst",
    nodes: 1,
    waitForKeys: false,
    nodeOptions: {
        tlsMode: "preferTLS",
        clusterAuthMode: "x509",
        tlsCertificateKeyFile: SERVER_CERT,
        tlsCAFile: CA_CERT,
        setParameter: {featureFlagStreams: true},
    }
});

rst.startSet();
rst.initiate(Object.extend(rst.getReplSetConfig(), {
    writeConcernMajorityJournalDefault: true,
}),
             null,
             {allNodesAuthorizedToRunRSGetStatus: false});

const conn = rst.getPrimary();
const admin = conn.getDB('admin');
const external = conn.getDB('$external');

const dbName = 'db';
const outputCollName = 'outputColl';
const writeCollName = 'writeToThisColl';

// Auth setup. Create an admin user, then create an x509 user that can read/write to any database.
// This mimicks what happens with authentication in an Atlas cluster: upon successful
// authentication, the client will recieve an x509 certificate that can be used connect to the
// instance.
admin.createUser({user: 'admin', pwd: 'pwd', roles: ['root']});
assert(admin.auth('admin', 'pwd'));

const X509USER = 'CN=client,OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US';
external.createUser({
    user: X509USER,
    roles:
        [{role: 'readWriteAnyDatabase', db: 'admin'}, {role: 'userAdminAnyDatabase', db: 'admin'}]
});

const db = conn.getDB(dbName);
const outputColl = db[outputCollName];
outputColl.drop();

const validConnectionName = 'conn1';
const invalidPemFileConnectionName = 'thisConnUsesANonExistentPemFile';
const invalidCaFileConnectionName = 'thisConnUsesANonExistentCaFile';
const validPemFileDoesNotMatchSubjectDN = 'thisConnUsesExistingCaFileThatDoesNotMatchSubjectDN';
const uri = 'mongodb://localhost:' + conn.port + '/?tls=true&authMechanism=MONGODB-X509';
const bogusPath = 'this/file/does/not/exist';
const connectionRegistry = [
    {
        name: validConnectionName,
        type: 'atlas',
        options: {uri: uri, pemFile: CLIENT_CERT, caFile: CA_CERT},
    },
    {
        name: invalidPemFileConnectionName,
        type: 'atlas',
        options: {uri: uri, pemFile: bogusPath, caFile: CA_CERT},
    },
    {
        name: invalidCaFileConnectionName,
        type: 'atlas',
        options: {uri: uri, pemFile: CLIENT_CERT, caFile: bogusPath},
    },
    {
        name: validPemFileDoesNotMatchSubjectDN,
        type: 'atlas',
        options: {uri: uri, pemFile: CLIENT_CERT_MISMATCHED_DN, caFile: CA_CERT},
    },

];
const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

const validationExpr = {
    $gt: ['$fullDocument._id', 9]
};
const processorName = 'authEnabledStreamProcessor';
let pipeline = [
    {$source: {connectionName: validConnectionName, db: dbName, coll: writeCollName}},
    {$validate: {validator: {$expr: validationExpr}, validationAction: 'dlq'}},
    {$merge: {into: {connectionName: validConnectionName, db: dbName, coll: outputCollName}}},
];
sp.createStreamProcessor(processorName, pipeline);
const processor = sp[processorName];

const dlqCollName = "validateDLQ";
const options = {
    dlq: {connectionName: validConnectionName, db: dbName, coll: dlqCollName},
    featureFlags: {},
};
assert.commandWorked(db.runCommand(processor.makeStartCmd(options)));

// Insert some documents. This should be picked up by our change stream $source.
const docs = [
    {_id: 5, a: 7, otherTimeField: Date.now()},
    {_id: 6, a: 33, otherTimeField: Date.now()},
    {_id: 7, a: 35, otherTimeField: Date.now()},
    {_id: 10, a: 133, otherTimeField: Date.now()},
    {_id: 17, a: 33, otherTimeField: Date.now()}
];
const writeColl = db[writeCollName];
assert.commandWorked(writeColl.insertMany(docs));

assert.soon(() => {
    const docs = outputColl.find({}).toArray();
    return docs.length == 2;
});

assert.commandWorked(db.runCommand(processor.makeStopCmd()));

const projection = {
    _id: '$fullDocument._id',
    a: '$fullDocument.a',
    otherTimeField: '$fullDocument.otherTimeField'
};

// The only documents that will have be written to 'outputColl' are those which passed the
// validition expression.
const validResults = outputColl.find({$expr: validationExpr}, projection).sort({_id: 1}).toArray();
const validDocs = docs.filter(doc => doc._id > 9);

// Verify that the contents of the collections match the valid documents.
assert.eq(validDocs, validResults, validResults);

// Verify that the documents which did not pass the validation expression were written to the DLQ
// collection.
const dlqColl = db[dlqCollName];

// The DLQ will store the change events in a 'doc' field. We run an aggregate to transform
// the DLQ documents such that we can use the original validation expression and projection.
const invalidResults = dlqColl
                           .aggregate([
                               {$replaceRoot: {newRoot: "$doc"}},
                               {$match: {$expr: {$not: [validationExpr]}}},
                               {$project: projection},
                               {$sort: {_id: 1}}
                           ])
                           .toArray();

const invalidDocs = docs.filter(doc => doc._id <= 9);
assert.eq(invalidDocs, invalidResults, invalidResults);

// Verify that the documents from both the output collection and the dlq collection form the
// original collection.
assert.eq(invalidDocs.concat(validDocs), docs);
assert.eq(invalidResults.concat(validResults), docs);

// Error cases. Verify that we fail to create stream processors with invalid file names.
const badPEMFileProcessorName = 'badPEM';
pipeline = [
    {$source: {connectionName: invalidPemFileConnectionName, db: dbName, coll: writeCollName}},
    {
        $merge:
            {into: {connectionName: invalidPemFileConnectionName, db: dbName, coll: outputCollName}}
    }
];
sp.createStreamProcessor(badPEMFileProcessorName, pipeline);

const badCAFileProcessorName = 'badCA';
pipeline = [
    {$source: {connectionName: invalidCaFileConnectionName, db: dbName, coll: writeCollName}},
    {
        $merge:
            {into: {connectionName: invalidCaFileConnectionName, db: dbName, coll: outputCollName}}
    }
];
sp.createStreamProcessor(badCAFileProcessorName, pipeline);

// While this stream processor has a valid PEM file, it will not be authenticated by 'CA_CERT'
// because its subject DN does not match.
const clientCertDoesNotMatchSubjectDNProcessorName = 'certDoesNotMatch';
pipeline = [
    {$source: {connectionName: validPemFileDoesNotMatchSubjectDN, db: dbName, coll: writeCollName}},
    {
        $merge: {
            into: {
                connectionName: validPemFileDoesNotMatchSubjectDN,
                db: dbName,
                coll: outputCollName
            }
        }
    }
];
sp.createStreamProcessor(clientCertDoesNotMatchSubjectDNProcessorName, pipeline);

const badPEMFileProcessor = sp[badPEMFileProcessorName];
const badCAFileProcessor = sp[badCAFileProcessorName];
const caFileDoesNotMatchSubjectDNProcessor = sp[clientCertDoesNotMatchSubjectDNProcessorName];

let result = db.runCommand(badPEMFileProcessor.makeStartCmd());
assert.commandFailed(result);
assert.eq(ErrorCodes.StreamProcessorAtlasConnectionError, result.code);

result = db.runCommand(badCAFileProcessor.makeStartCmd());
assert.commandFailed(result);
assert.eq(ErrorCodes.StreamProcessorAtlasConnectionError, result.code);

// Note: The bad SubjectDN will allow the initial connection to establish, but the streamProcessor
// will later see an error like:
//  not authorized on db to execute command { update: \"outputColl\", ordered: true, $db: \"db\",
//  lsid: { id: UUID(\"90999a3f-475c-42b7-baa7-4a479221b8b9\") }
assert.commandWorked(db.runCommand(caFileDoesNotMatchSubjectDNProcessor.makeStartCmd()));

const listCmd = {
    streams_listStreamProcessors: '',
    tenantId: TEST_TENANT_ID
};
result = db.runCommand(listCmd);
assert.eq(result["ok"], 1, result);
assert.eq(result["streamProcessors"].length, 1, result);
assert.commandWorked(writeColl.insert({}));

// Verify this streamProcessor goes into an error state.
assert.soon(() => {
    result = db.runCommand(listCmd);
    assert.eq(result["ok"], 1, result);
    assert.eq(result["streamProcessors"].length, 1, result);
    let sp = result["streamProcessors"][0];
    if (sp.hasOwnProperty("error")) {
        let errorText = "not authorized on db to execute command { update: \"outputColl\"";
        return sp["status"] == "error" &&
            sp["error"]["code"] == ErrorCodes.StreamProcessorAtlasUnauthorizedError &&
            sp["error"]["reason"].includes(errorText) && sp["error"]["retryable"] == true;
    } else {
        return false;
    }
});

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
rst.stopSet();