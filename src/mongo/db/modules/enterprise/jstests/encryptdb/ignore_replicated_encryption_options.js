/**
 * Tests that replicating encryption options to a non-encrypted secondary, either by steady state
 * replication or by logical intial sync, does not cause the secondary node to fail. The encryption
 * options should be ignored.
 * @tags: [
 *   uses_pykmip,
 *   incompatible_with_s390x,
 *   # TODO (SERVER-89668): Remove tag. Currently incompatible due to collection options
 *   # containing the recordIdsReplicated:true option, which this test dislikes.
 *   exclude_when_record_ids_replicated,
 * ]
 */

// TODO SERVER-81069: Remove this test as it is unnecessary.

import {
    killPyKMIPServer,
    startPyKMIPServer
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

// Collection metadata will be different on the secondary because encryption options are removed.
// A hash check would fail.
TestData.skipCheckDBHashes = true;

const testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";
const kmipServerPort = 6572;

function createNodeConfigWithEncryption(params) {
    const defaultParams = {
        enableEncryption: "",
        kmipServerName: "127.0.0.1",
        kmipPort: kmipServerPort,
        kmipServerCAFile: "jstests/libs/trusted-ca.pem",
        encryptionCipherMode: "AES256-CBC",
        setParameter: {"failpoint.allowEncryptionOptionsInCreationString": '{mode: "alwaysOn"}'},
    };

    const opts = Object.merge(defaultParams, params);

    if (opts.kmipClientCertificateSelector !== undefined) {
        // Certificate selector specified,
        // no need to specify a certificate file.
    } else if (/OpenSSL/.test(getBuildInfo().openssl.running)) {
        // Linux (via OpenSSL) supports encryption, use it.
        opts.kmipClientCertificateFile = testDir + "libs/trusted_client_password_protected.pem";
        opts.kmipClientCertificatePassword = "qwerty";
    } else {
        // Windows and Apple SSL providers don't support encrypted PEM files.
        opts.kmipClientCertificateFile = "jstests/libs/trusted-client.pem";
    }

    return opts;
}

function doCatalogOpsWithExplicitEncryption(db) {
    const storageEngineOpts = {
        storageEngine: {
            wiredTiger: {
                configString:
                    "allocation_size=4KB,encryption=(keyid=\"admin\",name=AES256-CBC),internal_page_max=4KB"
            }
        }
    };

    // Create a collection with no options, replication / cloning should not receive implicit
    // encryption opts.
    assert.commandWorked(db.createCollection('collWithNoOpts'));

    // Create an index with explicit encryption opts to test index option replication / cloning.
    assert.commandWorked(db['collWithNoOpts'].createIndex({a: 1}, storageEngineOpts));

    // Insert a document, to trigger an index build for the following createIndex.
    assert.commandWorked(db['collWithNoOpts'].insert({a: "test", b: "test"}));
    assert.commandWorked(db['collWithNoOpts'].createIndex({b: 1}, storageEngineOpts));

    // Explicitly set encryption option on collection, to test collection option replication /
    // cloning.
    assert.commandWorked(db.createCollection('collWithExplicitEncryption', storageEngineOpts));
}

function assertSecondaryHasNotReplicatedEncryption(db) {
    // For reference see options used in doCatalogOpsWithExplicitEncryption. Only encryption options
    // should have been removed.
    const sanitizedStorageEngineOpts = {
        storageEngine: {wiredTiger: {configString: "allocation_size=4KB,internal_page_max=4KB"}}
    };

    // Query the collection infos and assert only encryption was removed from options.
    const infos = db.getCollectionInfos({name: "collWithExplicitEncryption"});
    assert.eq(infos[0].options, sanitizedStorageEngineOpts);

    // Query indexes and assert only encryption options are removed.
    const indexes = db['collWithNoOpts'].getIndexes();
    indexes.forEach(index => {
        if (JSON.stringify(index.key) != JSON.stringify({"_id": 1})) {
            assert.eq(index.storageEngine, sanitizedStorageEngineOpts.storageEngine);
        }
    });
}

function testInitialSyncWithEncryptionConfig() {
    // Initiate replica set with encryption.
    const nodeOptionsWithEncryption = createNodeConfigWithEncryption({});
    const replSet =
        new ReplSetTest({name: jsTestName(), nodes: 1, nodeOptions: nodeOptionsWithEncryption});
    replSet.startSet();
    replSet.initiate();

    doCatalogOpsWithExplicitEncryption(replSet.getPrimary().getDB('testdb'));

    // Add node without encryption enabled, if encryption options are cloned and not properly
    // sanitized, the node should crash during initial sync.
    replSet.add({});
    replSet.reInitiate();
    replSet.awaitSecondaryNodes();

    assertSecondaryHasNotReplicatedEncryption(replSet.getSecondary().getDB('testdb'));
    replSet.stopSet();
}

function testSteadyStateReplicationWithEncryptionConfig() {
    // Initiate replica set with one node using encryption, and one without.
    const nodeOptionsWithEncryption = createNodeConfigWithEncryption({});
    const replSet = new ReplSetTest({name: jsTestName(), nodes: [nodeOptionsWithEncryption, {}]});
    replSet.startSet();
    replSet.initiate();

    doCatalogOpsWithExplicitEncryption(replSet.getPrimary().getDB('testdb'));

    // Wait for oplog entries with encryption configuration to be replicated, if encryption options
    // are replicated and not properly sanitized, the secondary node should crash during oplog
    // application.
    replSet.awaitReplication();

    assertSecondaryHasNotReplicatedEncryption(replSet.getSecondary().getDB('testdb'));
    replSet.stopSet();
}

const kmipServerPid = startPyKMIPServer(kmipServerPort);

testInitialSyncWithEncryptionConfig();
testSteadyStateReplicationWithEncryptionConfig();

// Cleanup KMIP server.
killPyKMIPServer(kmipServerPid);
