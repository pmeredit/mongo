// Tests `local`, `remote` and `intermediates` fields are present in audit logs when connected with
// or without a load balancer.

import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {get_ipaddr} from "jstests/libs/host_ipaddr.js";
import {ProxyProtocolServer} from "jstests/sharding/libs/proxy_protocol.js";
import {
    AuditSpooler,
    ShardingFixture,
    StandaloneFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kATypes = {
    kCreateDatabase: {mongo: "createDatabase", OCSF: "create_database"},
    kCreateCollection: {mongo: "createCollection", OCSF: "create_collection"},
    kCreateIndex: {mongo: "createIndex", OCSF: "create_index"},
    kDropDatabase: {mongo: "dropDatabase", OCSF: "drop_database"},
    kDropCollection: {mongo: "dropCollection", OCSF: "drop_collection"},
    kDropIndex: {mongo: "dropIndex", OCSF: "drop_index"},
    kRenameCollection: {mongo: "renameCollection", OCSF: "rename_collection_destination"}
};

const kDbName = "test";
const kCollectionRenamed = "test_collection_rename";
const kCollectionName = "test_collection";
const kNamespace = `${kDbName}.${kCollectionName}`;
const kNamespaceRenamed = `${kDbName}.${kCollectionRenamed}`;

const kSchemaMongo = "mongo";
const kSchemaOCSF = "ocsf";

const kUserName = "admin";
const kPassword = "psw";
const klocalHost = "127.0.0.1";

function getAddress(host) {
    const match = host.match(/ip-([\d-]+)(?::|\.)/);
    return match ? match[1].replace(/-/g, '.') : null;
}

/* For standalone:
 * - host is the mongod address
 * - port is the mongod port
 * - audit AuditSpool for mongod
 *
 * For sharded clusters:
 * - host is the address of the shard
 * - port is the port of the shard
 * - audit AuditSpool for shard
 * - expectedIntermediates includes the mongos address and port
 */

function setup(fixture, schema, isSharded, proxy_server = null) {
    let opts = {};

    if (proxy_server) {
        opts = {
            other: {mongosOptions: {setParameter: {loadBalancerPort: proxy_server.getEgressPort()}}}
        };
    }

    if (isSharded) {
        opts.other = opts.other || {};
        opts.other.rsOptions = {
            auditPath: MongoRunner.dataPath + "shard_audit.log",
            auditDestination: "file",
            auditFormat: "JSON"
        };
    }

    let {conn, audit, admin} = fixture.startProcess(opts, "JSON", schema);
    let host = klocalHost;
    let port = conn.port;

    let shellPort = conn.getShellPort();
    let intermediates = [];

    if (isSharded) {
        const st = fixture.getShardingTest();
        const mongosPort = conn.port;

        audit = new AuditSpooler(MongoRunner.dataPath + "shard_audit.log");

        // Windows hosts return the name of the server but we want the actual ip address
        if (_isWindows()) {
            host = get_ipaddr();
        } else {
            host = getAddress(st.rs0.getPrimary().host);
        }

        port = st.rs0.getPrimary().port;

        intermediates.push({ip: klocalHost, port: mongosPort});
    }

    if (proxy_server) {
        const ingressPort = proxy_server.getIngressPort();
        const egressPort = proxy_server.getEgressPort();

        const uri = `mongodb://127.0.0.1:${ingressPort}/?loadBalanced=true`;
        conn = new Mongo(uri);
        admin = conn.getDB("admin");

        shellPort = conn.getShellPort();

        intermediates = [];
        intermediates.push({ip: klocalHost, port: egressPort});
        intermediates.push({ip: klocalHost, port: ingressPort});
    }

    const local = {ip: host, port: port};
    const remote = {ip: klocalHost, port: shellPort};
    const expectedAddresses = {local, remote, intermediates};

    return {conn, audit, admin, expectedAddresses};
}

function assertEntry(audit, schema, atype, param, expectedAddresses) {
    const schemaType = schema === kSchemaMongo ? atype.mongo : atype.OCSF;
    const entry = audit.assertEntryRelaxed(schemaType, param);

    assert(entry.hasOwnProperty('local'));
    assert(entry.hasOwnProperty('remote'));
    assert.eq(Object.keys(entry.local).length, 2);
    assert.eq(Object.keys(entry.remote).length, 2);

    assert.docEq(expectedAddresses.local, entry.local);
    assert.docEq(expectedAddresses.remote, entry.remote);

    if (expectedAddresses.intermediates.length) {
        assert.docEq(expectedAddresses.intermediates, entry.intermediates);
    }
}

function runTest(fixture, schema, isSharded = false, proxy_server = null) {
    const {conn, audit, admin, expectedAddresses} = setup(fixture, schema, isSharded, proxy_server);
    assert.commandWorked(
        admin.runCommand({createUser: kUserName, pwd: kPassword, roles: ['root']}));
    assert(admin.auth(kUserName, kPassword));

    // TODO SERVER-83990: remove
    if (!FeatureFlagUtil.isPresentAndEnabled(admin, "ExposeClientIpInAuditLogs")) {
        fixture.stopProcess();

        return;
    }

    const testDB = conn.getDB(kDbName);

    assert.commandWorked(testDB[kCollectionName].insert({x: 1}));
    assertEntry(audit, schema, kATypes.kCreateDatabase, {ns: kDbName}, expectedAddresses);
    assertEntry(audit, schema, kATypes.kCreateCollection, {ns: kNamespace}, expectedAddresses);

    assert.commandWorked(testDB[kCollectionName].createIndex({field: 1}));
    assertEntry(audit, schema, kATypes.kCreateIndex, {ns: kNamespace}, expectedAddresses);

    assert.commandWorked(testDB[kCollectionName].renameCollection(kCollectionRenamed, true));
    assertEntry(audit,
                schema,
                kATypes.kRenameCollection,
                {old: kNamespace, new: kNamespaceRenamed},
                expectedAddresses);

    assert.commandWorked(testDB.dropDatabase());
    const dropCollPos = audit._auditLine;

    assertEntry(audit, schema, kATypes.kDropCollection, {ns: kNamespaceRenamed}, expectedAddresses);
    audit._auditLine = dropCollPos;

    assertEntry(audit, schema, kATypes.kDropIndex, {ns: kNamespaceRenamed}, expectedAddresses);
    audit._auditLine = dropCollPos;

    assertEntry(audit, schema, kATypes.kDropDatabase, {ns: kDbName}, expectedAddresses);

    fixture.stopProcess();
}

{
    jsTest.log("Testing standalone");
    const fixture = new StandaloneFixture();
    runTest(fixture, kSchemaMongo);
}

{
    jsTest.log("Testing sharded cluster");
    const fixture = new ShardingFixture();
    runTest(fixture, kSchemaMongo, true /* isSharded */);
}

// TODO: SERVER-100859: remove
// Proxy protocol server does not work on windows
if (_isWindows()) {
    quit();
}

{
    jsTest.log("Testing sharded cluster with load balanced connection with version 1");
    const ingressPort = allocatePort();
    const egressPort = allocatePort();

    const proxy_server = new ProxyProtocolServer(ingressPort, egressPort, 1);
    proxy_server.start();

    const fixture = new ShardingFixture();
    runTest(fixture, kSchemaMongo, true /* isSharded */, proxy_server);

    proxy_server.stop();
}

{
    jsTest.log("Testing sharded cluster with load balanced connection with version 2");
    const ingressPort = allocatePort();
    const egressPort = allocatePort();

    const proxy_server = new ProxyProtocolServer(ingressPort, egressPort, 2);
    proxy_server.start();

    const fixture = new ShardingFixture();
    runTest(fixture, kSchemaMongo, true /* isSharded */, proxy_server);

    proxy_server.stop();
}
