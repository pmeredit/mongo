/**
 * Library functions for mongo and OCSF audit_remote_local_intermediate tests.
 */
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {get_ipaddr} from "jstests/libs/host_ipaddr.js";
import {
    AuditSpooler,
    AuditSpoolerOCSF
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kATypes = {
    kCreateUser: {mongo: "createUser", OCSF: "createUser"},
    kAuthenticate: {mongo: "authenticate", OCSF: "authenticate"},
    kCreateDatabase: {mongo: "createDatabase", OCSF: "create_database"},
    kCreateCollection: {mongo: "createCollection", OCSF: "create_collection"},
    kCreateIndex: {mongo: "createIndex", OCSF: "create_index"},
    kDropDatabase: {mongo: "dropDatabase", OCSF: "drop_database"},
    kDropCollection: {mongo: "dropCollection", OCSF: "drop_collection"},
    kDropIndex: {mongo: "dropIndex", OCSF: "drop_index"},
    kRenameCollection: {mongo: "renameCollection", OCSF: "rename_collection_source"}
};

const kDbName = "test";
const kCollectionRenamed = "test_collection_rename";
const kCollectionName = "test_collection";
const kNamespace = `${kDbName}.${kCollectionName}`;
const kNamespaceRenamed = `${kDbName}.${kCollectionRenamed}`;

const kUserName = "admin";
const kPassword = "psw";
const klocalHost = "127.0.0.1";

const kExpectedFields = {
    kAuthenticate: {mongo: {db: 'admin', user: kUserName}, OCSF: {}},
    kCreateUser: {mongo: {db: 'admin', user: kUserName}, OCSF: {authenticationRestrictions: []}},
    kCreateDatabase: {mongo: {ns: kDbName}, OCSF: {name: kDbName}},
    kCreateCollection: {mongo: {ns: kNamespace}, OCSF: {name: kNamespace}},
    kCreateIndex: {mongo: {ns: kNamespace}, OCSF: {name: `field_1@${kNamespace}`}},
    kDropDatabase: {mongo: {ns: kDbName}, OCSF: {name: kDbName}},
    kDropCollection: {mongo: {ns: kNamespaceRenamed}, OCSF: {name: kNamespaceRenamed}},
    kDropIndex: {mongo: {ns: kNamespaceRenamed}, OCSF: {name: `field_1@${kNamespaceRenamed}`}},
    kRenameCollection: {mongo: {old: kNamespace, new: kNamespaceRenamed}, OCSF: {name: kNamespace}},
};

export const kSchemaMongo = "mongo";
export const kSchemaOCSF = "ocsf";

function getAddress(host) {
    const match = host.match(/ip-([\d-]+)(?::|\.)/);
    return match ? match[1].replace(/-/g, '.') : null;
}

function getFixtureOptions(isSharded, proxy_server, schema) {
    let opts = {};
    const schemaOpt = (schema === kSchemaMongo) ? "mongo" : "OCSF";

    if (isSharded) {
        opts.other = opts.other || {};
        if (proxy_server) {
            opts.other.mongosOptions = {
                setParameter: {loadBalancerPort: proxy_server.getEgressPort()},
                auditPath: MongoRunner.dataPath + "audit.log",
                auditDestination: "file",
                auditFormat: "JSON",
                auditSchema: schemaOpt
            };
        } else {
            opts.other.mongosOptions = {
                auditPath: MongoRunner.dataPath + "audit.log",
                auditDestination: "file",
                auditFormat: "JSON",
                auditSchema: schemaOpt,
            };
        }

        opts.other.rsOptions = {
            auditPath: MongoRunner.dataPath + "shard_audit.log",
            auditDestination: "file",
            auditFormat: "JSON",
            auditSchema: schemaOpt,
        };
        opts.other.configOptions = {
            auditPath: MongoRunner.dataPath + "config_audit.log",
            auditDestination: "file",
            auditFormat: "JSON",
            auditSchema: schemaOpt,
        };
    }

    return opts;
}

function setupAuth(admin) {
    assert.commandWorked(
        admin.runCommand({createUser: kUserName, pwd: kPassword, roles: ['root']}));
    assert(admin.auth(kUserName, kPassword));
}

function assertAuditAuth(schema, isSharded, authExpectedAddress, audit) {
    const {configExpectedAddress, mongosExpectedAddress, mongodExpectedAddress} =
        authExpectedAddress;

    if (isSharded) {
        let auditConfig;
        if (schema === kSchemaMongo) {
            auditConfig = new AuditSpooler(MongoRunner.dataPath + "config_audit.log");
        } else {
            auditConfig = new AuditSpoolerOCSF(MongoRunner.dataPath + "config_audit.log");
        }

        assertEntry(auditConfig,
                    schema,
                    kATypes.kCreateUser,
                    kExpectedFields.kCreateUser,
                    configExpectedAddress);
        assertEntry(audit,
                    schema,
                    kATypes.kAuthenticate,
                    kExpectedFields.kAuthenticate,
                    mongosExpectedAddress);
    } else {
        assertEntry(
            audit, schema, kATypes.kCreateUser, kExpectedFields.kCreateUser, mongodExpectedAddress);
        assertEntry(audit,
                    schema,
                    kATypes.kAuthenticate,
                    kExpectedFields.kAuthenticate,
                    mongodExpectedAddress);
    }
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
    const opts = getFixtureOptions(isSharded, proxy_server, schema);

    let {conn, audit, admin} = fixture.startProcess(opts, "JSON", schema);
    let host = klocalHost;
    let port = conn.port;
    let auditShard;
    let configPort;
    let mongosPort;

    let shellPort = conn.getShellPort();
    let intermediates = [];

    if (isSharded) {
        const st = fixture.getShardingTest();
        mongosPort = conn.port;

        // We want the shard's audit log on sharded clusters rather than the mongos, since that will
        // contain audit entries propagated via $audit.
        if (schema === kSchemaMongo) {
            auditShard = new AuditSpooler(MongoRunner.dataPath + "shard_audit.log");
        } else {
            auditShard = new AuditSpoolerOCSF(MongoRunner.dataPath + "shard_audit.log");
        }

        // Windows hosts return the name of the server but we want the actual ip address
        host = _isWindows() ? get_ipaddr() : getAddress(st.rs0.getPrimary().host);
        port = st.rs0.getPrimary().port;
        admin = st.s0.getDB('admin');
        configPort = st.c0.port;

        intermediates.push({ip: klocalHost, port: mongosPort});
    }

    if (proxy_server) {
        const ingressPort = proxy_server.getIngressPort();
        mongosPort = proxy_server.getEgressPort();

        const uri = `mongodb://127.0.0.1:${ingressPort}/?loadBalanced=true`;
        conn = new Mongo(uri);
        admin = conn.getDB("admin");

        shellPort = conn.getShellPort();

        intermediates = [];
        intermediates.push({ip: klocalHost, port: mongosPort});
        intermediates.push({ip: klocalHost, port: ingressPort});
    }

    const expectedAddresses = {
        local: {ip: host, port: port},
        remote: {ip: klocalHost, port: shellPort},
        intermediates
    };
    const authExpectedAddress = {
        configExpectedAddress: {
            local: {ip: host, port: configPort},
            remote: expectedAddresses.remote,
            intermediates: expectedAddresses.intermediates
        },
        mongosExpectedAddress: {
            local: {ip: klocalHost, port: mongosPort},
            remote: expectedAddresses.remote,
            intermediates: proxy_server ? [expectedAddresses.intermediates[1]] : []
        },
        mongodExpectedAddress: {...expectedAddresses}
    };

    setupAuth(admin);

    // TODO SERVER-83990: remove
    if (!FeatureFlagUtil.isPresentAndEnabled(admin, "ExposeClientIpInAuditLogs")) {
        fixture.stopProcess();
        quit();
    }

    assertAuditAuth(schema, isSharded, authExpectedAddress, audit);

    return {conn, audit: auditShard || audit, admin, expectedAddresses};
}

function assertEntry(audit, schema, atype, param, expectedAddresses) {
    if (schema === kSchemaMongo) {
        const entry = audit.assertEntryRelaxed(atype.mongo, param.mongo);
        assert(entry.hasOwnProperty('local'));
        assert(entry.hasOwnProperty('remote'));
        assert.eq(Object.keys(entry.local).length, 2);
        assert.eq(Object.keys(entry.remote).length, 2);

        assert.docEq(expectedAddresses.local, entry.local);
        assert.docEq(expectedAddresses.remote, entry.remote);

        if (expectedAddresses.intermediates.length) {
            for (let i = 0; i < expectedAddresses.intermediates.length; i++) {
                assert.docEq(expectedAddresses.intermediates[i], entry.intermediates[i]);
            }
        } else {
            assert(!entry.hasOwnProperty('intermediates'));
        }
    } else {
        let entry;
        if (atype.OCSF === 'createUser' || atype.OCSF === 'authenticate') {
            entry = audit.assertUnmappedEntryRelaxed(atype.OCSF, param.OCSF);
        } else {
            entry = audit.assertEntityEntryRelaxed(atype.OCSF, param.OCSF);
        }

        assert(entry.hasOwnProperty('src_endpoint'));
        const expectedNumSrcEndpointFields = 2 +
            (expectedAddresses.intermediates.length
                 ? 1
                 : 0);  // Should be 3 if there are intermediates expected, otherwise 2.
        assert.eq(Object.keys(entry.src_endpoint).length, expectedNumSrcEndpointFields);

        // dst_endpoint only appears on some OCSF entries checked in this test.
        if (entry.dst_endpoint) {
            assert.eq(Object.keys(entry.dst_endpoint).length, 2);
            assert.docEq(expectedAddresses.local, entry.dst_endpoint);
        }

        // src_endpoint isn't an exact match to remote because it also may contain intermediates.
        assert.eq(expectedAddresses.remote.ip, entry.src_endpoint.ip);
        assert.eq(expectedAddresses.remote.port, entry.src_endpoint.port);
        if (expectedAddresses.intermediates.length) {
            for (let i = 0; i < expectedAddresses.intermediates.length; i++) {
                assert.docEq(expectedAddresses.intermediates[i],
                             entry.src_endpoint.intermediate_ips[i]);
            }
        } else {
            assert(!entry.src_endpoint.hasOwnProperty('intermediate_ips'));
        }
    }
}

export function runTest(fixture, schema, isSharded = false, proxy_server = null) {
    const {conn, audit, expectedAddresses} = setup(fixture, schema, isSharded, proxy_server);

    const testDB = conn.getDB(kDbName);

    assert.commandWorked(testDB[kCollectionName].insert({x: 1}));
    assertEntry(
        audit, schema, kATypes.kCreateDatabase, kExpectedFields.kCreateDatabase, expectedAddresses);
    assertEntry(audit,
                schema,
                kATypes.kCreateCollection,
                kExpectedFields.kCreateCollection,
                expectedAddresses);

    assert.commandWorked(testDB[kCollectionName].createIndex({field: 1}));
    assertEntry(
        audit, schema, kATypes.kCreateIndex, kExpectedFields.kCreateIndex, expectedAddresses);

    assert.commandWorked(testDB[kCollectionName].renameCollection(kCollectionRenamed, true));
    assertEntry(audit,
                schema,
                kATypes.kRenameCollection,
                kExpectedFields.kRenameCollection,
                expectedAddresses);

    assert.commandWorked(testDB.dropDatabase());

    // DropDatabase causes the collection, database, and index to all get dropped. However, the
    // order that these events occur in is not strictly defined. Therefore, we get the current line
    // in the audit log so that it can be reset at the starting point of the search for each of the
    // next 3 events.
    const dropCollPos = audit._auditLine;
    assertEntry(
        audit, schema, kATypes.kDropCollection, kExpectedFields.kDropCollection, expectedAddresses);

    audit._auditLine = dropCollPos;
    assertEntry(audit, schema, kATypes.kDropIndex, kExpectedFields.kDropIndex, expectedAddresses);

    audit._auditLine = dropCollPos;
    assertEntry(
        audit, schema, kATypes.kDropDatabase, kExpectedFields.kDropDatabase, expectedAddresses);

    fixture.stopProcess();
}
