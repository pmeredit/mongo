// Verify {create,update}{User,Role} is sent to audit log
// @tags: [ requires_fcv_80 ]

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kApplicationActivity = 6;
const kAPIActivity = 6003;
const kAPIActivityCreate = 1;
const kAPIActivityRead = 2;
const kAPIActivityUpdate = 3;
const kAPIActivityDelete = 4;
const kAPIActivityOther = 99;

const kCRUDColl = "crudColl";
const kKillColl = "killColl";

function assertEntryArgs(entry, db, coll, expectSuccess, verb) {
    assert(entry.api !== undefined, "Missing required field 'api'");

    assert(entry.api.request !== undefined, "Missing required field 'api.request'");
    assert.eq(entry.api.request.uid, `${db}.${coll}`);

    assert(entry.api.response !== undefined, "Missing required field 'api.response'");
    assert.eq(entry.api.response.code, expectSuccess ? 0 : ErrorCodes.Unauthorized);

    assert(entry.unmapped !== undefined, "Missing required field 'unmapped'");
    assert(entry.unmapped.args !== undefined, "Missing required field 'unmapped.args'");

    assert.eq(entry.unmapped.args[verb], coll);
}

function assertCrudOp(conn, spooler, auditSuccess, activity, cmd) {
    const verb = Object.keys(cmd)[0];
    const db1 = conn.getDB('db1');
    const db2 = conn.getDB('db2');
    // Operations should succeed on db1, and fail on db2.
    // We only get the success entries when auditSuccess===true.
    ['db1', 'db2'].forEach(function(db) {
        print(`== Testing ${db}.${kCRUDColl} => ${verb}/${activity}`);
        const expectSuccess = (db === 'db1');
        const expectEntry = auditSuccess || !expectSuccess;
        spooler.fastForward();
        const result = conn.getDB(db).runCommand(cmd);
        if (expectSuccess) {
            assert.commandWorked(result);
        } else {
            assert.commandFailed(result);
        }
        if (!expectEntry) {
            sleep(1000);
            spooler.assertNoEntry(kApplicationActivity, kAPIActivity, activity);
            return;
        }

        const entry = spooler.assertEntry(kApplicationActivity, kAPIActivity, activity);
        assertEntryArgs(entry, db, kCRUDColl, expectSuccess, verb);
    });
}

function testCRUD(conn, spooler, auditSuccess) {
    assertCrudOp(
        conn, spooler, auditSuccess, kAPIActivityCreate, {insert: kCRUDColl, documents: [{x: 1}]});
    assertCrudOp(conn, spooler, auditSuccess, kAPIActivityRead, {find: kCRUDColl});
    assertCrudOp(conn,
                 spooler,
                 auditSuccess,
                 kAPIActivityUpdate,
                 {update: kCRUDColl, updates: [{q: {x: 1}, u: {x: 2}}]});
    assertCrudOp(conn,
                 spooler,
                 auditSuccess,
                 kAPIActivityDelete,
                 {"delete": kCRUDColl, deletes: [{q: {x: 2}, limit: 1}]});
}

function testKillCursors(adminConn, userConn, spooler, auditSuccess) {
    [{db: 'db1', findConn: userConn, killConn: userConn, expectSuccess: true},
     {db: 'db1', findConn: adminConn, killConn: userConn, expectSuccess: false},
     {db: 'db2', findConn: adminConn, killConn: userConn, expectSuccess: false},
     {db: 'db2', findConn: adminConn, killConn: adminConn, expectSuccess: true}]
        .forEach(function(test) {
            print(`== Testing ${test.db}.${kKillColl} => killCursors/99`);
            test.expectEntry = auditSuccess || !test.expectSuccess;

            spooler.fastForward();
            const findDB = test.findConn.getDB(test.db);
            const find = assert.commandWorked(findDB.runCommand({find: kKillColl, batchSize: 10}));
            const cursorId = find.cursor.id;

            const killDB = test.killConn.getDB(test.db);
            const result = killDB.runCommand({killCursors: kKillColl, cursors: [cursorId]});
            if (test.expectSuccess) {
                assert.commandWorked(result);
            } else {
                assert.commandFailed(result);
            }

            if (!test.expectEntry) {
                sleep(1000);
                spooler.assertNoEntry(kApplicationActivity, kAPIActivity, kAPIActivityOther);
                return;
            }

            const entry =
                spooler.assertEntry(kApplicationActivity, kAPIActivity, kAPIActivityOther);
            assertEntryArgs(entry, test.db, kKillColl, test.expectSuccess, 'killCursors');
        });
}

function runTest(conn, admin, spooler) {
    assert.commandWorked(admin.runCommand({createUser: 'admin', pwd: "pwd", roles: ['root']}));
    assert(admin.auth('admin', 'pwd'));
    assert.commandWorked(admin.runCommand(
        {createUser: 'db1User', pwd: "pwd", roles: [{db: 'db1', role: 'readWrite'}]}));

    const userConn = new Mongo(conn.host);
    assert(userConn.getDB('admin').auth('db1User', 'pwd'));

    // Seed db1 and db2 with data for killCursors tests.
    ['db1', 'db2'].forEach(function(dbName) {
        const coll = conn.getDB(dbName)[kKillColl];
        for (let i = 0; i < 100; ++i) {
            assert.commandWorked(coll.insert({x: i}));
        }
    });

    [true, false].forEach(function(auditSuccess) {
        const config = {
            filter: {class_uid: kAPIActivity},
            auditAuthorizationSuccess: auditSuccess,
        };
        assert.commandWorked(admin.runCommand({setClusterParameter: {auditConfig: config}}));

        testCRUD(userConn, spooler, auditSuccess);
        testKillCursors(conn, userConn, spooler, auditSuccess);
    });
}

{
    const standaloneFixture = new StandaloneFixture();
    jsTest.log("Testing OCSF authCheck output on standalone");

    const {conn, audit, admin} =
        standaloneFixture.startProcess({auditRuntimeConfiguration: true}, 'JSON', 'ocsf');

    // Wait for the FTDC periodic metadata collector to audit its getClusterParameter invocation
    audit.assertEntry(kApplicationActivity, kAPIActivity, kAPIActivityRead);

    runTest(conn, admin, audit);
    standaloneFixture.stopProcess();
}
