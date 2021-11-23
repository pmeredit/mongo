// Tests the functionality of rotate log on startup.

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

(function() {

'use strict';

const kListeningOnID = 23015;

print("Testing functionality of rotating both logs.");
function testRotateLogsOnStartup(fixture) {
    {
        print("Testing functionality of rotating the audit log on startup.");
        const {conn, audit, admin} = fixture.startProcess();
        fixture.createUserAndAuth();

        admin.auth({user: "user2", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        const options = conn.fullOptions;

        // We need to sleep to prevent race conditions where the audit spooler is still reading
        // the previous audit file and the name is not being moved to an existing audit archive
        // file from the previous rotate.
        sleep(2000);

        fixture.restartMainFixtureProcess(conn, options, false);

        // The rotate should happen on startup and we should not
        // see the previous auth user in the audit log
        audit.assertNoEntry("authenticate",
                            {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        fixture.stopProcess();
    }

    // We need this sleep in case the server shutdown happens so quickly that the log tries to
    // be moved to the same name as an existing log archive file from the previous rotate.
    sleep(2000);
    {
        print(
            "Testing functionality of rotating the audit log on startup after an unclean shutdown.");
        const {conn, audit, admin} = fixture.startProcess();
        fixture.createUserAndAuth();

        admin.auth({user: "user2", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        const options = conn.fullOptions;

        // We need to sleep to prevent race conditions where the audit spooler is still reading
        // the previous audit file and the name is not being moved to an existing audit archive
        // file from the previous rotate.
        sleep(2000);

        // Unclean shutdown is not being tested in ShardingFixture
        fixture.restartMainFixtureProcess(conn, options, true);

        // The rotate should happen on startup and we should not
        // see the previous auth user in the audit log
        audit.assertNoEntry("authenticate",
                            {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        fixture.stopProcess();
    }
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing rotate functionality on standalone");
    testRotateLogsOnStartup(standaloneFixture);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing rotate functionality on sharded cluster");
    testRotateLogsOnStartup(shardingFixture);
}
})();
