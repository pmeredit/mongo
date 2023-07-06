// Verify auth is sent to audit log
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function runTestMongod({options, func}) {
    const mongod = MongoRunner.runMongodAuditLogger(options);
    func({conn: mongod, audit: mongod.auditSpooler()});

    MongoRunner.stopMongod(mongod);
}

function runTestMongos({baseOptions, options, func}) {
    const st = MongoRunner.runShardedClusterAuditLogger(baseOptions, options);
    func({conn: st.s0, audit: st.s0.auditSpooler()});

    st.stop();
}

function checkAppName({appName, conn, audit}) {
    audit.fastForward();

    let newConn;
    if (appName === undefined) {
        jsTest.log(`Testing audit log for default appName`);
        newConn = new Mongo(`mongodb://${conn.host}/`);
    } else {
        jsTest.log(`Testing audit log for appName "${appName}"`);
        newConn = new Mongo(`mongodb://${conn.host}/?appName=${appName}`);
    }

    {
        const entry = audit.assertEntry("clientMetadata");
        print(`Found new entry: ${tojson(entry)}`);

        assert(entry.param.hasOwnProperty('localEndpoint'));

        const metadata = entry.param.clientMetadata;
        assert(metadata.hasOwnProperty('application'));
        if (appName !== undefined) {
            assert.eq(metadata.application.name, appName);
        }

        assert(metadata.hasOwnProperty('driver'));
        assert(metadata.hasOwnProperty('os'));
    }
    newConn.close();

    audit.assertNoNewEntries("clientMetadata");
}

runTestMongod({
    options: {},
    func: function({conn, audit}) {
        checkAppName({appName: undefined, conn: conn, audit: audit});
        checkAppName({appName: "foo", conn: conn, audit: audit});
        checkAppName({appName: "bar", conn: conn, audit: audit});
    }
});

runTestMongos({
    options: {},
    func: function({conn, audit}) {
        checkAppName({appName: undefined, conn: conn, audit: audit});
        checkAppName({appName: "foo", conn: conn, audit: audit});
        checkAppName({appName: "bar", conn: conn, audit: audit});
    }
});
