/**
 * Tests that clients authenticating and performing operations as LDAP users receive the expected
 * responses from the MongoDB cluster depending on the state of the LDAP servers. In particular,
 * it ensures that auth continues to succeed during LDAP server failover and/or timeouts. It also
 * ensures that auth fails when all LDAP servers are down or issuing extremely slow responses.
 * @tags: [requires_ldap_pool]
 */

import {Thread} from "jstests/libs/parallelTester.js";
import {LDAPFSM} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_fsm_util.js";

const clientCallbackFn = function(host, expectedResult) {
    const mongo = new Mongo(host);
    const externalDB = mongo.getDB('$external');
    const testDB = mongo.getDB('test');

    // Depending on the state of the LDAP FSM, the clients may or may not be able to authenticate.
    // This is specified by 'expectedResult.'
    if (expectedResult === 'success') {
        assert(externalDB.auth({
            user: 'ldapz_ldap1',
            pwd: 'Secret123',
            mechanism: 'PLAIN',
            digestPassword: false,
        }));

        assert.writeOK(testDB.test.insert({a: 1}), "Cannot write despite having privileges");
    } else {
        assert(!externalDB.auth({
            user: 'ldapz_ldap1',
            pwd: 'Secret123',
            mechanism: 'PLAIN',
            digestPassword: false,
        }));
    }

    return {ok: 1};
};

// runWorkload executes clientCallbackFn on 'numClients' parallel shells. Each clientCallbackFn
// asserts that it is able to succeed or fail LDAP auth based on the expected success value. If
// the result does not match the expected success value, 'numFailures' is incremented. runWorkload
// asserts that numFailures does not exceed the error margin, which is set to a nonzero value to
// account for sporadic auth errors if the wrong LDAP connection is chosen.
const numClients = 5;
const errorMargin = 1;
function runWorkload(conn, expectedResult) {
    const threads = [];
    for (let i = 0; i < numClients; i++) {
        threads.push(new Thread(clientCallbackFn, conn.host, expectedResult));
    }

    threads.forEach((thread) => thread.start());

    let numFailures = 0;
    threads.forEach((thread) => {
        try {
            thread.join();
            const res = thread.returnData();
            if (!res.ok) {
                numFailures++;
            }
        } catch (e) {
            // If any assertion in clientCallbackFn threw an exception, that would have been
            // rethrown by thread.returnData(). This signifies a failure.
            print('Caught exception in thread, incrementing failure count: ' + numFailures);
            numFailures++;
        }
    });

    assert.lte(numFailures, errorMargin);
}

const workloads = {
    normal: function(conn) {
        // When the LDAP FSM is in the 'normal' state, all client-side auth should succeed.
        runWorkload(conn, 'success');
    },
    partialConfiguredSlow: function(conn) {
        // When the LDAP FSM is in the 'partialSlow' state, all client-side auth should succeed
        // since all requests are still being satisfied within the timeout.
        runWorkload(conn, 'success');
    },
    allConfiguredSlow: function(conn) {
        // When the LDAP FSM is in the 'allSlow' state, all client-side auth should succeed since
        // all requests are still being satisfied within the timeout.
        runWorkload(conn, 'success');
    },
    partialConfiguredTimeout: function(conn) {
        // When the LDAP FSM is in the 'partialTimeout' state, all client-side auth should succeed
        // since there is still an LDAP server serving requests without delay.
        runWorkload(conn, 'success');
    },
    allConfiguredTimeout: function(conn) {
        // When the LDAP FSM is in the 'allTimeout' state, all client-side auth should fail since
        // all LDAP requests should be timing out.
        runWorkload(conn, 'failure');
    },
    failover: function(conn) {
        // When the LDAP FSM is in the 'failover' state, all client-side auth should succeed since
        // there is still an LDAP server up and serving requests.
        runWorkload(conn, 'success');
    },
    totalConfiguredOutage: function(conn) {
        // When the LDAP FSM is in the 'totalOutage' state, all client-side auth should fail since
        // all LDAP servers are down.
        runWorkload(conn, 'failure');
    },
};

const fsm = new LDAPFSM({}, workloads, 30);
fsm.run();
