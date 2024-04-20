/**
 * Defines a FSM that represents a fleet of 3 LDAP servers moving through
 * the following states: normal, partialConfiguredSlow, allConfiguredSlow,
 * partialConfiguredTimeout, allConfiguredTimeout, failover, and total outage.
 */

import {fsm} from "jstests/concurrency/fsm_libs/fsm.js";
import {
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";
import {
    MockLDAPServer
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_utils.js";

// If the server is already stopped, it starts it with the specified delay. If the server is already
// running with the same delay, `server.setDelay()` is a no-op. Otherwise, it sets the new delay
// and restarts it.
function updateLDAPServerDelayWithPossibleRestart(server, delay) {
    if (!server.isRunning()) {
        server.delay = delay;
        server.start();
    } else {
        server.setDelay(delay);
    }
}

/**
 *              MongoDB
 *                 |
 *                 |
 *      LDAP 1 <--------> LDAP 2
 *        |
 *        |
 *    (referral)
 *        |
 *        V
 *      LDAP 0
 *
 * The FSM mocks the topology shown above, with three LDAP servers and a sharded MongoDB cluster.
 * The cluster is only configured with LDAP 1 and LDAP 2. LDAP 1 returns referrals for all binds
 * and searches to LDAP 0. LDAP 2 uses its local directory to respond to all binds and searches.
 * LDAP 0 and LDAP 2 have the exact same directory structure, so they are replicas that can be used
 * for failover.
 */
export class LDAPFSM {
    constructor(customShardingOptions, workloads, iterations) {
        this.ldapServers = [];
        for (let i = 0; i < 3; i++) {
            let newLDAPServer;
            if (i === 1) {
                newLDAPServer = new MockLDAPServer(undefined, this.ldapServers[0].getHostAndPort());
            } else {
                newLDAPServer = new MockLDAPServer(
                    'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif');
            }
            newLDAPServer.start();
            this.ldapServers.push(newLDAPServer);
        }

        this.states = {
            normal: function normal(conn, configuredServers, workload, timeout) {
                // Represents a state where all configured LDAP servers are running without delays.
                jsTestLog('Running in the normal state');
                configuredServers.forEach(
                    ldapServer => updateLDAPServerDelayWithPossibleRestart(ldapServer, 0));

                // Sleep to give the cluster time to automatically refresh its connection pool.
                sleep(5000);
                workload(conn);
            },
            partialConfiguredSlow: function partialSlow(
                conn, configuredServers, workload, timeout) {
                // Represents a state where any one of the configured LDAP servers is responding
                // slowly (but within timeout). All other LDAP servers are running without delay.
                jsTestLog('Running in the partialConfiguredSlow state');
                const ldapServerIdx = Random.randInt(configuredServers.length);
                for (let i = 0; i < configuredServers.length; i++) {
                    if (i === ldapServerIdx) {
                        updateLDAPServerDelayWithPossibleRestart(configuredServers[i],
                                                                 Math.floor(timeout / 2));
                    } else {
                        updateLDAPServerDelayWithPossibleRestart(configuredServers[i], 0);
                    }
                }

                // Sleep to give the cluster time to automatically refresh its connection pool.
                sleep(5000);
                workload(conn);
            },
            allConfiguredSlow: function allSlow(conn, configuredServers, workload, timeout) {
                // Represents a state where both configured LDAP servers are responding slowly (but
                // within timeout).
                jsTestLog('Running in the allConfiguredSlow state');
                configuredServers.forEach(ldapServer => updateLDAPServerDelayWithPossibleRestart(
                                              ldapServer, Math.floor(timeout / 2)));

                // Sleep to give the cluster time to automatically refresh its connection pool.
                sleep(5000);
                workload(conn);
            },
            partialConfiguredTimeout: function partialTimeout(
                conn, configuredServers, workload, timeout) {
                // Represents a state where any one of the configured LDAP server is providing
                // responses after the timeout.
                jsTestLog('Running in the partialConfiguredTimeout state');
                const ldapServerIdx = Random.randInt(configuredServers.length);
                for (let i = 0; i < configuredServers.length; i++) {
                    if (i === ldapServerIdx) {
                        updateLDAPServerDelayWithPossibleRestart(configuredServers[i],
                                                                 Math.floor(timeout * 2));
                    } else {
                        updateLDAPServerDelayWithPossibleRestart(configuredServers[i], 0);
                    }
                }

                // Sleep to give the cluster time to automatically refresh its connection pool.
                sleep(5000);
                workload(conn);
            },
            allConfiguredTimeout: function allTimeout(conn, configuredServers, workload, timeout) {
                // Represents a state where all configured LDAP servers are timing out before
                // responding.
                jsTestLog('Running in the allConfiguredTimeout state');
                configuredServers.forEach(ldapServer => updateLDAPServerDelayWithPossibleRestart(
                                              ldapServer, (timeout * 2)));

                // Sleep to give the cluster time to automatically refresh its connection pool.
                sleep(5000);
                workload(conn);
            },
            failover: function failover(conn, configuredServers, workload, timeout) {
                // Represents a state where one configured LDAP server is totally down but the other
                // one is running.
                jsTestLog('Running in the failover state');
                const ldapServerIdx = Random.randInt(configuredServers.length);
                for (let i = 0; i < configuredServers.length; i++) {
                    if (i === ldapServerIdx) {
                        // Stop the server if it is running, otherwise leave it stopped.
                        if (configuredServers[i].isRunning()) {
                            configuredServers[i].stop();
                        }
                    } else {
                        // All other servers should be operating normally.
                        updateLDAPServerDelayWithPossibleRestart(configuredServers[i], 0);
                    }
                }

                // Sleep to give the cluster time to automatically refresh its connection pool.
                sleep(5000);
                workload(conn);
            },
            totalConfiguredOutage: function totalOutage(
                conn, configuredServers, workload, timeout) {
                // Represents a state where all configured LDAP servers are down and unreachable.
                jsTestLog('Running in the totalConfiguredOutage state');
                configuredServers.forEach(ldapServer => {
                    if (ldapServer.isRunning()) {
                        ldapServer.stop();
                    }
                });

                // Sleep to give the cluster time to automatically refresh its connection pool.
                sleep(5000);
                workload(conn);
            }
        };

        this.transitions = {
            normal: {
                normal: 0.4,
                partialConfiguredSlow: 0.2,
                partialConfiguredTimeout: 0.2,
                failover: 0.2
            },
            partialConfiguredSlow: {
                normal: 0.2,
                partialConfiguredSlow: 0.4,
                allConfiguredSlow: 0.2,
                partialConfiguredTimeout: 0.2
            },
            allConfiguredSlow: {
                partialConfiguredSlow: 0.2,
                allConfiguredSlow: 0.4,
                partialConfiguredTimeout: 0.2,
                allConfiguredTimeout: 0.2
            },
            partialConfiguredTimeout: {
                partialConfiguredSlow: 0.2,
                partialConfiguredTimeout: 0.4,
                allConfiguredTimeout: 0.2,
                failover: 0.2
            },
            allConfiguredTimeout: {
                allConfiguredSlow: 0.2,
                partialConfiguredTimeout: 0.2,
                allConfiguredTimeout: 0.4,
                totalConfiguredOutage: 0.2
            },
            failover: {
                normal: 0.2,
                partialConfiguredTimeout: 0.2,
                failover: 0.4,
                totalConfiguredOutage: 0.2
            },
            totalConfiguredOutage: {
                allConfiguredSlow: 0.2,
                partialConfiguredTimeout: 0.2,
                failover: 0.2,
                totalConfiguredOutage: 0.4
            },
        };

        TestData.skipCheckingIndexesConsistentAcrossCluster = true;
        TestData.skipCheckOrphans = true;
        TestData.skipCheckDBHashes = true;
        TestData.skipCheckShardFilteringMetadata = true;

        const configGenerator = new LDAPTestConfigGenerator();
        configGenerator.authorizationManagerCacheSize = 0;
        configGenerator.ldapServers =
            [this.ldapServers[1].getHostAndPort(), this.ldapServers[2].getHostAndPort()];
        configGenerator.ldapValidateLDAPServerConfig = false;
        configGenerator.ldapUserToDNMapping =
            [{match: "(.+)", substitution: "cn={0}," + defaultUserDNSuffix}];
        configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
        configGenerator.ldapTimeoutMS = 3000;
        configGenerator.ldapConnectionPoolHostRefreshIntervalMillis = 3000;

        const shardingConfig = configGenerator.generateShardingConfig();
        shardingConfig.other.writeConcernMajorityJournalDefault = false;

        this.st = new ShardingTest(Object.assign({}, shardingConfig, customShardingOptions));
        this.isReplicaSetEndpointActive = this.st.isReplicaSetEndpointActive();
        setupTest(this.st.s0);

        // The sharded cluster will only perform liveness checks on the LDAP servers that were
        // configured.
        this.configuredServers = [this.ldapServers[1], this.ldapServers[2]];

        this.workloads = workloads;
        this.iterations = iterations;
        this.timeout = Math.floor(configGenerator.ldapTimeoutMS / 1000);

        const seed = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
        Random.setRandomSeed(seed);
    }

    run() {
        // Initial state is always normal.
        let currentState = 'normal';
        for (let i = 0; i < this.iterations; i++) {
            const stateFn = this.states[currentState];
            const workloadFn = this.workloads[currentState];
            stateFn(this.st.s0, this.configuredServers, workloadFn, this.timeout);

            // Derive the next state.
            currentState =
                fsm._getWeightedRandomChoice(this.transitions[currentState], Random.rand());
        }

        // Teardown the sharded cluster and all LDAP servers.
        // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
        // on replica set that is fsync locked and has replica set endpoint enabled.
        this.st.stop({
            skipCheckDBHashes: this.isReplicaSetEndpointActive,
            skipValidation: this.isReplicaSetEndpointActive
        });
        this.ldapServers.forEach(ldapServer => {
            if (ldapServer.isRunning()) {
                ldapServer.stop();
            }
        });
    }
}
