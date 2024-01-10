// Test that downgrade from a version with audit config cluster parameter to a version without audit
// config cluster parameter is blocked when the audit config cluster parameter is set, and that when
// a downgrade succeeds, the final audit config is empty as expected.
// @tags: [requires_fcv_71, requires_persistence]

import {
    assertSameOID,
    assertSameTimestamp,
    findAllWithMajority,
    kDefaultDirectConfig,
    kDefaultParameterConfig,
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit_config_helpers.js";

class AuditConfigMigrationBaseFixture {
    upgrade() {
        assert(false);
    }

    downgrade(shouldSucceed) {
        assert(false);
    }

    setAuditConfigWithFeatureFlagDisabled(config) {
        assert(false);
    }

    expectSoonAuditConfigWithFeatureFlagEnabled(config) {
        assert(false);
    }

    setAuditConfigWithFeatureFlagEnabled(config) {
        assert(false);
    }

    deleteAuditConfigFromClusterParametersCollection() {
        assert(false);
    }

    stop() {
        assert(false);
    }

    testMigrationOnUpgrade() {
        this.expectSoonAuditConfigWithFeatureFlagEnabled(kDefaultParameterConfig);
        this.downgrade(true /* shouldSucceed */);

        this.expectSoonAuditConfigWithFeatureFlagDisabled(kDefaultDirectConfig);

        // If we set a config and upgrade, we expect to see that same config after we upgrade
        const myConfig = {filter: {atype: 'authCheck'}, auditAuthorizationSuccess: true};
        const configWithGeneration = Object.assign({generation: ObjectId()}, myConfig);
        this.setAuditConfigWithFeatureFlagDisabled(configWithGeneration);
        this.expectSoonAuditConfigWithFeatureFlagDisabled(configWithGeneration);
        this.upgrade();
        this.expectSoonAuditConfigWithFeatureFlagEnabled(myConfig);
    }

    testBlockingOnDowngrade() {
        // Non-default config to test with
        let myConfig = {
            filter: {atype: 'authCheck'},
            auditAuthorizationSuccess: true,
            clusterParameterTime: Timestamp()
        };

        // We should start with the default parameter
        this.expectSoonAuditConfigWithFeatureFlagEnabled(kDefaultParameterConfig);

        this.setAuditConfigWithFeatureFlagEnabled(myConfig);
        this.expectSoonAuditConfigWithFeatureFlagEnabled(myConfig);

        // After setting an audit config parameter, we shouldn't be able to downgrade
        this.downgrade(false /* shouldSucceed */);

        // Note that we have to explicitly delete here rather than setting; this is because after
        // downgrading, even though we failed, we will end up in a transitional version which acts
        // like the lower FCV version. This means setting the audit config cluster parameter will
        // fail because the current FCV is too low.
        this.deleteAuditConfigFromClusterParametersCollection();

        // After unsetting the cluster parameter we should be able to downgrade, and the audit
        // config should be the default.
        this.downgrade(true /* shouldSucceed */);
        this.expectSoonAuditConfigWithFeatureFlagDisabled(kDefaultDirectConfig);

        // We should now be able to set through setAuditConfigWithFeatureFlagDisabled and see our
        // update take effect.
        delete myConfig.clusterParameterTime;
        myConfig.generation = ObjectId();
        this.setAuditConfigWithFeatureFlagDisabled(myConfig);
        this.expectSoonAuditConfigWithFeatureFlagDisabled(myConfig);
    }

    testRevertFailedDowngrade() {
        let myConfig = {
            filter: {atype: 'authCheck'},
            auditAuthorizationSuccess: true,
            clusterParameterTime: Timestamp()
        };

        this.expectSoonAuditConfigWithFeatureFlagEnabled(kDefaultParameterConfig);

        this.setAuditConfigWithFeatureFlagEnabled(myConfig);
        this.expectSoonAuditConfigWithFeatureFlagEnabled(myConfig);

        this.downgrade(false /* shouldSucceed */);

        // Upgrade after failing the downgrade, and make sure the previous audit config is still
        // in-place as expected.
        this.upgrade();
        this.expectSoonAuditConfigWithFeatureFlagEnabled(myConfig);

        // Make sure that we can set and get a new audit config after this
        myConfig.filter.atype = 'abc';
        myConfig.clusterParameterTime = Timestamp();
        this.setAuditConfigWithFeatureFlagEnabled(myConfig);
        this.expectSoonAuditConfigWithFeatureFlagEnabled(myConfig);
    }
}

class AuditConfigMigrationStandaloneFixture extends AuditConfigMigrationBaseFixture {
    constructor(extraOpts = {}) {
        super();
        const baseOpts = {auditDestination: 'console', auditRuntimeConfiguration: true};
        // Standalone connection to perform actions on.
        this.conn = MongoRunner.runMongod(Object.assign({}, baseOpts, extraOpts));
    }

    stop() {
        MongoRunner.stopMongod(this.conn);
    }

    upgrade() {
        assert.commandWorked(this.conn.getDB('admin').runCommand(
            {setFeatureCompatibilityVersion: latestFCV, confirm: true}));

        // After migration, expect that there are no _id = audit entries in config.settings.
        const settings = this.conn.getDB("config").settings;
        assert.eq(settings.find({_id: 'audit'}).toArray(), []);
    }

    downgrade(shouldSucceed) {
        const res = this.conn.getDB('admin').runCommand(
            {setFeatureCompatibilityVersion: lastLTSFCV, confirm: true});
        if (shouldSucceed) {
            assert.commandWorked(res);
            // Ensure that there is no "auditConfig" entry in config.clusterParameters.
            const arr =
                this.conn.getDB("config").clusterParameters.find({_id: 'auditConfig'}).toArray();
            assert.eq(arr.length, 0);
        } else {
            assert.commandFailed(res);
        }
    }

    expectSoonAuditConfigWithFeatureFlagDisabled(config) {
        assert.soonNoExcept(
            () => {
                const auditManagerConfig =
                    assert.commandWorked(this.conn.getDB('admin').runCommand({getAuditConfig: 1}));
                assert.eq(bsonWoCompare(auditManagerConfig.filter, config.filter), 0);
                assert.eq(auditManagerConfig.auditAuthorizationSuccess,
                          config.auditAuthorizationSuccess);
                assertSameOID(auditManagerConfig.generation, config.generation);
                return true;
            },
            "On standalone w/ feature flag disabled, audit config did not match expected before timeout",
            20 * 1000 /* timeout */);
    }

    expectSoonAuditConfigWithFeatureFlagEnabled(config) {
        let auditManagerConfig;
        assert.soonNoExcept(
            () => {
                // Get through both getAuditConfig and getClusterParameter and make sure they match.
                auditManagerConfig =
                    assert.commandWorked(this.conn.getDB('admin').runCommand({getAuditConfig: 1}));
                delete auditManagerConfig.ok;
                assert.eq(bsonWoCompare(auditManagerConfig.filter, config.filter), 0);
                assert.eq(auditManagerConfig.auditAuthorizationSuccess,
                          config.auditAuthorizationSuccess);
                assert.neq(undefined, auditManagerConfig.clusterParameterTime);
                return true;
            },
            "On standalone w/ feature flag enabled, getAuditConfig audit config did not match expected before timeout",
            20 * 1000 /* timeout */);

        assert.soonNoExcept(
            () => {
                const clusterParameterConfig =
                    assert
                        .commandWorked(this.conn.getDB('admin').runCommand(
                            {getClusterParameter: 'auditConfig'}))
                        .clusterParameters[0]
                        .auditConfig;
                assert.eq(bsonWoCompare(clusterParameterConfig, auditManagerConfig), 0);
                return true;
            },
            "On standalone w/ feature flag enabled, cluster parameter audit config did not match expected before timeout",
            20 * 1000 /* timeout */);
    }

    setAuditConfigWithFeatureFlagDisabled(config) {
        // setAuditConfig command is expected to fail because we are in a downgraded state. Check
        // that this happens, and then directly write to config.settings to get around this.
        assert.commandFailed(
            this.conn.getDB('admin').runCommand(Object.assign({setAuditConfig: 1}, config)));

        const writeConfig = Object.assign({_id: 'audit'}, config);
        const settings = this.conn.getDB("config").settings;
        assert.commandWorked(settings.runCommand(
            {findAndModify: "settings", query: {_id: 'audit'}, update: writeConfig, upsert: true}));
    }

    setAuditConfigWithFeatureFlagEnabled(config) {
        if ('clusterParameterTime' in config) {
            // We let setClusterParameter decide the clusterParameterTime.
            config = Object.assign({}, config);
            delete config.clusterParameterTime;
        }
        assert.commandWorked(
            this.conn.getDB('admin').runCommand({setClusterParameter: {auditConfig: config}}));
    }

    deleteAuditConfigFromClusterParametersCollection() {
        // Delete directly to get around FCV restriction on setClusterParameter.
        assert.writeOK(this.conn.getDB('config').clusterParameters.remove({_id: 'auditConfig'}));
    }
}

class AuditConfigMigrationReplsetFixture extends AuditConfigMigrationBaseFixture {
    constructor(extraOpts = {}) {
        super();
        const baseOpts = {auditDestination: 'console', auditRuntimeConfiguration: true};
        // ReplSetTest to perform actions on.
        this.rst = new ReplSetTest({nodes: 2, nodeOptions: Object.assign({}, baseOpts, extraOpts)});
        this.rst.startSet();
        this.rst.initiate();
        this.rst.awaitSecondaryNodes();
    }

    stop() {
        this.rst.stopSet();
    }

    upgrade() {
        assert.commandWorked(this.rst.getPrimary().getDB('admin').runCommand(
            {setFeatureCompatibilityVersion: latestFCV, confirm: true}));

        const arr =
            findAllWithMajority(this.rst.getPrimary().getDB("config"), "config", {_id: "audit"});
        assert.eq(arr.length, 0);
    }

    downgrade(shouldSucceed) {
        const res = this.rst.getPrimary().getDB('admin').runCommand(
            {setFeatureCompatibilityVersion: lastLTSFCV, confirm: true});
        if (shouldSucceed) {
            assert.commandWorked(res);
            const arr = findAllWithMajority(
                this.rst.getPrimary().getDB("config"), "clusterParameters", {_id: 'auditConfig'});
            assert.eq(arr.length, 0);
        } else {
            assert.commandFailed(res);
        }
    }

    expectSoonAuditConfigWithFeatureFlagDisabled(config) {
        for (let conn of this.rst.nodes) {
            assert.soonNoExcept(
                () => {
                    const auditManagerConfig =
                        assert.commandWorked(conn.getDB('admin').runCommand({getAuditConfig: 1}));
                    assert.eq(bsonWoCompare(auditManagerConfig.filter, config.filter), 0);
                    assert.eq(auditManagerConfig.auditAuthorizationSuccess,
                              config.auditAuthorizationSuccess);
                    assertSameOID(auditManagerConfig.generation, config.generation);
                    return true;
                },
                "On replica set w/ feature flag disabled, member's audit config did not match expected before timeout",
                60 * 1000 /* timeout */);
        }
    }

    expectSoonAuditConfigWithFeatureFlagEnabled(config) {
        let clusterParameterConfig;
        assert.soonNoExcept(
            () => {
                clusterParameterConfig =
                    assert
                        .commandWorked(this.rst.getPrimary().getDB('admin').runCommand(
                            {getClusterParameter: 'auditConfig'}))
                        .clusterParameters[0]
                        .auditConfig;
                assert.eq(bsonWoCompare(clusterParameterConfig.filter, config.filter), 0);
                assert.eq(clusterParameterConfig.auditAuthorizationSuccess,
                          config.auditAuthorizationSuccess);
                assert.neq(undefined, clusterParameterConfig.clusterParameterTime);
                return true;
            },
            "On replica set w/ feature flag enabled, primary's cluster parameter audit config did not match expected before timeout",
            60 * 1000 /* timeout */);

        for (let conn of this.rst.nodes) {
            assert.soonNoExcept(
                () => {
                    const auditManagerConfig =
                        assert.commandWorked(conn.getDB('admin').runCommand({getAuditConfig: 1}));
                    assert.eq(bsonWoCompare(auditManagerConfig.filter, config.filter), 0);
                    assert.eq(auditManagerConfig.auditAuthorizationSuccess,
                              config.auditAuthorizationSuccess);
                    assert.eq(clusterParameterConfig.clusterParameterTime,
                              auditManagerConfig.clusterParameterTime);
                    return true;
                },
                "On replica set w/ feature flag enabled, member's getAuditConfig audit config did not match expected before timeout",
                60 * 1000 /* timeout */);
        }
    }

    setAuditConfigWithFeatureFlagDisabled(config) {
        assert.commandFailed(this.rst.getPrimary().getDB('admin').runCommand(
            Object.assign({setAuditConfig: 1}, config)));

        const writeConfig = Object.assign({_id: 'audit'}, config);
        const settings = this.rst.getPrimary().getDB("config").settings;
        assert.commandWorked(settings.runCommand({
            findAndModify: "settings",
            query: {_id: 'audit'},
            update: writeConfig,
            upsert: true,
            writeConcern: {w: 'majority'}
        }));
    }

    setAuditConfigWithFeatureFlagEnabled(config) {
        if ('clusterParameterTime' in config) {
            config = Object.assign({}, config);
            delete config.clusterParameterTime;
        }
        assert.commandWorked(this.rst.getPrimary().getDB('admin').runCommand(
            {setClusterParameter: {auditConfig: config}}));
    }

    deleteAuditConfigFromClusterParametersCollection() {
        assert.commandWorked(this.rst.getPrimary().getDB('config').clusterParameters.deleteOne(
            {_id: 'auditConfig'}, {writeConcern: {w: 'majority'}}));
    }
}

class AuditConfigMigrationShardingFixture extends AuditConfigMigrationBaseFixture {
    constructor(configPollingFrequencySecs,
                extraRsOpts = {},
                extraConfigOpts = {},
                extraMongosOpts = {}) {
        super();
        const baseOpts = {
            auditDestination: 'console',
            auditRuntimeConfiguration: true,
            setParameter: {
                auditConfigPollingFrequencySecs: configPollingFrequencySecs,
            }
        };
        // ShardingTest to perform actions on.
        this.st = new ShardingTest({
            mongos: 1,
            shards: 3,
            config: 1,
            rs: {nodes: 2},
            other: {
                rsOptions: Object.assign({}, baseOpts, extraRsOpts),
                configOptions: Object.assign({}, baseOpts, extraConfigOpts),
                mongosOptions: Object.assign({}, baseOpts, extraMongosOpts),
            }
        });
        this.configPollingFrequencySecs = configPollingFrequencySecs;
    }

    stop() {
        this.st.stop();
    }

    allPrimaries() {
        // All shard primaries, including config shard.
        return [this.st.configRS.getPrimary()].concat(this.st._connections);
    }

    upgrade() {
        assert.commandWorked(this.st.s.getDB('admin').runCommand(
            {setFeatureCompatibilityVersion: latestFCV, confirm: true}));

        for (let conn of this.allPrimaries()) {
            const arr = findAllWithMajority(conn.getDB("config"), "settings", {_id: "audit"});
            assert.eq(arr.length, 0);
        }
    }

    downgrade(shouldSucceed) {
        const res = this.st.s.getDB('admin').runCommand(
            {setFeatureCompatibilityVersion: lastLTSFCV, confirm: true});
        if (shouldSucceed) {
            assert.commandWorked(res);
            for (let conn of this.allPrimaries()) {
                const arr = findAllWithMajority(
                    conn.getDB("config"), "clusterParameters", {_id: "auditConfig"});
                assert.eq(arr.length, 0);
            }
        } else {
            assert.commandFailed(res);
        }
    }

    expectSoonAuditConfigWithFeatureFlagDisabled(config) {
        assert.soonNoExcept(
            () => {
                const auditManagerConfig =
                    assert.commandWorked(this.st.s.getDB('admin').runCommand({getAuditConfig: 1}));
                assert.eq(bsonWoCompare(auditManagerConfig.filter, config.filter), 0);
                assert.eq(auditManagerConfig.auditAuthorizationSuccess,
                          config.auditAuthorizationSuccess);
                assertSameOID(auditManagerConfig.generation, config.generation);
                return true;
            },
            "On sharded cluster w/ feature flag disabled, audit config polled through mongos did not match expected before timeout",
            60 * 1000 /* timeout */);

        for (let conn of this.allPrimaries()) {
            assert.soonNoExcept(
                () => {
                    // Since getAuditConfig is different on config shards vs data shards, query
                    // config.settings directly.
                    const configArray =
                        findAllWithMajority(conn.getDB("config"), "settings", {_id: "audit"});

                    let settingsCollectionConfig;
                    if (configArray.length == 0) {
                        settingsCollectionConfig = kDefaultDirectConfig;
                    } else {
                        assert.eq(configArray.length, 1);
                        settingsCollectionConfig = configArray[0];
                    }

                    assert.eq(bsonWoCompare(config.filter, settingsCollectionConfig.filter), 0);
                    assert.eq(config.auditAuthorizationSuccess,
                              settingsCollectionConfig.auditAuthorizationSuccess);
                    assertSameOID(config.generation, settingsCollectionConfig.generation);
                    return true;
                },
                "On sharded cluster w/ feature flag disabled, shard primary's audit config did not match expected before timeout",
                60 * 1000 /* timeout */);
        }
    }

    expectSoonAuditConfigWithFeatureFlagEnabled(config) {
        let auditManagerConfig, clusterParameterConfig;
        assert.soonNoExcept(
            () => {
                auditManagerConfig =
                    assert.commandWorked(this.st.s.getDB('admin').runCommand({getAuditConfig: 1}));
                assert.eq(bsonWoCompare(auditManagerConfig.filter, config.filter), 0);
                assert.eq(auditManagerConfig.auditAuthorizationSuccess,
                          config.auditAuthorizationSuccess);
                assert.neq(undefined, auditManagerConfig.clusterParameterTime);
                return true;
            },
            "On sharded cluster w/ feature flag enabled, getAuditConfig audit config polled through mongos did not match expected before timeout",
            60 * 1000 /* timeout */);

        assert.soonNoExcept(
            () => {
                clusterParameterConfig = assert
                                             .commandWorked(this.st.s.getDB('admin').runCommand(
                                                 {getClusterParameter: 'auditConfig'}))
                                             .clusterParameters[0]
                                             .auditConfig;
                assert.eq(bsonWoCompare(clusterParameterConfig.filter, config.filter), 0);
                assert.eq(clusterParameterConfig.auditAuthorizationSuccess,
                          config.auditAuthorizationSuccess);
                assertSameTimestamp(clusterParameterConfig.clusterParameterTime,
                                    auditManagerConfig.clusterParameterTime);
                return true;
            },
            "On sharded cluster w/ feature flag enabled, cluster parameter audit config polled through mongos did not match expected before timeout",
            60 * 1000 /* timeout */);

        for (let conn of this.allPrimaries()) {
            assert.soonNoExcept(
                () => {
                    // Query config.clusterParameters directly for all shards to ensure that it
                    // contains the config.
                    const configArray = findAllWithMajority(
                        conn.getDB("config"), "clusterParameters", {_id: "auditConfig"});

                    let clusterParametersCollectionConfig;
                    if (configArray.length == 0) {
                        clusterParametersCollectionConfig = kDefaultParameterConfig;
                    } else {
                        assert.eq(configArray.length, 1);
                        clusterParametersCollectionConfig = configArray[0];
                    }

                    assert.eq(
                        bsonWoCompare(config.filter, clusterParametersCollectionConfig.filter), 0);
                    assert.eq(config.auditAuthorizationSuccess,
                              clusterParametersCollectionConfig.auditAuthorizationSuccess);
                    assertSameTimestamp(clusterParameterConfig.clusterParameterTime,
                                        clusterParametersCollectionConfig.clusterParameterTime);
                    return true;
                },
                "On sharded cluster w/ feature flag enabled, shard primary's audit config did not match expected before timeout",
                60 * 1000 /* timeout */);
        }
    }

    setAuditConfigWithFeatureFlagDisabled(config) {
        assert.commandFailed(
            this.st.s.getDB('admin').runCommand(Object.assign({setAuditConfig: 1}, config)));

        const writeConfig = Object.assign({_id: 'audit'}, config);
        for (let conn of this.allPrimaries()) {
            const settings = conn.getDB("config").settings;
            assert.commandWorked(settings.runCommand({
                findAndModify: "settings",
                query: {_id: 'audit'},
                update: writeConfig,
                upsert: true,
                writeConcern: {w: 'majority'}
            }));
        }
    }

    setAuditConfigWithFeatureFlagEnabled(config) {
        if ('clusterParameterTime' in config) {
            config = Object.assign({}, config);
            delete config.clusterParameterTime;
        }
        assert.commandWorked(
            this.st.s.getDB('admin').runCommand({setClusterParameter: {auditConfig: config}}));
    }

    deleteAuditConfigFromClusterParametersCollection() {
        for (let node of this.allPrimaries()) {
            assert.commandWorked(node.getDB('config').clusterParameters.deleteOne(
                {_id: 'auditConfig'}, {writeConcern: {w: 'majority'}}));
        }
    }
}

function runOnStandalone(fn) {
    const fixture = new AuditConfigMigrationStandaloneFixture();
    fn(fixture);
    fixture.stop();
}

function runOnReplset(fn) {
    const fixture = new AuditConfigMigrationReplsetFixture();
    fn(fixture);
    fixture.stop();
}

function runOnSharding(fn) {
    const kPollingFrequencySecs = 1;
    const fixture = new AuditConfigMigrationShardingFixture(kPollingFrequencySecs);
    fn(fixture);
    fixture.stop();
}

jsTest.log("Running standalone tests...");
runOnStandalone((fixture) => { fixture.testBlockingOnDowngrade(); });
runOnStandalone((fixture) => { fixture.testMigrationOnUpgrade(); });
runOnStandalone((fixture) => { fixture.testRevertFailedDowngrade(); });

jsTest.log("Running replset tests...");
runOnReplset((fixture) => { fixture.testBlockingOnDowngrade(); });
runOnReplset((fixture) => { fixture.testMigrationOnUpgrade(); });
runOnReplset((fixture) => { fixture.testRevertFailedDowngrade(); });

jsTest.log("Running sharding tests...");
runOnSharding((fixture) => { fixture.testBlockingOnDowngrade(); });
runOnSharding((fixture) => { fixture.testMigrationOnUpgrade(); });
runOnSharding((fixture) => { fixture.testRevertFailedDowngrade(); });
