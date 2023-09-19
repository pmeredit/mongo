// Test that audit config is always available and correct during cluster upgrade and downgrade, and
// that it is settable when we expect.
// TODO SERVER-75941 When lastLTS has featureFlagAuditConfigClusterParameter enabled, remove this
// test
// @tags: [requires_fcv_71, does_not_support_stepdowns, requires_replication, requires_sharding]
import "jstests/multiVersion/libs/multi_rs.js";

import {
    assertSameOID,
    assertSameTimestamp,
    findAllWithMajority
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit_config_helpers.js";

const defaultConfig = {
    filter: {},
    auditAuthorizationSuccess: false,
};

const testConfig1 = {
    filter: {atype: 'a'},
    auditAuthorizationSuccess: true,
};

const testConfig2 = {
    filter: {atype: 'b'},
    auditAuthorizationSuccess: false,
};

class SingleTargetNodeMultiversionAuditFixture {
    // Virtual function
    getTargetNode() {
        assert(false);
    }

    testDowngraded() {
        // When downgraded, the audit config should be settable via setAuditConfig but not
        // setClusterParameter.
        this.expectAuditConfig(defaultConfig);
        assert.commandWorked(
            this.getTargetNode().adminCommand(Object.assign({setAuditConfig: 1}, testConfig1)));
        this.expectAuditConfig(testConfig1);
        assert.commandFailed(
            this.getTargetNode().adminCommand({setClusterParameter: {auditConfig: testConfig2}}));
        this.expectAuditConfig(testConfig1);

        // Reset audit config to default by deleting it from config.settings.
        assert.commandWorked(this.getTargetNode().getDB('config').settings.deleteOne(
            {_id: 'audit'}, {writeConcern: {w: 'majority'}}));
        this.expectAuditConfig(defaultConfig);
    }

    testUpgraded() {
        // When upgraded, the audit config should be settable via setAuditConfig and
        // setClusterParameter.
        this.expectAuditConfig(defaultConfig);
        assert.commandWorked(
            this.getTargetNode().adminCommand(Object.assign({setAuditConfig: 1}, testConfig1)));
        this.expectAuditConfig(testConfig1);
        assert.commandWorked(
            this.getTargetNode().adminCommand({setClusterParameter: {auditConfig: testConfig2}}));
        this.expectAuditConfig(testConfig2);

        // Reset audit config to default.
        assert.commandWorked(this.getTargetNode().getDB('config').clusterParameters.deleteOne(
            {_id: 'auditConfig'}, {writeConcern: {w: 'majority'}}));
        this.expectAuditConfig(defaultConfig);
    }

    expectAuditConfig(expectedConfig) {
        const actualConfig =
            assert.commandWorked(this.getTargetNode().adminCommand({getAuditConfig: 1}));
        assert.eq(bsonWoCompare(actualConfig.filter, expectedConfig.filter), 0);
        assert.eq(actualConfig.auditAuthorizationSuccess, expectedConfig.auditAuthorizationSuccess);
        if (expectedConfig.generation !== undefined) {
            assertSameOID(actualConfig.generation, expectedConfig.generation);

        } else if (expectedConfig.clusterParameterTime !== undefined) {
            assertSameTimestamp(actualConfig.clusterParameterTime,
                                expectedConfig.clusterParameterTime);
        }

        // Exactly one of the generation or cluster parameter time should be set when we call
        // getAuditConfig.
        if (actualConfig.generation === undefined) {
            assert.neq(undefined, actualConfig.clusterParameterTime);
        } else {
            assert.eq(undefined, actualConfig.clusterParameterTime);
        }
    }
}

class StandaloneUpgradeDowngradeAuditFixture extends SingleTargetNodeMultiversionAuditFixture {
    constructor(forUpgradeTest, extraOpts = {}) {
        super();
        this.downgradedOpts = Object.assign({
            auditDestination: 'console',
            auditRuntimeConfiguration: true,
            binVersion: 'last-lts',
            setParameter: {featureFlagAuditConfigClusterParameter: false}
        },
                                            extraOpts);
        this.upgradedOpts = Object.assign({
            auditDestination: 'console',
            auditRuntimeConfiguration: true,
            binVersion: 'latest',
            setParameter: {featureFlagAuditConfigClusterParameter: true}
        },
                                          extraOpts);

        // Standalone connection to perform actions on.
        if (forUpgradeTest) {
            this.conn = MongoRunner.runMongod(this.downgradedOpts);
        } else {
            this.conn = MongoRunner.runMongod(this.upgradedOpts);
        }

        this.forUpgradeTest = forUpgradeTest;
    }

    stop() {
        MongoRunner.stopMongod(this.conn);
    }

    getTargetNode() {
        return this.conn;
    }

    testUpgradeStandalone() {
        assert(this.forUpgradeTest,
               "Cannot test upgrading standalone on a node not set up for upgrade testing!");
        this.testDowngraded();
        MongoRunner.stopMongod(this.conn);
        this.conn = MongoRunner.runMongod(this.upgradedOpts);
        this.testUpgraded();
    }

    testDowngradeStandalone() {
        assert(!this.forUpgradeTest,
               "Cannot test downgrading standalone on a node set up for upgrade testing!");
        this.testUpgraded();
        assert.commandWorked(
            this.conn.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV, confirm: true}));
        MongoRunner.stopMongod(this.conn);
        this.conn = MongoRunner.runMongod(this.downgradedOpts);
        this.testDowngraded();
    }
}

class ReplicaSetUpgradeDowngradeAuditFixture extends SingleTargetNodeMultiversionAuditFixture {
    constructor(forUpgradeTest, extraOpts = {}) {
        super();
        this.downgradedOpts = Object.assign({
            auditDestination: 'console',
            auditRuntimeConfiguration: true,
            binVersion: 'last-lts',
            setParameter: {featureFlagAuditConfigClusterParameter: false}
        },
                                            extraOpts);
        this.upgradedOpts = Object.assign({
            auditDestination: 'console',
            auditRuntimeConfiguration: true,
            binVersion: 'latest',
            setParameter: {featureFlagAuditConfigClusterParameter: true}
        },
                                          extraOpts);

        // ReplSetTest to perform actions on.
        if (forUpgradeTest) {
            this.rst = new ReplSetTest({nodes: 3, nodeOptions: this.downgradedOpts});
        } else {
            this.rst = new ReplSetTest({nodes: 3, nodeOptions: this.upgradedOpts});
        }
        this.rst.startSet();
        this.rst.initiate();
        this.rst.awaitSecondaryNodes();

        this.forUpgradeTest = forUpgradeTest;
    }

    stop() {
        this.rst.stopSet();
    }

    getTargetNode() {
        return this.rst.getPrimary();
    }

    testUpgradeReplSet() {
        assert(this.forUpgradeTest,
               "Cannot test upgrading replset on a replset not set up for upgrade testing!");
        this.testDowngraded();
        this.rst.upgradeSet(this.upgradedOpts);
        assert.commandWorked(this.rst.getPrimary().adminCommand(
            {setFeatureCompatibilityVersion: latestFCV, confirm: true}));
        this.testUpgraded();
    }

    testDowngradeReplSet() {
        assert(!this.forUpgradeTest,
               "Cannot test downgrading replset on a replset set up for upgrade testing!");
        this.testUpgraded();
        assert.commandWorked(this.rst.getPrimary().adminCommand(
            {setFeatureCompatibilityVersion: lastLTSFCV, confirm: true}));
        this.rst.upgradeSet(this.downgradedOpts);
        this.testDowngraded();
    }
}

class ClusterUpgradeDowngradeAuditFixture {
    constructor(configPollingFrequencySecs,
                forUpgradeTest,
                extraRsOpts = {},
                extraConfigOpts = {},
                extraMongosOpts = {}) {
        const baseDowngradedOpts = {
            auditDestination: 'console',
            auditRuntimeConfiguration: true,
            binVersion: 'last-lts',
            setParameter: {
                auditConfigPollingFrequencySecs: configPollingFrequencySecs,
                featureFlagAuditConfigClusterParameter: false,
            }
        };
        const baseUpgradedOpts = {
            auditDestination: 'console',
            auditRuntimeConfiguration: true,
            binVersion: 'latest',
            setParameter: {
                auditConfigPollingFrequencySecs: configPollingFrequencySecs,
                featureFlagAuditConfigClusterParameter: true,
            }
        };

        let mongosBaseDowngradedOpts = Object.assign({}, baseDowngradedOpts);
        mongosBaseDowngradedOpts.setParameter =
            Object.assign({clusterServerParameterRefreshIntervalSecs: configPollingFrequencySecs},
                          baseDowngradedOpts.setParameter);
        let mongosBaseUpgradedOpts = Object.assign({}, baseUpgradedOpts);
        mongosBaseUpgradedOpts.setParameter =
            Object.assign({clusterServerParameterRefreshIntervalSecs: configPollingFrequencySecs},
                          baseUpgradedOpts.setParameter);
        this.downgradedRsOpts = Object.assign({}, baseDowngradedOpts, extraRsOpts);
        this.upgradedRsOpts = Object.assign({}, baseUpgradedOpts, extraRsOpts);
        this.downgradedConfigOpts = Object.assign({}, baseDowngradedOpts, extraConfigOpts);
        this.upgradedConfigOpts = Object.assign({}, baseUpgradedOpts, extraConfigOpts);
        this.downgradedMongosOpts = Object.assign({}, mongosBaseDowngradedOpts, extraMongosOpts);
        this.upgradedMongosOpts = Object.assign({}, mongosBaseUpgradedOpts, extraMongosOpts);

        if (forUpgradeTest) {
            this.st = new ShardingTest({
                mongos: 1,
                shards: 3,
                config: 1,
                rs: {nodes: 2},
                other: {
                    rsOptions: this.downgradedRsOpts,
                    configOptions: this.downgradedConfigOpts,
                    mongosOptions: this.downgradedMongosOpts,
                }
            });
        } else {
            this.st = new ShardingTest({
                mongos: 1,
                shards: 3,
                config: 1,
                rs: {nodes: 2},
                other: {
                    rsOptions: this.upgradedRsOpts,
                    configOptions: this.upgradedConfigOpts,
                    mongosOptions: this.upgradedMongosOpts,
                }
            });
        }

        this.forUpgradeTest = forUpgradeTest;
    }

    stop() {
        this.st.stop();
    }

    // Each of the following tests will test for the expected behavior of the cluster in a given
    // state. Note: "High" refers to the upgraded binary version/FCV version in which the feature
    // flag is enabled, "Low" refers to the downgraded binary version/FCV version in which it is
    // disabled.

    testFullyUpgraded() {
        // When fully upgraded, the audit config should be settable via setClusterParameter or
        // setAuditConfig.
        // Since the refresher is a bit behind (the oplog time specified in its transaction is often
        // behind the config server, meaning it won't fetch the latest cluster parameter update ),
        // we use the soon expect whenever we test an update to the audit config.
        this.expectAuditConfigSoon(defaultConfig);
        assert.commandWorked(
            this.st.admin.runCommand(Object.assign({setAuditConfig: 1}, testConfig1)));
        this.expectAuditConfigSoon(testConfig1);
        assert.commandWorked(
            this.st.admin.runCommand({setClusterParameter: {auditConfig: testConfig2}}));
        this.expectAuditConfigSoon(testConfig2);

        // Delete audit config to return to original state.
        for (let conn of [this.st.configRS.getPrimary()].concat(this.st._connections)) {
            assert.commandWorked(conn.getDB('config').clusterParameters.deleteOne(
                {_id: 'auditConfig'}, {writeConcern: {w: 'majority'}}));
        }
        this.expectAuditConfigSoon(defaultConfig);
    }

    testBinariesHighFCVLow() {
        // When the config server's binary version is high but the FCV is low, the audit config
        // should not be settable through commands. Internally, we expect to be reading the audit
        // config from config.settings when in this state. Since we may have just downgraded to the
        // new FCV, we have to wait for the auditSynchronizeJob to run.
        this.expectAuditConfigSoon(defaultConfig);
        assert.commandFailed(
            this.st.admin.runCommand(Object.assign({setAuditConfig: 1}, testConfig1)));
        this.expectAuditConfig(defaultConfig);
        assert.commandFailed(
            this.st.admin.runCommand({setClusterParameter: {auditConfig: testConfig2}}));
        this.expectAuditConfig(defaultConfig);

        // Since setAuditConfig is blocked, write directly to config.settings to ensure that the
        // in-memory audit config is still linked to the on-disk audit config in this state.
        const configWithGeneration = Object.assign({generation: ObjectId()}, testConfig1);
        this.writeConfigToSettings(configWithGeneration);
        this.expectAuditConfigSoon(configWithGeneration);

        // Delete audit config to return to original state.
        for (let conn of [this.st.configRS.getPrimary()].concat(this.st._connections)) {
            assert.commandWorked(conn.getDB('config').settings.deleteOne(
                {_id: 'audit'}, {writeConcern: {w: 'majority'}}));
        }
        this.expectAuditConfigSoon(defaultConfig);
    }

    testConfigHighShardsHighMongosLow() {
        this.testBinariesHighFCVLow();
    }

    testConfigHighShardsMixedMongosLow() {
        this.testBinariesHighFCVLow();
    }

    testConfigHighShardsLowMongosLow() {
        this.testBinariesHighFCVLow();
    }

    testFullyDowngraded() {
        // When we are fully downgraded, setAuditConfig should work again, but setClusterParameter
        // should not.
        this.expectAuditConfigSoon(defaultConfig);
        assert.commandWorked(
            this.st.admin.runCommand(Object.assign({setAuditConfig: 1}, testConfig1)));
        this.expectAuditConfigSoon(testConfig1);
        assert.commandFailed(
            this.st.admin.runCommand({setClusterParameter: {auditConfig: testConfig2}}));
        this.expectAuditConfigSoon(testConfig1);

        // Write directly to config.settings to ensure that we are reading from it when we are fully
        // downgraded.
        const configWithGeneration = Object.assign({generation: ObjectId()}, testConfig2);
        this.writeConfigToSettings(configWithGeneration);
        this.expectAuditConfigSoon(configWithGeneration);

        // Delete audit config to return to original state.
        for (let conn of [this.st.configRS.getPrimary()].concat(this.st._connections)) {
            assert.commandWorked(conn.getDB('config').settings.deleteOne(
                {_id: 'audit'}, {writeConcern: {w: 'majority'}}));
        }
        this.expectAuditConfigSoon(defaultConfig);
    }

    writeConfigToSettings(config) {
        assert.commandWorked(this.st.configRS.getPrimary().getDB("config").runCommand({
            findAndModify: "settings",
            query: {_id: 'audit'},
            update: config,
            upsert: true,
            writeConcern: {w: 'majority'}
        }));
    }

    // Helper which wraps expectAuditConfig with an assert.soonNoExcept.
    expectAuditConfigSoon(expectedConfig, timeout) {
        assert.soonNoExcept(() => {
            try {
                this.expectAuditConfig(expectedConfig);
            } catch (e) {
                jsTest.log("failed expect, retrying");
                jsTest.log(e);
                return false;
            }
            return true;
        }, "Audit config did not match expected within timeframe.", timeout);
    }

    expectAuditConfig(expectedConfig) {
        const mongosConfig =
            assert.commandWorked(this.st.s.getDB('admin').runCommand({getAuditConfig: 1}));
        assert.eq(bsonWoCompare(mongosConfig.filter, expectedConfig.filter), 0);
        assert.eq(mongosConfig.auditAuthorizationSuccess, expectedConfig.auditAuthorizationSuccess);
        if (expectedConfig.clusterParameterTime !== undefined) {
            assertSameTimestamp(expectedConfig.clusterParameterTime,
                                mongosConfig.clusterParameterTime);
        } else if (expectedConfig.generation !== undefined) {
            assertSameOID(expectedConfig.generation, mongosConfig.generation);
        }
        // Note - if config hasn't defined clusterParameterTime or generation, we don't particularly
        // care what it is as long as it matches between all our getAuditConfig calls

        const clusterParametersAuditConfigArray =
            findAllWithMajority(this.st.configRS.getPrimary().getDB('config'),
                                'clusterParameters',
                                {_id: 'auditConfig'});
        const settingsAuditConfigArray = findAllWithMajority(
            this.st.configRS.getPrimary().getDB('config'), 'settings', {_id: 'audit'});
        const hasAuditConfigInClusterParameters = clusterParametersAuditConfigArray.length === 1;
        const hasAuditConfigInSettings = settingsAuditConfigArray.length === 1;
        // We don't expect both config.settings and config.clusterParameters to be set when in
        // stable FCV state.
        assert(!(hasAuditConfigInClusterParameters && hasAuditConfigInSettings));

        for (let conn of [this.st.configRS.getPrimary()].concat(this.st._connections)) {
            const connConfig =
                assert.commandWorked(conn.getDB('admin').runCommand({getAuditConfig: 1}));
            assert.eq(bsonWoCompare(expectedConfig.filter, connConfig.filter), 0);
            assert.eq(expectedConfig.auditAuthorizationSuccess,
                      connConfig.auditAuthorizationSuccess);

            assertSameTimestamp(mongosConfig.clusterParameterTime, connConfig.clusterParameterTime);
            assertSameOID(mongosConfig.generation, connConfig.generation);

            const connClusterParametersAuditConfigArray = findAllWithMajority(
                conn.getDB('config'), 'clusterParameters', {_id: 'auditConfig'});
            assert.eq(
                hasAuditConfigInClusterParameters,
                connClusterParametersAuditConfigArray.length === 1,
                "connection unexpectedly has/does not have audit config in cluster parameters");
            // Note that we are not checking config.settings -- this is because only the config
            // server stores the audit config on disk when audit config cluster parameter is not
            // enabled.
        }
    }

    testUpgradeCluster() {
        assert(this.forUpgradeTest,
               "Cannot test upgrading cluster on a cluster not set up for upgrade testing!");
        this.testFullyDowngraded();

        {  // Step 1: Upgrade config server
            const numConfigs = this.st.configRS.nodes.length;

            for (var i = 0; i < numConfigs; i++) {
                var configSvr = this.st.configRS.nodes[i];

                MongoRunner.stopMongod(configSvr);
                // When we were downgraded, we manually disabled the feature flag. When we now
                // upgrade, we must manually enable it.
                configSvr = MongoRunner.runMongod(Object.assign(
                    {}, this.upgradedConfigOpts, {restart: configSvr, appendOptions: true}));

                this.st["config" + i] = this.st["c" + i] = this.st.configRS.nodes[i] = configSvr;
            }
        }
        this.testConfigHighShardsLowMongosLow();

        {  // Step 2: Upgrade shards
            let first = true;
            this.st._rs.forEach((rs) => {
                if (!first) {
                    this.testConfigHighShardsMixedMongosLow();
                }
                rs.test.upgradeSet(this.upgradedRsOpts);
                first = false;
            });
        }
        this.testConfigHighShardsHighMongosLow();

        {  // Step 3: Upgrade mongos
            MongoRunner.stopMongos(this.st.s);
            const mongos = MongoRunner.runMongos(Object.assign(
                {}, this.upgradedMongosOpts, {restart: this.st.s, appendOptions: true}));
            this.st.s = this.st.s0 = this.st._mongos[0] = mongos;

            this.st.config = this.st.s.getDB("config");
            this.st.admin = this.st.s.getDB("admin");
        }
        this.testBinariesHighFCVLow();

        {  // Step 4: Upgrade FCV
            assert.commandWorked(this.st.s.getDB("admin").runCommand(
                {setFeatureCompatibilityVersion: latestFCV, confirm: true}));
        }
        this.testFullyUpgraded();
    }

    testDowngradeCluster() {
        assert(!this.forUpgradeTest,
               "Cannot test downgrading cluster on a cluster set up for upgrade testing!");
        this.testFullyUpgraded();

        {  // Step 1: Downgrade FCV
            assert.commandWorked(this.st.s.getDB("admin").runCommand(
                {setFeatureCompatibilityVersion: lastLTSFCV, confirm: true}));
        }
        this.testBinariesHighFCVLow();

        {  // Step 2: Downgrade mongos
            MongoRunner.stopMongos(this.st.s);
            const mongos = MongoRunner.runMongos(Object.assign(
                {}, this.downgradedMongosOpts, {restart: this.st.s, appendOptions: true}));
            this.st.s = this.st.s0 = this.st._mongos[0] = mongos;

            this.st.config = this.st.s.getDB("config");
            this.st.admin = this.st.s.getDB("admin");
        }
        this.testConfigHighShardsHighMongosLow();

        {  // Step 3: Downgrade shards
            let first = true;
            this.st._rs.forEach((rs) => {
                if (!first) {
                    this.testConfigHighShardsMixedMongosLow();
                }
                rs.test.upgradeSet(this.downgradedRsOpts);
                first = false;
            });
        }
        this.testConfigHighShardsLowMongosLow();

        {  // Step 4: Downgrade config server
            const numConfigs = this.st.configRS.nodes.length;

            for (var i = 0; i < numConfigs; i++) {
                var configSvr = this.st.configRS.nodes[i];

                MongoRunner.stopMongod(configSvr);
                configSvr = MongoRunner.runMongod(Object.assign(
                    {}, this.downgradedConfigOpts, {restart: configSvr, appendOptions: true}));

                this.st["config" + i] = this.st["c" + i] = this.st.configRS.nodes[i] = configSvr;
            }
        }
        this.testFullyDowngraded();
    }
}

{
    jsTest.log("Testing standalone downgrade...");
    const fixture = new StandaloneUpgradeDowngradeAuditFixture(false /* forUpgradeTest */);
    fixture.testDowngradeStandalone();
    fixture.stop();
}

{
    jsTest.log("Testing standalone upgrade...");
    const fixture = new StandaloneUpgradeDowngradeAuditFixture(true /* forUpgradeTest */);
    fixture.testUpgradeStandalone();
    fixture.stop();
}

{
    jsTest.log("Testing replset downgrade...");
    const fixture = new ReplicaSetUpgradeDowngradeAuditFixture(false /* forUpgradeTest */);
    fixture.testDowngradeReplSet();
    fixture.stop();
}

{
    jsTest.log("Testing replset upgrade...");
    const fixture = new ReplicaSetUpgradeDowngradeAuditFixture(true /* forUpgradeTest */);
    fixture.testUpgradeReplSet();
    fixture.stop();
}

{
    jsTest.log("Testing cluster downgrade...");
    const fixture = new ClusterUpgradeDowngradeAuditFixture(1 /* configPollingFrequencySecs */,
                                                            false /* forUpgradeTest */);
    fixture.testDowngradeCluster();
    fixture.stop();
}

{
    jsTest.log("Testing cluster upgrade...");
    const fixture = new ClusterUpgradeDowngradeAuditFixture(1 /* configPollingFrequencySecs */,
                                                            true /* forUpgradeTest */);
    fixture.testUpgradeCluster();
    fixture.stop();
}
