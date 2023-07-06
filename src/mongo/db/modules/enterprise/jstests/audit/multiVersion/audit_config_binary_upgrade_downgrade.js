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
        let baseOpts;
        if (forUpgradeTest) {
            baseOpts = {
                auditDestination: 'console',
                auditRuntimeConfiguration: true,
                binVersion: 'last-lts',
                setParameter: {featureFlagAuditConfigClusterParameter: false}
            };
        } else {
            baseOpts = {auditDestination: 'console', auditRuntimeConfiguration: true};
        }
        this.opts = Object.assign({}, baseOpts, extraOpts);

        // Standalone connection to perform actions on.
        this.conn = MongoRunner.runMongod(this.opts);
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
        this.conn = MongoRunner.runMongod(Object.assign({}, this.opts, {
            binVersion: 'latest',
            setParameter: Object.assign(
                {}, this.opts.setParameter, {featureFlagAuditConfigClusterParameter: true})
        }));
        this.testUpgraded();
    }

    testDowngradeStandalone() {
        assert(!this.forUpgradeTest,
               "Cannot test downgrading standalone on a node set up for upgrade testing!");
        this.testUpgraded();
        assert.commandWorked(
            this.conn.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV, confirm: true}));
        MongoRunner.stopMongod(this.conn);
        this.conn = MongoRunner.runMongod(Object.assign({}, this.opts, {binVersion: 'last-lts'}));
        this.testDowngraded();
    }
}

class ReplicaSetUpgradeDowngradeAuditFixture extends SingleTargetNodeMultiversionAuditFixture {
    constructor(forUpgradeTest, extraOpts = {}) {
        super();
        let baseOpts;
        if (forUpgradeTest) {
            baseOpts = {
                auditDestination: 'console',
                auditRuntimeConfiguration: true,
                binVersion: 'last-lts',
                setParameter: {featureFlagAuditConfigClusterParameter: false}
            };
        } else {
            baseOpts = {auditDestination: 'console', auditRuntimeConfiguration: true};
        }
        this.opts = Object.assign({}, baseOpts, extraOpts);
        // ReplSetTest to perform actions on.
        this.rst = new ReplSetTest({nodes: 3, nodeOptions: this.opts});
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
        this.rst.upgradeSet({
            binVersion: 'latest',
            setParameter: Object.assign(
                {}, this.opts.setParameter, {featureFlagAuditConfigClusterParameter: true})
        });
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
        this.rst.upgradeSet({binVersion: 'last-lts'});
        this.testDowngraded();
    }
}

class ClusterUpgradeDowngradeAuditFixture {
    constructor(configPollingFrequencySecs,
                forUpgradeTest,
                extraRsOpts = {},
                extraConfigOpts = {},
                extraMongosOpts = {}) {
        let baseOpts;
        if (forUpgradeTest) {
            baseOpts = {
                auditDestination: 'console',
                auditRuntimeConfiguration: true,
                binVersion: 'last-lts',
                setParameter: {
                    auditConfigPollingFrequencySecs: configPollingFrequencySecs,
                    featureFlagAuditConfigClusterParameter: false,
                }
            };
        } else {
            baseOpts = {
                auditDestination: 'console',
                auditRuntimeConfiguration: true,
                setParameter: {
                    auditConfigPollingFrequencySecs: configPollingFrequencySecs,
                }
            };
        }
        let mongosBaseOpts = Object.assign({}, baseOpts);
        mongosBaseOpts.setParameter =
            Object.assign({clusterServerParameterRefreshIntervalSecs: configPollingFrequencySecs},
                          baseOpts.setParameter);
        this.rsOpts = Object.assign({}, baseOpts, extraRsOpts);
        this.configOpts = Object.assign({}, baseOpts, extraConfigOpts);
        this.mongosOpts = Object.assign({}, mongosBaseOpts, extraMongosOpts);
        this.st = new ShardingTest({
            mongos: 1,
            shards: 3,
            config: 1,
            rs: {nodes: 2},
            other: {
                rsOptions: this.rsOpts,
                configOptions: this.configOpts,
                mongosOptions: this.mongosOpts,
            }
        });
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
        this.expectAuditConfig(defaultConfig);
        assert.commandWorked(
            this.st.admin.runCommand(Object.assign({setAuditConfig: 1}, testConfig1)));
        this.expectAuditConfig(testConfig1);
        assert.commandWorked(
            this.st.admin.runCommand({setClusterParameter: {auditConfig: testConfig2}}));
        this.expectAuditConfig(testConfig2);

        // Delete audit config to return to original state.
        for (let conn of [this.st.configRS.getPrimary()].concat(this.st._connections)) {
            assert.commandWorked(conn.getDB('config').clusterParameters.deleteOne(
                {_id: 'auditConfig'}, {writeConcern: {w: 'majority'}}));
        }
        // Since the refresher is a bit behind (the oplog time specified in its transaction is often
        // behind the config server, meaning it won't have the delete), we use the soon expect.
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
    expectAuditConfigSoon(expectedConfig, timeout = 10000) {
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
                configSvr = MongoRunner.runMongod({
                    restart: configSvr,
                    binVersion: 'latest',
                    appendOptions: true,
                    setParameter: Object.assign({},
                                                this.configOpts.setParameter,
                                                {featureFlagAuditConfigClusterParameter: true})
                });

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
                rs.test.upgradeSet({
                    binVersion: 'latest',
                    setParameter: Object.assign({},
                                                this.rsOpts.setParameter,
                                                {featureFlagAuditConfigClusterParameter: true})
                });
                first = false;
            });
        }
        this.testConfigHighShardsHighMongosLow();

        {  // Step 3: Upgrade mongos
            MongoRunner.stopMongos(this.st.s);
            const mongos = MongoRunner.runMongos({
                restart: this.st.s,
                binVersion: 'latest',
                appendOptions: true,
                setParameter: Object.assign({},
                                            this.mongosOpts.setParameter,
                                            {featureFlagAuditConfigClusterParameter: true})
            });
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
            const mongos = MongoRunner.runMongos(
                {restart: this.st.s, binVersion: 'last-lts', appendOptions: true});
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
                rs.test.upgradeSet({binVersion: 'last-lts'});
                first = false;
            });
        }
        this.testConfigHighShardsLowMongosLow();

        {  // Step 4: Downgrade config server
            const numConfigs = this.st.configRS.nodes.length;

            for (var i = 0; i < numConfigs; i++) {
                var configSvr = this.st.configRS.nodes[i];

                MongoRunner.stopMongod(configSvr);
                configSvr = MongoRunner.runMongod(
                    {restart: configSvr, binVersion: 'last-lts', appendOptions: true});

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