/**
 * Tests that cluster server parameters are audited in the scenarios listed below:
 * 1. During initialization.
 * 2. When executing getClusterParameter
 * 3. When executing setClusterParameter. This occurs on config server and shard primaries only
 *    in sharded clusters and on replica set primaries only on non-sharded replica sets.
 * 4. When parameters are updated in-memory. This occurs on all mongods and can be caused by
      setClusterParameter or some replication event like rollback.
 * @tags: [
 *   # Requires all nodes to be running the latest binary.
 *   requires_fcv_61,
 *   does_not_support_stepdowns,
 *   requires_replication,
 *   requires_sharding
 *  ]
 */

import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

import {ReplSetTest} from "jstests/libs/replsettest.js";

const expectedGetSingleClusterParameterAudit = {
    requestedClusterServerParameters: "changeStreamOptions",
};

const expectedGetMultipleClusterParameterAudit = {
    requestedClusterServerParameters: ["changeStreamOptions", "testStrClusterParameter"],
};

const expectedGetAllClusterParameterAudit = {
    requestedClusterServerParameters: "*",
};

const expectedSetUpdateClusterParameterAudit = {
    originalClusterServerParameter: {
        _id: "changeStreamOptions",
        preAndPostImages: {
            expireAfterSeconds: "off",
        },
    },
    updatedClusterServerParameter: {
        _id: "changeStreamOptions",
        preAndPostImages: {
            expireAfterSeconds: NumberLong(30),
        },
    },
};

class ClusterServerParameterAuditFixtureReplicaSet {
    /**
     * Launches a replica set with auditing and retains handles to its primary, secondaries, and
     * audit spoolers for all of those nodes.
     */
    constructor() {
        const rst = ReplSetTest.runReplSetAuditLogger({
            nodes: 3,
        });
        rst.awaitReplication();

        this.rst = rst;
        this.primary = rst.getPrimary();
        this.secondaries = rst.getSecondaries();
        this.nodes = [this.primary].concat(this.secondaries);

        this.primarySpooler = this.primary.auditSpooler();
        this.secondarySpoolers = this.secondaries.map((secondary) => secondary.auditSpooler());
        this.spoolers = [this.primarySpooler].concat(this.secondarySpoolers);
    }

    /**
     * Returns the set of audit spoolers where we expect to find specific entries.
     */
    selectSpoolers(aType) {
        switch (aType) {
            case 'getClusterParameter':
            case 'updateCachedClusterServerParameter':
                return this.spoolers;
            case 'setClusterParameter':
                return [this.primarySpooler];
            default:
                throw "This fixture does not support audit entries of type " + aType;
        }
    }

    /**
     * Check for an audit event observed only on the nodes we expect it to be audited on.
     */
    assertAudited(aType, params) {
        this.rst.awaitReplication();
        const spoolers = this.selectSpoolers(aType);

        spoolers.forEach((spooler) => { spooler.assertEntryRelaxed(aType, params); });
    }

    /**
     * Runs getClusterParameter on all nodes.
     */
    runGetClusterParameter(params) {
        this.nodes.forEach(
            (node) => { assert.commandWorked(node.adminCommand({getClusterParameter: params})); });
    }

    /**
     * Runs setClusterParameter on the primary.
     */
    runSetClusterParameter(param) {
        assert.commandWorked(this.primary.adminCommand({setClusterParameter: param}));
    }

    /**
     * Stop the replica set.
     */
    stop() {
        this.rst.stopSet();
    }
}

class ClusterServerParameterAuditFixtureSharded {
    /**
     * Launches a sharded cluster with auditing and retains handles to its config servers, shards,
     * and mongoses and maintains audit spoolers for all of those nodes.
     */
    constructor() {
        const st = MongoRunner.runShardedClusterAuditLogger();

        this.st = st;

        this.configPrimary = st.configRS.getPrimary();
        this.shardPrimary = st.rs0.getPrimary();
        this.mongos = st.s0;

        this.nodes = [].concat(this.configPrimary).concat(this.shardPrimary).concat(this.mongos);

        this.configPrimarySpooler = this.configPrimary.auditSpooler();
        this.shardPrimarySpooler = this.shardPrimary.auditSpooler();
        this.mongosSpooler = this.mongos.auditSpooler();

        this.mongodSpoolers = [].concat(this.configPrimarySpooler).concat(this.shardPrimarySpooler);
        this.spoolers = [].concat(this.mongodSpoolers).concat(this.mongosSpooler);
    }

    /**
     * Returns the set of audit spoolers where we expect to find specific entries.
     */
    selectSpoolers(aType) {
        switch (aType) {
            case 'getClusterParameter':
                return this.spoolers;
            case 'updateCachedClusterServerParameter':
                return this.spoolers;
            case 'setClusterParameter':
                return this.mongodSpoolers;
            default:
                throw "This fixture does not support audit entries of type " + aType;
        }
    }

    /**
     * Check for an audit event observed only on the nodes we expect it to be audited on.
     */
    assertAudited(aType, params) {
        this.st.configRS.awaitReplication();
        this.st.rs0.awaitReplication();

        const spoolers = this.selectSpoolers(aType);

        spoolers.forEach((spooler) => { spooler.assertEntryRelaxed(aType, params, 35000); });
    }

    /**
     * Runs getClusterParameter on all nodes.
     */
    runGetClusterParameter(params) {
        this.nodes.forEach(
            (node) => { assert.commandWorked(node.adminCommand({getClusterParameter: params})); });
    }

    /**
     * Runs setClusterParameter on the mongos.
     */
    runSetClusterParameter(param) {
        assert.commandWorked(this.mongos.adminCommand({setClusterParameter: param}));
    }

    /**
     * Stop the sharded cluster.
     */
    stop() {
        this.st.stop();
    }
}

function runTest(fixture) {
    // Check that getClusterParameter is audited on all nodes.
    fixture.runGetClusterParameter("changeStreamOptions");
    fixture.assertAudited('getClusterParameter', expectedGetSingleClusterParameterAudit);
    fixture.runGetClusterParameter(["changeStreamOptions", "testStrClusterParameter"]);
    fixture.assertAudited('getClusterParameter', expectedGetMultipleClusterParameterAudit);
    fixture.runGetClusterParameter("*");
    fixture.assertAudited('getClusterParameter', expectedGetAllClusterParameterAudit);

    // Check that setClusterParameter is audited on the config and shard primaries.
    fixture.runSetClusterParameter(
        {changeStreamOptions: {preAndPostImages: {expireAfterSeconds: 30}}});
    fixture.assertAudited('setClusterParameter', expectedSetUpdateClusterParameterAudit);

    // Check that the in-memory cluster parameter updates were audited on all nodes.
    fixture.assertAudited('updateCachedClusterServerParameter',
                          expectedSetUpdateClusterParameterAudit);
}

{
    // Replica set.
    const rst = new ClusterServerParameterAuditFixtureReplicaSet();
    runTest(rst);
    rst.stop();
}

{
    // Sharded cluster
    const st = new ClusterServerParameterAuditFixtureSharded();
    runTest(st);
    st.stop();
}
