/**
 * Test releaseMemory command for cursors with sort stages.
 * @tags: [
 *   assumes_read_preference_unchanged,
 *   assumes_superuser_permissions,
 *   does_not_support_transactions,
 *   not_allowed_with_signed_security_token,
 *   requires_fcv_82,
 *   requires_getmore,
 *   uses_getmore_outside_of_transaction,
 * ]
 */

import {DiscoverTopology} from "jstests/libs/discover_topology.js";
import {
    accumulateServerStatusMetric,
    assertReleaseMemoryFailedWithCode
} from "jstests/libs/release_memory_util.js";
import {setParameterOnAllHosts} from "jstests/noPassthrough/libs/server_parameter_helpers.js";

function getServerParameter(knob) {
    return assert.commandWorked(db.adminCommand({getParameter: 1, [knob]: 1}))[knob];
}

function setServerParameter(knob, value) {
    setParameterOnAllHosts(DiscoverTopology.findNonConfigNodes(db.getMongo()), knob, value);
}

const sortMemoryLimitKnob = "internalQueryMaxBlockingSortMemoryUsageBytes";

db.dropDatabase();
const coll = db[jsTestName()];

function getSortSpillCounter() {
    return accumulateServerStatusMetric(db, metrics => metrics.query.sort.spillToDisk);
}

const kDocCount = 40;
for (let i = 0; i < kDocCount; ++i) {
    assert.commandWorked(coll.insertOne({index: i, padding: 'X'.repeat(1024 * 1024)}));
}

function assertCursorSortedByIndex(cursor) {
    for (let i = 0; i < kDocCount; ++i) {
        const doc = cursor.next();
        assert.eq(doc.index, i);
    }
    assert.eq(cursor.hasNext(), false);
}

// Some background queries can use $group and classic $group uses sorter to spill, so this
// background spills can affect server status metrics.
const classicGroupIncreasedSpillingKnob = "internalQueryEnableAggressiveSpillsInGroup";
const classicGroupIncreasedSpillingInitialValue =
    getServerParameter(classicGroupIncreasedSpillingKnob);
setServerParameter(classicGroupIncreasedSpillingKnob, false);

// TODO SERVER-102896 add bounded sort test
// TODO SERVER-99158, SERVER-99179 Add find test

const pipeline = [
    {$_internalInhibitOptimization: {}},
    {$sort: {index: 1, padding: 1}},  // Secondary sort on padding prevents projection pushdown
    {$project: {padding: 0}},
];

let previousSpillCount = getSortSpillCounter();
assertCursorSortedByIndex(coll.aggregate(pipeline));
assert.eq(previousSpillCount, getSortSpillCounter());

{
    const cursor = coll.aggregate(pipeline, {cursor: {batchSize: 1}});
    const cursorId = cursor.getId();
    assert.eq(previousSpillCount, getSortSpillCounter());

    const releaseMemoryRes = db.runCommand({releaseMemory: [cursorId]});
    assert.commandWorked(releaseMemoryRes);
    assert.eq(releaseMemoryRes.cursorsReleased, [cursorId], releaseMemoryRes);
    assert.lt(previousSpillCount, getSortSpillCounter());
    previousSpillCount = getSortSpillCounter();

    assertCursorSortedByIndex(cursor);
}

{
    const cursor = coll.aggregate(pipeline, {cursor: {batchSize: 1}, allowDiskUse: false});
    const cursorId = cursor.getId();

    const releaseMemoryRes = db.runCommand({releaseMemory: [cursorId]});
    assert.commandWorked(releaseMemoryRes);
    assertReleaseMemoryFailedWithCode(
        releaseMemoryRes, cursorId, ErrorCodes.QueryExceededMemoryLimitNoDiskUseAllowed);

    assertCursorSortedByIndex(cursor);
}

{
    const originalKnobValue = getServerParameter(sortMemoryLimitKnob);
    setServerParameter(sortMemoryLimitKnob, 5 * 1024 * 1024);

    const cursor = coll.aggregate(pipeline, {cursor: {batchSize: 1}});
    const cursorId = cursor.getId();
    assert.lt(previousSpillCount, getSortSpillCounter());
    previousSpillCount = getSortSpillCounter();

    const releaseMemoryRes = db.runCommand({releaseMemory: [cursorId]});
    assert.commandWorked(releaseMemoryRes);
    assert.eq(releaseMemoryRes.cursorsReleased, [cursorId], releaseMemoryRes);
    assert.eq(previousSpillCount, getSortSpillCounter());

    assertCursorSortedByIndex(cursor);
    setServerParameter(sortMemoryLimitKnob, originalKnobValue);
}

setServerParameter(classicGroupIncreasedSpillingKnob, classicGroupIncreasedSpillingInitialValue);
