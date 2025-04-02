/**
 * Verifies the behavior of the "featureFlagInDevelopmentForTest" test Incremental Feature Rollout
 * (IFR) flag. Unlike regular feature flags, IFR flags can be toggled at runtime.
 * @tags: [
 *   # setParameter.
 *   command_not_supported_in_serverless,
 *
 *   # All getParameter, setParameter, and serverStatus commands must go to the same node.
 *   does_not_support_stepdowns,
 *
 *   # Changes to the test feature flag by a simultaneous test would affect this test.
 *   incompatible_with_concurrency_simultaneous,
 *
 *   # Earlier versions of the server do not define the "featureFlagInDevelopmentForTest" server
 *   # parameter.
 *   requires_fcv_82,
 *   # TODO (SERVER-102377): Re-enable this test.
 *   DISABLED_TEMPORARILY_DUE_TO_FCV_UPGRADE,
 * ]
 */

// Call the getParameter command, verify that it returns a document for the given flag, and return
// the value for that flag.
function queryIncrementalFeatureFlagViaGetParameter(flagName) {
    const flagValue = assert.commandWorked(
        db.adminCommand({getParameter: 1, featureFlagInDevelopmentForTest: {}}));
    assert("featureFlagInDevelopmentForTest" in flagValue, flagValue);
    return flagValue.featureFlagInDevelopmentForTest.value;
}

// Call the serverStatus command, verify that is has an "incrementalRollout" section, verify that
// the section has exactly one entry matching the given name, and return that entry.
function queryIncrementalFeatureFlagViaServerStatus(flagName) {
    const serverStatus = assert.commandWorked(db.adminCommand({serverStatus: 1}));
    assert("incrementalRollout" in serverStatus, serverStatus);
    assert(Array.isArray(serverStatus.incrementalRollout.featureFlags), serverStatus);

    const matchingStatuses =
        serverStatus.incrementalRollout.featureFlags.filter(status => status.name == flagName);
    assert.eq(matchingStatuses.length, 1, serverStatus.incrementalRollout);
    return matchingStatuses[0];
}

// Get the initial value of the "dish" feature flag.
const initialFeatureFlagInDevelopmentForTestValue =
    queryIncrementalFeatureFlagViaGetParameter("featureFlagInDevelopmentForTest");

// Check that the "dish" feature flag gets reported by the "serverStatus" command and indicates the
// same value we got from the "getParameter" command.
const initialDishStatus =
    queryIncrementalFeatureFlagViaServerStatus("featureFlagInDevelopmentForTest");

assert.eq(initialDishStatus.value, initialFeatureFlagInDevelopmentForTestValue, initialDishStatus);
assert("falseChecks" in initialDishStatus, initialDishStatus);
assert("trueChecks" in initialDishStatus, initialDishStatus);
assert("numToggles" in initialDishStatus, initialDishStatus);

// Check that it's possible to change the feature flag's value at runtime.
const newFeatureFlagInDevelopmentForTestValue = !initialFeatureFlagInDevelopmentForTestValue;
assert.commandWorked(db.adminCommand(
    {setParameter: 1, featureFlagInDevelopmentForTest: newFeatureFlagInDevelopmentForTestValue}));
assert.eq(queryIncrementalFeatureFlagViaGetParameter("featureFlagInDevelopmentForTest"),
          newFeatureFlagInDevelopmentForTestValue);

// Check that changing the value of the feature flag increments its "numToggles" count but not the
// "falseChecks" or "trueChecks" counts.
const updatedDishStatus =
    queryIncrementalFeatureFlagViaServerStatus("featureFlagInDevelopmentForTest");
assert.docEq(Object.assign({}, initialDishStatus, {
    value: newFeatureFlagInDevelopmentForTestValue,
    numToggles: initialDishStatus.numToggles + 1
}),
             updatedDishStatus);

// Check that a no-op "setParameter" command that sets the flag to its existing value does not
// increment its "numToggles" count.
assert.commandWorked(db.adminCommand(
    {setParameter: 1, featureFlagInDevelopmentForTest: newFeatureFlagInDevelopmentForTestValue}));
assert.docEq(queryIncrementalFeatureFlagViaServerStatus("featureFlagInDevelopmentForTest"),
             updatedDishStatus);
