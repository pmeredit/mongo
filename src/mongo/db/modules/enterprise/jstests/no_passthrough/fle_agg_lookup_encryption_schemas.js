/**
 * Tests feature flag for $lookup support with multiple encryption schemas.
 * TODO SERVER-91167 this test can be removed when the feature flag is removed.
 *  * @tags: [
 *   featureFlagLookupEncryptionSchemasFLE_incompatible
 * ]
 */
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";

function testFeatureFlag(mongodParams, testFunc) {
    const conn = MongoRunner.runMongod(mongodParams);
    assert.neq(null, conn, 'failed to start mongod');

    const testDB = conn.getDB('test');
    testFunc(testDB);
    MongoRunner.stopMongod(conn);
}

// Test feature enabling.
testFeatureFlag({setParameter: {featureFlagLookupEncryptionSchemasFLE: true}}, function(testDB) {
    assert(FeatureFlagUtil.isPresentAndEnabled(testDB, "LookupEncryptionSchemasFLE"),
           "featureFlagLookupEncryptionSchemasFLE is undefined or disabled");
});

// Test the existence of feature flag and disabled value.
testFeatureFlag({}, function(testDB) {
    assert(FeatureFlagUtil.isPresentAndDisabled(testDB, "LookupEncryptionSchemasFLE"),
           "featureFlagLookupEncryptionSchemasFLE is undefined or enabled");
});
