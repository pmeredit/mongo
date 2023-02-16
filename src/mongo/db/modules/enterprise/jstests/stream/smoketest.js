/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

let result = db.runCommand({_startStreamProcessor: '', pipeline: []});
jsTestLog(result);
assert.eq(result["ok"], 1);
}());