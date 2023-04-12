/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertErrorCode().

function notAllowedFromAggPipeline() {
    db.test1.insert({});
    let coll = db.test1;
    assertErrorCode(coll,
                    [{
                        $tumblingWindow: {
                            interval: {size: 1, unit: "second"},
                            pipeline: [
                                {
                                    $group: {
                                        _id: "$id",
                                        sum: {$sum: "$value"},
                                    }
                                },
                            ]
                        }
                    }],
                    5491300);
}

notAllowedFromAggPipeline();
}());