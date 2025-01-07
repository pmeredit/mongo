/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {sp} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";

// cleanup is used to ignore a few fields in the comparison
function cleanup(docs) {
    for (let doc of docs) {
        if (doc.hasOwnProperty("_dlqMessage")) {
            delete doc._dlqMessage.dlqTime;
            delete doc._dlqMessage.processorName;
        }
    }
    return docs;
}

function lateDataExample() {
    assert(resultsEq(
        cleanup(sp.process([
            {
                $source: {
                    documents: [
                        {a: 1, ts: ISODate("2024-03-01T02:00:00")},
                        {a: 1, ts: ISODate("2024-03-01T02:00:01")},
                        // Set watermark to 02:00:11 (minus 1 ms, minus 3 second allowed lateness)
                        // This will close the 02:00:00-02:00:10 window.
                        {a: 1, ts: ISODate("2024-03-01T02:00:14")},
                        // Late.
                        {a: 1, ts: ISODate("2024-03-01T02:00:02")},
                    ],
                    timeField: "$ts"
                }
            },
            {
                $tumblingWindow: {
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}],
                    interval: {unit: "second", size: NumberInt(10)},
                    allowedLateness: {unit: "second", size: NumberInt(3)}
                }
            }
        ])),
        [
            {
                _id: null,
                count: 2,
                _stream_meta: {
                    source: {type: "generated"},
                    window: {
                        start: ISODate("2024-03-01T02:00:00Z"),
                        end: ISODate("2024-03-01T02:00:10Z")
                    }
                }
            },
            {
                _dlqMessage: {
                    "_stream_meta": {"source": {"type": "generated"}},
                    "errInfo": {"reason": "Input document arrived late."},
                    "operatorName": "GroupOperator",
                    "doc": {
                        "a": 1,
                        "ts": ISODate("2024-03-01T02:00:02Z"),
                        "_ts": ISODate("2024-03-01T02:00:02Z")
                    },
                    "missedWindowStartTimes": [ISODate("2024-03-01T02:00:00.000Z")],
                }
            }
        ],
        true /* verbose */,
        ));
}

lateDataExample();
