/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

function sampleDataSourceWindowMerge() {
    const uri = 'mongodb://' + db.getMongo().host;
    const name = "sp1";
    let connectionRegistry = [
        {
            name: "sample_solar_1",
            type: 'sample_solar',
            options: {},
        },
        {name: "db1", type: 'atlas', options: {uri: uri}}
    ];
    const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

    sp.process([
        {
            $source: {
                connectionName: "sample_solar_1",
                timeField: {$toDate: "$timestamp"},
                tsFieldOverride: "__ts",
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [
                    {
                        $group: {
                            _id: "$group_id",
                            sumTemp: {$sum: "$obs.temp"},
                            sumWatts: {$sum: "$obs.watts"},
                            pushAll: {$push: "$$ROOT"}
                        }
                    },
                    {$sort: {sum: 1}}
                ]
            }
        },
        {$merge: {into: {connectionName: "db1", db: "test", coll: name}}}
    ]);

    let results = db.getSiblingDB("test").sp1.find({}).toArray();
    assert.gte(results.length, 1);
    for (let result of results) {
        for (let field of ["_id", "_stream_meta", "pushAll", "sumTemp", "sumWatts"]) {
            assert(result.hasOwnProperty(field));
        }
    }
}

sampleDataSourceWindowMerge();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);