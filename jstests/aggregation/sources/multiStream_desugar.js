
const coll = db.docs;
coll.drop();

const res = coll.aggregate(
                    [
                        {
                            $betaMultiStream: {
                                primary: [{$echoOxide: {"hi": "there!"}}],
                                secondary: [{$helloWorldWithFiveCrabs: {}}],
                                finishMethod: "setVar"
                            }
                        },
                        {$project: {hi: 1, meta: "$$SEARCH_META"}}
                    ],
                    {cursor: {batchSize: 0}})
                .toArray();

assert.eq(res.length, 1);
assert.eq(res[0], {"hi": "there!", meta: {hello: "world", someCrabs: "🦀🦀🦀🦀🦀"}});
