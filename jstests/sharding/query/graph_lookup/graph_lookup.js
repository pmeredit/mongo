// Test aggregating a sharded collection while using $graphLookup on an unsharded collection.
import {ShardingTest} from "jstests/libs/shardingtest.js";

const st = new ShardingTest({shards: 2, rs: {nodes: 1}});

assert.commandWorked(st.s0.adminCommand({enablesharding: "test"}));
assert.commandWorked(st.s0.adminCommand({enablesharding: "foreign_db"}));
assert.commandWorked(st.s0.adminCommand({shardCollection: "test.foo", key: {_id: "hashed"}, primaryShard: st.shard0.name}));
assert.commandWorked(st.s0.adminCommand({shardCollection: "foreign_db.bar", key: {_id: "hashed"}, primaryShard: st.shard1.name}));

let db = st.s0.getDB("test");
let foreignDB = st.s0.getDB("foreign_db");

assert.commandWorked(db.foo.insert([{}, {}, {}, {}]));
assert.commandWorked(db.bar.insert({_id: 1, x: 1}));
assert.commandWorked(foreignDB.bar.insert({_id: 1, x: 1}));

{
    const res = db.foo
                        .aggregate([{
                            $graphLookup: {
                                from: "bar",
                                startWith: {$literal: 1},
                                connectFromField: "x",
                                connectToField: "_id",
                                as: "res"
                            }
                        }])
                        .toArray();

    assert.eq(res.length, 4);
    res.forEach(function(c) {
        assert.eq(c.res.length, 1);
        assert.eq(c.res[0]._id, 1);
        assert.eq(c.res[0].x, 1);
    });
}

{
    const res = db.foo
                        .aggregate([{
                            $graphLookup: {
                                from: {"db": "foreign_db", "coll": "bar"},
                                startWith: {$literal: 1},
                                connectFromField: "x",
                                connectToField: "_id",
                                as: "res"
                            }
                        }])
                        .toArray();

    assert.eq(res.length, 4);
    res.forEach(function(c) {
        assert.eq(c.res.length, 1);
        assert.eq(c.res[0]._id, 1);
        assert.eq(c.res[0].x, 1);
    });
}

// Be sure $graphLookup is banned on sharded foreign collection when the feature flag is disabled
// and allowed when it is enabled.
assert.commandWorked(st.s0.adminCommand({shardCollection: "test.baz", key: {_id: "hashed"}}));
assert.commandWorked(db.baz.insert({_id: 1, x: 1}));

{
    const res = db.foo
                    .aggregate([{
                        $graphLookup: {
                            from: "bar",
                            startWith: {$literal: 1},
                            connectFromField: "x",
                            connectToField: "_id",
                            as: "res"
                        }
                    }])
                    .toArray();

    assert.eq(res.length, 4);
    res.forEach(function(c) {
        assert.eq(c.res.length, 1);
        assert.eq(c.res[0]._id, 1);
        assert.eq(c.res[0].x, 1);
    });
}

st.stop();
