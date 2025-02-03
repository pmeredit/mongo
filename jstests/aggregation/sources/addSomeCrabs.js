const coll = db.docs;
coll.drop();

assert.commandWorked(coll.insertMany([
    {_id: 0, a: 1},
    {_id: 1, a: 2},
    {_id: 2, a: 3}
]));

assert.eq(coll.aggregate([{$addSomeCrabs:{}}]).toArray(), [
    {_id: 0, a: 1, some_crabs: "🦀🦀🦀"},
    {_id: 1, a: 2, some_crabs: "🦀🦀🦀"},
    {_id: 2, a: 3, some_crabs: "🦀🦀🦀"}
]);

assert.eq(coll.aggregate([{$addSomeCrabs:{}}, {$limit: 2}]).toArray(), [
    {_id: 0, a: 1, some_crabs: "🦀🦀🦀"},
    {_id: 1, a: 2, some_crabs: "🦀🦀🦀"},
]);

assert.eq(coll.aggregate([{$match: {a: {"$gte": 2}}}, {$addSomeCrabs:{}}]).toArray(), [
    {_id: 1, a: 2, some_crabs: "🦀🦀🦀"},
    {_id: 2, a: 3, some_crabs: "🦀🦀🦀"}
]);