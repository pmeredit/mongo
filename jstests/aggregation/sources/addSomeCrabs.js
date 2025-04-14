const coll = db.docs;
coll.drop();

assert.commandWorked(coll.insertMany([{_id: 0, a: 1}, {_id: 1, a: 2}, {_id: 2, a: 3}]));

assert.eq(coll.aggregate([{$addSomeCrabs: 3}]).toArray(), [
    {_id: 0, a: 1, someCrabs: "🦀🦀🦀"},
    {_id: 1, a: 2, someCrabs: "🦀🦀🦀"},
    {_id: 2, a: 3, someCrabs: "🦀🦀🦀"}
]);

assert.eq(coll.aggregate([{$addSomeCrabs: 1}, {$limit: 2}]).toArray(), [
    {_id: 0, a: 1, someCrabs: "🦀"},
    {_id: 1, a: 2, someCrabs: "🦀"},
]);

assert.eq(coll.aggregate([{$match: {a: {"$gte": 2}}}, {$addSomeCrabs: 5}]).toArray(),
          [{_id: 1, a: 2, someCrabs: "🦀🦀🦀🦀🦀"}, {_id: 2, a: 3, someCrabs: "🦀🦀🦀🦀🦀"}]);