const coll = db.docs;
coll.drop();
let sortedByA = [
    {_id: 3, a: -2},
    {_id: 0, a: 1},
    {_id: 1, a: 2},
    {_id: 4, a: 3},
    {_id: 2, a: 4},
    {_id: 5, a: 7},
];

let sortedById = [
    {_id: 0, a: 1},
    {_id: 1, a: 2},
    {_id: 2, a: 4},
    {_id: 3, a: -2},
    {_id: 4, a: 3},
    {_id: 5, a: 7},
];

assert.commandWorked(coll.insertMany(sortedById));

assert.eq(coll.aggregate([{$pluginSort: "a"}]).toArray(), sortedByA);
assert.eq(coll.aggregate([{$pluginSort: "_id"}]).toArray(), sortedById);
