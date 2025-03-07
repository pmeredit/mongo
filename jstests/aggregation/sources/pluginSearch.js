const coll = db.docs;
coll.drop();

coll.insertMany([
  { _id: ObjectId("67c52a1933a2e15c422984c7"), title: "Star Trek" },
  { _id: ObjectId("67c52a1933a2e15c422984c6"), title: "Star Wars" },
  { _id: ObjectId("67c52a1b33a2e15c422984c8"), title: "Star Chronicles" },
  { _id: ObjectId("67c52a1b33a2e15c422984c9"), title: "Star Quest" },
  { _id: ObjectId("67c52a1c33a2e15c422984ca"), title: "Star Legends" },
  { _id: ObjectId("67c52a1c33a2e15c422984cb"), title: "Star Empire" },
  { _id: ObjectId("67c52a1d33a2e15c422984cc"), title: "Star Rising" },
  { _id: ObjectId("67c52a1d33a2e15c422984cd"), title: "Star Hunters" },
  { _id: ObjectId("67c52a1d33a2e15c422984ce"), title: "Star Rebels" },
  { _id: ObjectId("67c52a1d33a2e15c422984cf"), title: "Star Universe" }
]);

// no limit, score projected
let fullResults = coll.aggregate([
    {
      $pluginSearch: {
        "text": {
          "path": "title",
          "query": "Star"
        },
        "index": "default"
      }
    },
    {
      $addFields: {
        "score": { 
          "$meta": "searchScore" 
        }
      }
    }
  ]).toArray();

printjson(fullResults);
assert.eq(fullResults.length, 10);
assert(resultsWithScore.every(doc => 
  ["_id", "title", "score"].every(key => key in doc) && Object.keys(doc).length === 3
));

// with limit
let limitedResults = coll.aggregate([
  {
    $pluginSearch: {
      "text": {
         "path": "title",
         "query": "Star"
      },
      "index": "default"
    }
  },
  {
    $limit: 7
  }
]).toArray();

printjson(limitedResults);
assert.eq(limitedResults.length, 7);
assert(resultsWithScore.every(doc => 
  ["_id", "title"].every(key => key in doc) && Object.keys(doc).length === 2
));

// with stored source
let storedSourceResults = coll.aggregate([
  {
    $pluginSearch: {
      "text": {
        "path": "title",
        "query": "Star"
      },
      "index": "default",
      "returnStoredSource": true
    }
  }
]).toArray();

printjson(storedSourceResults);
assert.eq(storedSourceResults.length, 10);

assert(resultsWithScore.every(doc => 
  ["_id"].every(key => key in doc) && Object.keys(doc).length === 1
));
