const coll = db.docs;
coll.drop();

coll.insertMany([
  { _id: ObjectId("67c52a1933a2e15c422984c6"), title: "Star Wars", description: [0.313, 0.841] },
  { _id: ObjectId("67c52a1933a2e15c422984c7"), title: "Star Voyager", description: [0.512, 0.678] },
  { _id: ObjectId("67c52a1b33a2e15c422984c8"), title: "Star Chronicles", description: [0.229, 0.905] },
  { _id: ObjectId("67c52a1b33a2e15c422984c9"), title: "Star Quest", description: [0.714, 0.632] },
  { _id: ObjectId("67c52a1c33a2e15c422984ca"), title: "Star Legends", description: [0.401, 0.789] },
  { _id: ObjectId("67c52a1c33a2e15c422984cb"), title: "Star Empire", description: [0.655, 0.233] },
  { _id: ObjectId("67c52a1d33a2e15c422984cc"), title: "Star Rising", description: [0.921, 0.478] },
  { _id: ObjectId("67c52a1d33a2e15c422984cd"), title: "Star Hunters", description: [0.318, 0.846] },
  { _id: ObjectId("67c52a1d33a2e15c422984ce"), title: "Star Rebels", description: [0.537, 0.621] },
  { _id: ObjectId("67c52a1d33a2e15c422984cf"), title: "Star Universe", description: [0.784, 0.356] }
]);

let results = coll.aggregate([
    {
      $pluginVectorSearch: {
        path: "description",
        index: "default",
        queryVector: [0.714, 0.632],
        numCandidates: 10,
        limit: 5
      }
    }
  ]).toArray();

printjson(results);
assert.eq(results.length, 5);
assert(resultsWithScore.every(doc => 
  ["_id", "title", "description"].every(key => key in doc) && Object.keys(doc).length === 3
));

// with score projected
let resultsWithScore = coll.aggregate([
  {
    $pluginVectorSearch: {
      path: "description",
      index: "default",
      queryVector: [0.714, 0.632],
      numCandidates: 10,
      limit: 5
    }
  },
  {
    $addFields: {
      "score": { 
        "$meta": "vectorSearchScore"
      }
    }
  }
]).toArray();

printjson(resultsWithScore);
assert.eq(resultsWithScore.length, 5);
assert(resultsWithScore.every(doc => 
  ["_id", "title", "description", "score"].every(key => key in doc) && Object.keys(doc).length === 4
));
