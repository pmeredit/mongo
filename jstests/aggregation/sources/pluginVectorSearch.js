const coll = db.docs;
coll.drop(); // no need for a collection as the remote mongot we test here gets the docs from a remote mongod

let results = coll.aggregate([
    {
      $pluginVectorSearch: {
        path: "description",
        index: "default",
        queryVector: [1, 2],
        numCandidates: 20,
        limit: 5
      }
    }
  ]).toArray();

printjson(results);
assert.eq(results.length, 5);
assert(results.every(doc => Object.keys(doc).length === 2 && "_id" in doc && "$vectorSearchScore" in doc));
