const coll = db.docs;
coll.drop(); // no need for a collection as the remote mongot we test here gets the docs from a remote mongod

let fullResults = db.coll.aggregate([
    {
      $pluginSearch: {
        "text": {
          "path": "title",
          "query": "Star"
        },
        "index": "default"
      }
    }
  ]).toArray();

assert.eq(fullResults.length, 58);
assert(fullResults.every(doc => Object.keys(doc).length === 2 && "_id" in doc && "$searchScore" in doc));

let limitedResults = db.coll.aggregate([
  {
    $pluginSearch: {
      "text": {
         "path": "title",
         "query": "Trek"
      },
      "index": "default"
    }
  },
  {
    $limit: 17
  }
]).toArray();

assert.eq(limitedResults.length, 17);
assert(limitedResults.every(doc => Object.keys(doc).length === 2 && "_id" in doc && "$searchScore" in doc));
