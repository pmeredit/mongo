import {createSearchIndex, dropSearchIndex} from "jstests/libs/search.js";

const collName = "docs";
const coll = db.getCollection(collName);
coll.drop();

coll.insertMany([
    {_id: 0, title: "Star Trek"},
    {_id: 1, title: "Star Wars"},
    {_id: 2, title: "Star Chronicles"},
    {_id: 3, title: "Star Quest"},
    {_id: 4, title: "Star Legends"},
    {_id: 5, title: "Star Empire"},
    {_id: 6, title: "Star Rising"},
    {_id: 7, title: "Star Hunters"},
    {_id: 8, title: "Star Rebels"},
    {_id: 9, title: "Star Universe"}
]);

const indexSpec = {
    name: "default",
    definition:
        {"name": "default", "mappings": {"dynamic": true}, "storedSource": {"exclude": ["title"]}}
};

createSearchIndex(coll, indexSpec);

// no limit, score projected
let fullResults =
    coll.aggregate([
            {$pluginSearch: {"text": {"path": "title", "query": "Star Trek"}, "index": "default"}},
            // TODO to work on sharded, requires DocumentSourceExtension::getDependencies
            // to specify that stage produces $searchScore
            // {
            //   $addFields: {
            //     "score": {
            //       "$meta": "searchScore"
            //     }
            //   }
            // }
        ])
        .toArray();

printjson(fullResults);
assert.eq(fullResults.length, 10);
assert(fullResults.every(doc => ["_id", "title"].every(key => key in doc) &&
                             Object.keys(doc).length === 2));

// with limit
let limitedResults =
    coll.aggregate([
            {$pluginSearch: {"text": {"path": "title", "query": "Star"}, "index": "default"}},
            {$limit: 7}
        ])
        .toArray();

printjson(limitedResults);
assert.eq(limitedResults.length, 7);
assert(limitedResults.every(doc => ["_id", "title"].every(key => key in doc) &&
                                Object.keys(doc).length === 2));

// with stored source
let storedSourceResults = coll.aggregate([{
                                  $pluginSearch: {
                                      "text": {"path": "title", "query": "Star"},
                                      "index": "default",
                                      "returnStoredSource": true
                                  }
                              }])
                              .toArray();

printjson(storedSourceResults);
assert.eq(storedSourceResults.length, 10);

assert(storedSourceResults.every(doc => ["_id"].every(key => key in doc) &&
                                     Object.keys(doc).length === 1));

dropSearchIndex(coll, {name: "default"});
