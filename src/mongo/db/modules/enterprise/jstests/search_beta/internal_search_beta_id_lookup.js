/**
 * Test the basic operation of a `$_internalSearchBetaIdLookup` aggregation stage.
 */

(function() {
    "use strict";

    let conn = MongoRunner.runMongod();
    let db = conn.getDB("test");
    const collName = "internal_search_beta_id_lookup";
    db[collName].drop();

    assert.writeOK(db[collName].insert({_id: 1, x: "ow"}));
    assert.writeOK(db[collName].insert({_id: 2, x: "now", y: "lorem"}));
    assert.writeOK(db[collName].insert({_id: 3, x: "brown", y: "ipsum"}));
    assert.writeOK(db[collName].insert({_id: 4, x: "cow", y: "lorem ipsum"}));
    assert.writeOK(db[collName].insert({_id: 5, x: "brown", y: "ipsum"}));
    assert.writeOK(db[collName].insert({_id: 6, x: "cow", y: "lorem ipsum"}));
    assert.writeOK(db[collName].insert({_id: 7, x: "brown", y: "ipsum"}));
    assert.writeOK(db[collName].insert({_id: 8, x: "cow", y: "lorem ipsum"}));

    // Demonstrate that $_internalSearchBeta skips `_id`s it cannot find.
    assert.eq(4,
              db[collName]
                  .aggregate([
                      {$match: {}},
                      {$addFields: {idToLookFor: {$toInt: "$_id"}}},
                      {$project: {'_id': {$multiply: ['$idToLookFor', 2]}}},
                      {$_internalSearchBetaIdLookup: {}}
                  ])
                  .itcount());

    // Check returned documents are what we expect after skipping some `id`s.
    const expectedEvenIdDocs = [
        {_id: 2, x: "now", y: "lorem"},
        {_id: 4, x: "cow", y: "lorem ipsum"},
        {_id: 6, x: "cow", y: "lorem ipsum"},
        {_id: 8, x: "cow", y: "lorem ipsum"}
    ];
    assert.eq(expectedEvenIdDocs,
              db[collName]
                  .aggregate([
                      {$match: {}},
                      {$addFields: {idToLookFor: {$toInt: "$_id"}}},
                      {$project: {'_id': {$multiply: ['$idToLookFor', 2]}}},
                      {$_internalSearchBetaIdLookup: {}},
                      {$sort: {'_id': 1}}
                  ])
                  .toArray());

    MongoRunner.stopMongod(conn);
})();
