/**
 * Test the basic operation of a `$searchBeta` aggregation stage.
 */
(function() {
    "use strict";

    let conn = MongoRunner.runMongod();
    let db = conn.getDB("test");
    const collName = "search_beta";
    db[collName].drop();

    assert.writeOK(db[collName].insert({"_id": 1, "title": "cakes"}));
    assert.writeOK(db[collName].insert({"_id": 2, "title": "cookies and cakes"}));
    assert.writeOK(db[collName].insert({"_id": 3, "title": "vegetables"}));
    assert.writeOK(db[collName].insert({"_id": 4, "title": "oranges"}));

    // Perform a $searchBeta query.
    let response = assert.commandWorked(db.runCommand(
        {aggregate: collName, pipeline: [{$searchBeta: {}}], cursor: {batchSize: 2}}));
    let cursorId = response.cursor.id;

    // Sanity check the results.
    assert.neq(0, cursorId);
    assert.eq(2, response.cursor.firstBatch.length);

    response = assert.commandWorked(db.runCommand(
        {aggregate: collName, pipeline: [{$searchBeta: {}}, {$sort: {"_id": 1}}], cursor: {}}));

    const expectedReturnDocs = [
        {"_id": 1, "title": "cakes"},
        {"_id": 2, "title": "cookies and cakes"},
        {"_id": 3, "title": "vegetables"},
        {"_id": 4, "title": "oranges"}
    ];

    assert.eq(4, response.cursor.firstBatch.length);
    assert.eq(expectedReturnDocs, response.cursor.firstBatch);

    // Fail on non-local read concern.
    assert.commandFailed(db.runCommand({
        aggregate: collName,
        pipeline: [{$searchBeta: {}}],
        options: {readConcern: "majority", cursor: {}}
    }));

    MongoRunner.stopMongod(conn);
})();
