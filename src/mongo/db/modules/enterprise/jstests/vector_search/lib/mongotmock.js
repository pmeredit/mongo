/**
 * Control mongotmock containing extensions for vector search.
 */

load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

function mongotCommandForKnnQuery({
    queryVector,
    path,
    numCandidates,
    limit,
    index = null,
    filter = null,
    collName,
    dbName,
    collectionUUID
}) {
    assert.eq(arguments.length, 1, "Expected one argument to mongotCommandForKnnQuery()");
    let cmd = {
        knn: collName,
        $db: dbName,
        collectionUUID,
        queryVector,
        path,
        numCandidates,
        limit,
    };

    if (index) {
        cmd.index = index;
    }

    if (filter) {
        cmd.filter = filter;
    }

    return cmd;
}
