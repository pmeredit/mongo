/**
 * Control mongotmock containing extensions for vector search.
 */

load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

function mongotCommandForKnnQuery(
    {queryVector, path, candidates, indexName, filter = null, collName, dbName, collectionUUID}) {
    let cmd =
        {knn: collName, $db: dbName, collectionUUID, queryVector, path, candidates, indexName};

    if (filter) {
        cmd.filter = filter;
    }

    return cmd;
}
