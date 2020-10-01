// Helper library to export a collection from a standalone.

const exportEmptyCollectionFromStandalone = function(dbName, collName) {
    jsTestLog(`Exporting collection ${dbName}.${collName} from standalone`);
    let standalone = MongoRunner.runMongod({setParameter: "featureFlagLiveImportExport=true"});
    let testDB = standalone.getDB(dbName);
    assert.commandWorked(testDB.createCollection(collName));
    MongoRunner.stopMongod(standalone);

    // Get a sample output of the exportCollection command for the replica set test.
    standalone = MongoRunner.runMongod({
        setParameter: "featureFlagLiveImportExport=true",
        dbpath: standalone.dbpath,
        noCleanData: true,
        queryableBackupMode: ""
    });
    testDB = standalone.getDB(dbName);
    const collectionProperties =
        assert.commandWorked(testDB.runCommand({exportCollection: collName}));
    MongoRunner.stopMongod(standalone);
    return collectionProperties;
};

const assertCollectionExists = function(testDB, collName) {
    const res =
        assert.commandWorked(testDB.runCommand({listCollections: 1, filter: {name: collName}}));
    assert.eq(1, res.cursor.firstBatch.length);
};

const assertCollectionNotFound = function(testDB, collName) {
    const res =
        assert.commandWorked(testDB.runCommand({listCollections: 1, filter: {name: collName}}));
    assert.eq(0, res.cursor.firstBatch.length);
};
