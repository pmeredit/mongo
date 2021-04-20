// Helper library for exporting and importing collections.

/**
 * Creates and exports the collection with namespace 'dbName.collName'. An optional 'ops' parameter
 * is provided that allows operations to be performed on the collection prior to exporting it.
 */
const exportCollection = function(dbName, collName, ops, username, password) {
    ops = ops || ((coll) => {});

    let standalone = MongoRunner.runMongod({setParameter: "featureFlagLiveImportExport=true"});
    let db = standalone.getDB(dbName);

    // in some tests collection already exists, so we ignore if this command fails
    db.createCollection(collName);

    jsTestLog(`Running operations on collection ${dbName}.${collName}`);
    ops(db.getCollection(collName));
    MongoRunner.stopMongod(standalone);

    // After creating the collection and running operations on it, export it.
    jsTestLog(`Exporting collection ${dbName}.${collName}`);
    let params = {
        setParameter: "featureFlagLiveImportExport=true",
        dbpath: standalone.dbpath,
        noCleanData: true,
        queryableBackupMode: ""
    };
    standalone = MongoRunner.runMongod(params);

    db = standalone.getDB(dbName);
    if (username) {
        db.auth(username, password);
    }

    const collectionProperties = assert.commandWorked(db.runCommand({exportCollection: collName}));
    MongoRunner.stopMongod(standalone);
    return collectionProperties;
};

/**
 * Copies the exported files from the collection properties into the target dbpath.
 * This does not support copying files when running with directoryPerDB or directoryForIndexes.
 */
const copyFilesForExport = function(collectionProperties, targetDbPath) {
    let separator = '/';
    if (_isWindows()) {
        separator = '\\';
    }
    let lastChar = targetDbPath[targetDbPath.length - 1];
    if (lastChar !== '/' && lastChar !== '\\') {
        targetDbPath += separator;
    }

    const collectionFile = collectionProperties.collectionFile;
    let fileName = collectionFile.substring(collectionFile.lastIndexOf(separator) + 1);
    jsTestLog(`Copying collection table ${collectionFile} to ${targetDbPath + fileName}`);
    copyFile(collectionFile, targetDbPath + fileName);

    const indexFiles = collectionProperties.indexFiles;
    for (const indexFile in indexFiles) {
        fileName =
            indexFiles[indexFile].substring(indexFiles[indexFile].lastIndexOf(separator) + 1);
        jsTestLog(`Copying index table ${indexFiles[indexFile]} to ${targetDbPath + fileName}`);
        copyFile(indexFiles[indexFile], targetDbPath + fileName);
    }
};

/**
 * Validates the imported collection.
 */
const validateImportCollection = function(collection, collectionProperties) {
    assert(collection);

    const stats = assert.commandWorked(collection.stats());
    assert.eq(stats.ns, collectionProperties.ns);
    assert.eq(stats.size, collectionProperties.dataSize);
    assert.eq(stats.count, collectionProperties.numRecords);
    assert.eq(stats.count, collection.find({}).itcount());

    const indexes = collectionProperties.metadata.md.indexes;
    assert.eq(stats.nindexes, indexes.length);

    for (const index in indexes) {
        const indexName = indexes[index].spec.name;
        assert(stats.indexSizes.hasOwnProperty(indexName));
    }

    // Do a full validation to check the integrity of the storage engines files.
    assert.commandWorked(collection.validate({full: true, background: false}));
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
