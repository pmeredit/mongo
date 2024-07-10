// Testing the output of OCSF process activity.
//
// Does not need to test all edge cases, that should be taken care
// of in the mongo audit event tests.

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kEntityActivityCreate = 1;
const kEntityActivityUpdate = 3;
const kEntityActivityDelete = 4;

const kEntityManagementCategory = 3;
const kEntityManagementClass = 3004;

const kEntityId = "entity";
const kEntityResultId = "entity_result";

// Database
const kCreateDatabaseValue = "create_database";
const kDropDatabaseValue = "drop_database";

// Collection
const kCollectionValue = "collection";
const kCreateCollectionValue = "create_collection";
const kRenameCollectionSourceValue = "rename_collection_source";
const kRenameCollectionDestinationValue = "rename_collection_destination";
const kDropCollectionValue = "drop_collection";

// Index
const kCreateIndexValue = "create_index";
const kDropIndexValue = "drop_index";

// View
const kCreateViewValue = "create_view";
const kDropViewValue = "drop_view";

function entitySearch(line, entityName, entityType, entityResultName, entityResultType) {
    let entity = line[kEntityId];

    if (entity["name"] !== entityName) {
        return false;
    }
    if (entity["type"] !== entityType) {
        return false;
    }

    if (!entityResultName) {
        return true;
    }

    let entityResult = line[kEntityResultId];
    if (!entityResult) {
        return false;
    }

    if (entityResult["name"] !== entityResultName) {
        return false;
    }

    if (entityResult["type"] !== entityResultType) {
        return false;
    }

    return true;
}

/*
 * Searches for a particular string in an audit line (uses raw string matching)
 * and does not increment the audit line for the spooler.
 */
function assertEntrySearch(
    audit, activity, entityName, entityType, entityResultName, entityResultType) {
    let line;

    assert.soon(() => {
        let lines =
            audit.findAllEntries(kEntityManagementCategory, kEntityManagementClass, activity);
        for (var idx in lines) {
            line = lines[idx];
            if (entitySearch(line, entityName, entityType, entityResultName, entityResultType)) {
                return true;
            }
        }
        return false;
    });

    return line;
}

{
    const standaloneFixture = new StandaloneFixture();
    jsTest.log("Testing OCSF entity management output on standalone");

    const {conn, audit, admin} =
        standaloneFixture.startProcess({"auditSchema": "OCSF"}, "JSON", "ocsf");

    standaloneFixture.createUserAndAuth();
    audit.fastForward();

    {
        const testDb = admin.getSiblingDB("test");
        assert.commandWorked(testDb.coll.insert({"foo": "bar"}));

        const index = {a: 1};
        assert.commandWorked(testDb.coll.createIndex(index));

        const createdNamespace = "test.coll";
        const viewNamespace = "test.foo";

        assertEntrySearch(audit, kEntityActivityCreate, "test", kCreateDatabaseValue);
        assertEntrySearch(audit, kEntityActivityCreate, createdNamespace, kCreateCollectionValue);
        assertEntrySearch(
            audit, kEntityActivityCreate, "_id_@" + createdNamespace, kCreateIndexValue);

        audit.fastForward();
        testDb.createView("foo", "coll", []);
        assertEntrySearch(audit,
                          kEntityActivityCreate,
                          createdNamespace,
                          kCollectionValue,
                          viewNamespace,
                          kCreateViewValue);

        audit.fastForward();
        testDb.foo.drop();
        assertEntrySearch(audit,
                          kEntityActivityDelete,
                          createdNamespace,
                          kCollectionValue,
                          viewNamespace,
                          kDropViewValue);

        audit.fastForward();
        testDb.coll.renameCollection("test");
        assertEntrySearch(audit,
                          kEntityActivityUpdate,
                          createdNamespace,
                          kRenameCollectionSourceValue,
                          "test.test",
                          kRenameCollectionDestinationValue);
        testDb.test.renameCollection("coll");

        audit.fastForward();
        testDb.coll.dropIndex(index);
        assertEntrySearch(audit, kEntityActivityDelete, "a_1@" + createdNamespace, kDropIndexValue);

        audit.fastForward();
        testDb.dropDatabase();
        assertEntrySearch(audit, kEntityActivityDelete, createdNamespace, kDropCollectionValue);
        assertEntrySearch(audit, kEntityActivityDelete, "test", kDropDatabaseValue);
    }

    standaloneFixture.stopProcess();
}
