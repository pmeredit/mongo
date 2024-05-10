/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit/audit_manager.h"
#include "audit/audit_mongod.h"
#include "audit/audit_op_observer.h"
#include "mongo/db/catalog/create_collection.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/oplog_interface_local.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/logv2/log.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/options_parser/environment.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace moe = mongo::optionenvironment;

namespace mongo {
namespace audit {
namespace {

const std::vector<NamespaceString> kIgnoredNamespaces = {
    NamespaceString::createNamespaceString_forTest("config"_sd, "other"_sd),
    NamespaceString::createNamespaceString_forTest("admin"_sd, "settings"_sd),
    NamespaceString::createNamespaceString_forTest("test"_sd, "foo"_sd)};

constexpr StringData kATypeField = "atype"_sd;
const OID kNilOID("000000000000000000000000");

BSONObj makeSettingsDoc(const OID& generation, BSONObj filter, bool auditSuccess) {
    return BSON(AuditConfigDocument::k_idFieldName
                << kAuditDocID << AuditConfigDocument::kGenerationFieldName << generation
                << AuditConfigDocument::kFilterFieldName << filter
                << AuditConfigDocument::kAuditAuthorizationSuccessFieldName << auditSuccess);
}

/**
 * Delete the 'audit' document from the config.settings collection.
 */
void deleteConfig() {
    auto opCtx = cc().makeOperationContext();

    BSONObj res;
    DBDirectClient(opCtx.get())
        .runCommand(
            DatabaseName::kConfig,
            [] {
                write_ops::DeleteCommandRequest deleteOp(NamespaceString::kConfigSettingsNamespace);
                deleteOp.setDeletes({[] {
                    write_ops::DeleteOpEntry entry;
                    entry.setQ(BSON(AuditConfigDocument::k_idFieldName << kAuditDocID));
                    entry.setMulti(true);
                    return entry;
                }()});
                return deleteOp.toBSON();
            }(),
            res);

    BatchedCommandResponse response;
    std::string errmsg;
    if (!response.parseBSON(res, &errmsg)) {
        uasserted(ErrorCodes::FailedToParse,
                  str::stream() << "Failed to parse reply to delete command: " << errmsg);
    }
    uassertStatusOK(response.toStatus());
}

class AuditOpObserverTest : public ServiceContextMongoDTest {
public:
    void setUp() final {
        setUpAuditing();

        // Set up mongod.
        ServiceContextMongoDTest::setUp();

        auto service = getServiceContext();
        auto opCtx = cc().makeOperationContext();
        repl::StorageInterface::set(service, std::make_unique<repl::StorageInterfaceMock>());

        // Set up ReplicationCoordinator and create oplog.
        repl::ReplicationCoordinator::set(
            service,
            std::make_unique<repl::ReplicationCoordinatorMock>(service, createReplSettings()));
        repl::createOplog(opCtx.get());

        // Ensure that we are primary.
        auto replCoord = repl::ReplicationCoordinator::get(opCtx.get());
        ASSERT_OK(replCoord->setFollowerMode(repl::MemberState::RS_PRIMARY));

        ASSERT_OK(createCollection(opCtx.get(),
                                   CreateCommand(NamespaceString::kConfigSettingsNamespace)));
        for (auto&& nss : kIgnoredNamespaces) {
            ASSERT_OK(createCollection(opCtx.get(), CreateCommand(nss)));
        }
    }

    void tearDown() final {
        getGlobalAuditManager()->~AuditManager();
        new (getGlobalAuditManager()) AuditManager();
    }

    void doInserts(const NamespaceString& nss, std::initializer_list<BSONObj> docs) {
        std::vector<InsertStatement> stmts;
        std::transform(docs.begin(), docs.end(), std::back_inserter(stmts), [](auto doc) {
            return InsertStatement(doc);
        });
        auto opCtx = cc().makeOperationContext();
        AutoGetCollection autoColl(opCtx.get(), nss, MODE_IX);
        observer.onInserts(opCtx.get(),
                           *autoColl,
                           stmts.cbegin(),
                           stmts.cend(),
                           /*recordIds*/ {},
                           /*fromMigrate=*/std::vector<bool>(stmts.size(), false),
                           /*defaultFromMigrate=*/false);
    }

    void doUpdate(const NamespaceString& nss, BSONObj update, BSONObj updatedDoc) {
        // Actual UUID doesn't matter, just use any...
        BSONObj pre;
        pre.makeOwned();
        CollectionUpdateArgs updateArgs{pre};
        updateArgs.update = update;
        updateArgs.updatedDoc = updatedDoc;
        auto opCtx = cc().makeOperationContext();
        AutoGetCollection autoColl(opCtx.get(), nss, MODE_IX);
        OplogUpdateEntryArgs entryArgs(&updateArgs, *autoColl);
        observer.onUpdate(opCtx.get(), entryArgs);
    }

    void doDelete(const NamespaceString& nss, BSONObj deletedDoc) {
        auto opCtx = cc().makeOperationContext();
        AutoGetCollection autoColl(opCtx.get(), nss, MODE_IX);
        OplogDeleteEntryArgs args;
        observer.aboutToDelete(opCtx.get(), *autoColl, deletedDoc, &args);
        observer.onDelete(opCtx.get(), *autoColl, 1 /* StmtId */, deletedDoc, args);
    }

    void doDropDatabase(StringData dbname) {
        auto opCtx = cc().makeOperationContext();
        observer.onDropDatabase(opCtx.get(),
                                DatabaseName::createDatabaseName_forTest(boost::none, dbname));
    }

    void doRenameCollection(const NamespaceString& fromColl, const NamespaceString& toColl) {
        auto opCtx = cc().makeOperationContext();
        observer.postRenameCollection(opCtx.get(),
                                      fromColl,
                                      toColl,
                                      UUID::gen(),
                                      boost::none /* targetUUID */,
                                      false /* stayTemp */);
    }

    void doImportCollection(const NamespaceString& nss) {
        auto opCtx = cc().makeOperationContext();
        observer.onImportCollection(opCtx.get(),
                                    UUID::gen(),
                                    nss,
                                    10 /* num records */,
                                    1 << 20 /* data size */,
                                    BSONObj() /* catalogEntry */,
                                    BSONObj() /* storageMetadata */,
                                    false /* isDryRun */);
    }

    void doReplicationRollback(const std::vector<NamespaceString>& namespaces) {
        OpObserver::RollbackObserverInfo info;
        for (const auto& nss : namespaces) {
            info.rollbackNamespaces.insert(nss);
        }
        auto opCtx = cc().makeOperationContext();
        observer.onReplicationRollback(opCtx.get(), info);
    }

    // Create an actual config document in storage and fire upsert observer
    AuditConfigDocument initializeAuditConfig(const OID& generation,
                                              BSONObj filter,
                                              bool authSuccess) {
        auto config = makeSettingsDoc(generation, filter, authSuccess);
        auto doc = AuditConfigDocument::parse(IDLParserContext{"initializeAuditConfig"}, config);

        upsertConfig(cc().makeOperationContext().get(), doc);
        doUpdate(NamespaceString::kConfigSettingsNamespace, BSON("$set" << config), config);

        auto* am = getGlobalAuditManager();
        ASSERT_EQ(am->getConfigGeneration(), generation);
        ASSERT_BSONOBJ_EQ(am->getFilterBSON(), filter);
        ASSERT_EQ(am->getAuditAuthorizationSuccess(), authSuccess);

        return doc;
    }

    // Asserts that the audit config state does not change for this action.
    template <typename F>
    void assertIgnored(const NamespaceString& nss, F fn) {
        auto* am = getGlobalAuditManager();
        const auto initialGen = am->getConfigGeneration();
        const auto initialFilter = am->getFilterBSON();
        const auto initialAuthSuccess = am->getAuditAuthorizationSuccess();
        fn(nss);
        ASSERT_EQ(am->getConfigGeneration(), initialGen);
        ASSERT_BSONOBJ_EQ(am->getFilterBSON(), initialFilter);
        ASSERT_EQ(am->getAuditAuthorizationSuccess(), initialAuthSuccess);
    }

    // Asserts that a given action is ignore anywhere outside of config.settings.
    template <typename F>
    void assertIgnoredOtherNamespaces(F fn) {
        for (const auto& nss : kIgnoredNamespaces) {
            assertIgnored(nss, fn);
        }
    }

    // Asserts that a given action is ignored anywhere, even on the config.settings NS.
    template <typename F>
    void assertIgnoredAlways(F fn) {
        assertIgnoredOtherNamespaces(fn);
        assertIgnored(NamespaceString::kConfigSettingsNamespace, fn);
    }

private:
    static void setUpAuditing() {
        auto* am = getGlobalAuditManager();
        if (am->isEnabled()) {
            return;
        }

        moe::Environment env;
        ASSERT_OK(env.set(moe::Key("auditLog.destination"), moe::Value("console")));
        ASSERT_OK(env.set(moe::Key("auditLog.runtimeConfiguration"), moe::Value(true)));
        getGlobalAuditManager()->initialize(env);

        // Validate initial state.
        ASSERT_EQ(am->getConfigGeneration(), kNilOID);
        ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
        ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);
    }

    static repl::ReplSettings createReplSettings() {
        repl::ReplSettings settings;
        settings.setOplogSizeBytes(5 * 1024 * 1024);
        settings.setReplSetString("mySet/node1:12345");
        return settings;
    }

    // Audit op observer is only functional when this feature flag is disabled.
    RAIIServerParameterControllerForTest _controller{"featureFlagAuditConfigClusterParameter",
                                                     false};
    AuditOpObserver observer;
};

TEST_F(AuditOpObserverTest, OnInsertRecord) {
    auto* am = getGlobalAuditManager();
    const auto initialGen = am->getConfigGeneration();
    const auto initialFilter = am->getFilterBSON();
    const auto initialAuthSuccess = am->getAuditAuthorizationSuccess();

    // Insert a record we should care about.
    const auto newGen = OID::gen();
    const auto newFilter = BSON("onInsertRecord" << 1);
    const auto newAuthSuccess = !initialAuthSuccess;
    ASSERT_NE(initialGen, newGen);
    doInserts(NamespaceString::kConfigSettingsNamespace,
              {makeSettingsDoc(newGen, newFilter, newAuthSuccess)});
    ASSERT_EQ(am->getConfigGeneration(), newGen);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), newFilter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), newAuthSuccess);

    // Relevant record, multi-insert.
    const auto finalGen = OID::gen();
    const auto finalFilter = BSON("onInsertRecord" << 2);
    const auto finalAuthSuccess = newAuthSuccess;
    doInserts(NamespaceString::kConfigSettingsNamespace,
              {
                  BSON(AuditConfigDocument::k_idFieldName << "ignored"),
                  makeSettingsDoc(finalGen, finalFilter, finalAuthSuccess),
                  BSON(AuditConfigDocument::k_idFieldName << "alsoIgnored"),
              });
    ASSERT_EQ(am->getConfigGeneration(), finalGen);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), finalFilter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), finalAuthSuccess);

    // Insert plausible records to namespaces we don't care about.
    assertIgnoredOtherNamespaces([this](const auto& nss) {
        doInserts(nss,
                  {makeSettingsDoc(OID::gen(),
                                   BSON("atype"
                                        << "none"),
                                   false)});
    });
    // Plausible on other NS, multi-insert.
    assertIgnoredOtherNamespaces([this](const auto& nss) {
        auto d0 = makeSettingsDoc(OID::gen(), BSONObj(), false);
        auto d1 = makeSettingsDoc(OID::gen(), BSONObj(), false);
        auto d2 = makeSettingsDoc(OID::gen(), BSONObj(), false);
        doInserts(nss, {d0, d1, d2});
    });

    // Non-audit config record ignored on all namespaces.
    assertIgnoredAlways([this](const auto& nss) {
        doInserts(nss,
                  {BSON("_id"
                        << "ignored")});
    });
    // Non-audit, multi-insert.
    assertIgnoredAlways([this](const auto& nss) {
        doInserts(nss,
                  {BSON("_id"
                        << "ignored"),
                   BSON("_id"
                        << "also-ingored")});
    });
}

TEST_F(AuditOpObserverTest, OnUpdateRecord) {
    auto* am = getGlobalAuditManager();
    const auto initialGen = am->getConfigGeneration();
    const auto initialFilter = am->getFilterBSON();
    const auto initialAuthSuccess = am->getAuditAuthorizationSuccess();

    // Legit update.
    const auto newGen = OID::gen();
    const auto newFilter = BSON("atype"
                                << "onUpdateRecord");
    const auto newAuthSuccess = !initialAuthSuccess;
    const auto updatedDoc = makeSettingsDoc(newGen, newFilter, newAuthSuccess);
    const auto update = BSON("$set" << updatedDoc);
    ASSERT_NE(initialGen, newGen);
    doUpdate(NamespaceString::kConfigSettingsNamespace, update, updatedDoc);
    ASSERT_EQ(am->getConfigGeneration(), newGen);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), newFilter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), newAuthSuccess);

    // Plausible doc in wrong namespace.
    assertIgnoredOtherNamespaces([this](const auto& nss) {
        const auto updatedDoc = makeSettingsDoc(OID::gen(),
                                                BSON("a"
                                                     << "b"),
                                                false);
        const auto update = BSON("$set" << updatedDoc);
        doUpdate(nss, update, updatedDoc);
    });

    // Non-audit doc.
    assertIgnoredAlways([this](const auto& nss) {
        const auto updatedDoc = BSON(AuditConfigDocument::k_idFieldName << "ignored");
        const auto update = BSON("$set" << updatedDoc);
        doUpdate(nss, update, updatedDoc);
    });
}

TEST_F(AuditOpObserverTest, onDeleteRecord) {
    auto* am = getGlobalAuditManager();

    // Set up an initial configuration to clear.
    const auto generation = OID::gen();
    const auto filter = BSON("atype"
                             << "onDeleteRecord");
    const auto authSuccess = true;

    auto initialDoc = makeSettingsDoc(generation, filter, authSuccess);
    auto initialDocument =
        AuditConfigDocument::parse(IDLParserContext{"Initialize onDeleteRecord"}, initialDoc);
    upsertConfig(cc().makeOperationContext().get(), initialDocument);
    doUpdate(NamespaceString::kConfigSettingsNamespace, BSON("$set" << initialDoc), initialDoc);
    ASSERT_EQ(am->getConfigGeneration(), generation);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), filter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), authSuccess);

    // Ignore deletes in other namespaces, wither with or without deleted doc.
    assertIgnoredOtherNamespaces(
        [this, initialDoc](const auto& nss) { doDelete(nss, initialDoc); });

    // Ignore deletes where the _id is known to not be 'audit'.
    assertIgnoredAlways([this](const auto& nss) {
        doDelete(nss, BSON(AuditConfigDocument::k_idFieldName << "ignored"));
    });

    // Audit config gets cleared when we claim to have actually deleted 'audit' from settings.
    deleteConfig();
    doDelete(NamespaceString::kConfigSettingsNamespace,
             BSON(AuditConfigDocument::k_idFieldName << kAuditDocID));
    ASSERT_EQ(am->getConfigGeneration(), kNilOID);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);

    // Restore configured state.
    upsertConfig(cc().makeOperationContext().get(), initialDocument);
    doUpdate(NamespaceString::kConfigSettingsNamespace, BSON("$set" << initialDoc), initialDoc);
    ASSERT_EQ(am->getConfigGeneration(), generation);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), filter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), authSuccess);

    // Audit config gets reset from disk when we don't know what was deleted.
    // Since we never actually mutated durable storage, this ends up being initial setup.
    deleteConfig();
    doDelete(NamespaceString::kConfigSettingsNamespace,
             BSON(AuditConfigDocument::k_idFieldName << kAuditDocID));
    ASSERT_EQ(am->getConfigGeneration(), kNilOID);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);
}

TEST_F(AuditOpObserverTest, onDropDatabase) {
    // Set up an initial configuration to clear.
    const auto generation = OID::gen();
    const auto filter = BSON("atype"
                             << "onDropDatabase");
    const auto authSuccess = true;
    const auto doc = initializeAuditConfig(generation, filter, authSuccess);

    // Drop ignorable databases.
    assertIgnoredOtherNamespaces([this](const auto& nss) {
        const auto dbname = nss.db_forTest();
        if (dbname != kConfigDB) {
            doDropDatabase(dbname);
        }
    });

    // Actually drop the config DB.
    deleteConfig();
    doDropDatabase(kConfigDB);

    auto* am = getGlobalAuditManager();
    ASSERT_EQ(am->getConfigGeneration(), kNilOID);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);
}

TEST_F(AuditOpObserverTest, onRenameCollection) {
    // Set up an initial configuration to clear.
    const auto generation = OID::gen();
    const auto filter = BSON("atype"
                             << "onRenameCollection");
    const auto authSuccess = true;
    const auto doc = initializeAuditConfig(generation, filter, authSuccess);

    const NamespaceString kTestFoo = NamespaceString::createNamespaceString_forTest("test", "foo");
    // Rename ignorable collections.
    assertIgnoredOtherNamespaces([&](const auto& nss) { doRenameCollection(nss, kTestFoo); });
    assertIgnoredOtherNamespaces([&](const auto& nss) { doRenameCollection(kTestFoo, nss); });

    auto* am = getGlobalAuditManager();

    // Rename away (and reset to initial)
    doRenameCollection(NamespaceString::kConfigSettingsNamespace, kTestFoo);
    ASSERT_EQ(am->getConfigGeneration(), kNilOID);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);

    // Rename away (and reset to initial)
    doRenameCollection(kTestFoo, NamespaceString::kConfigSettingsNamespace);
    ASSERT_EQ(am->getConfigGeneration(), generation);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), filter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), authSuccess);
}

TEST_F(AuditOpObserverTest, onImportCollection) {
    // Set up an initial configuration to clear.
    const auto generation = OID::gen();
    const auto filter = BSON("atype"
                             << "onImportCollection");
    const auto authSuccess = true;
    initializeAuditConfig(generation, filter, authSuccess);

    auto* am = getGlobalAuditManager();

    // Clear internal state of audit manager without touching the storage.
    doDropDatabase(kConfigDB);
    ASSERT_EQ(am->getConfigGeneration(), kNilOID);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);

    // Import ignorable collections.
    assertIgnoredOtherNamespaces([this](const auto& nss) { doImportCollection(nss); });

    // Import our namespace and pick up the document that's still actually there.
    doImportCollection(NamespaceString::kConfigSettingsNamespace);
    ASSERT_EQ(am->getConfigGeneration(), generation);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), filter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), authSuccess);
}

TEST_F(AuditOpObserverTest, onReplicationRollback) {
    // Set up an initial configuration to clear.
    const auto generation = OID::gen();
    const auto filter = BSON("atype"
                             << "onReplicationRollback");
    const auto authSuccess = true;
    const auto doc = initializeAuditConfig(generation, filter, authSuccess);

    auto* am = getGlobalAuditManager();

    // Clear internal state of audit manager without touching the storage.
    doDropDatabase(kConfigDB);
    ASSERT_EQ(am->getConfigGeneration(), kNilOID);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);

    // Rollback ignorable namespaces (single).
    assertIgnoredOtherNamespaces([this](const auto& nss) { doReplicationRollback({nss}); });

    // Rollback ignorable namespaces (multi).
    doReplicationRollback(kIgnoredNamespaces);
    ASSERT_EQ(am->getConfigGeneration(), kNilOID);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), BSONObj());
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);

    // Rollback settings namespace (which will pick up document inserted earlier).
    doReplicationRollback(
        {kIgnoredNamespaces[0], NamespaceString::kConfigSettingsNamespace, kIgnoredNamespaces[1]});
    ASSERT_EQ(am->getConfigGeneration(), generation);
    ASSERT_BSONOBJ_EQ(am->getFilterBSON(), filter);
    ASSERT_EQ(am->getAuditAuthorizationSuccess(), authSuccess);
}

// Not strictly an observer test, but use the fixture to avoid hassling with setup.
TEST_F(AuditOpObserverTest, InvalidFilterSyntax) {
    const std::vector<BSONObj> kInvalidFilters = {
        // Comparator without subject.
        BSON("$gt" << 1),
        // $in on scalar.
        BSON("x" << BSON("$in" << 42)),
    };

    auto* am = getGlobalAuditManager();
    for (const auto& testcase : kInvalidFilters) {
        AuditConfigDocument config;
        config.set_id(kAuditDocID);
        config.setGeneration(OID::gen());
        config.setFilter(testcase.getOwned());
        config.setAuditAuthorizationSuccess(false);
        ASSERT_THROWS_CODE(
            am->setConfiguration(nullptr, config), DBException, ErrorCodes::BadValue);
    }
}

}  // namespace
}  // namespace audit
}  // namespace mongo
