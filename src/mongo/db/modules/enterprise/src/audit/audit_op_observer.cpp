/**
 *    Copyright (C) 2021 MongoDB Inc.
 */


#include "audit/audit_op_observer.h"

#include "audit/audit_manager.h"
#include "audit/audit_mongod.h"
#include "mongo/db/audit.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {
namespace audit {

namespace {
const auto deletingAuditConfigDecoration = OperationContext::declareDecoration<bool>();

bool isAuditingConfigured() {
    return getGlobalAuditManager()->getConfigGeneration().isSet();
}

constexpr auto kIDField = "_id"_sd;
bool isAuditDoc(BSONObj doc) {
    auto idElem = doc[kIDField];
    return (idElem.type() == String) && (idElem.valueStringData() == kAuditDocID);
}

bool isConfigNamespace(const NamespaceString& nss) {
    return nss == kSettingsNS;
}

boost::optional<BSONObj> getAuditConfigurationFromDisk(OperationContext* opCtx) {
    AutoGetCollectionForReadCommandMaybeLockFree ctx(opCtx, kSettingsNS);
    BSONObj res;

    if (Helpers::findOne(opCtx, ctx.getCollection(), BSON("_id" << kAuditDocID), res)) {
        return res.getOwned();
    }

    return boost::none;
}
}  // namespace

void AuditOpObserver::updateAuditConfig(Client* client, const AuditConfigDocument& config) try {
    getGlobalAuditManager()->setConfiguration(client, config);
} catch (const DBException& ex) {
    // In practice, this should only result when filter is unparsable as a match expression,
    // and that should never happen because the setAuditConfig command would have failed.
    // This should only result from a user manually manipulating the collection document
    // and at that point all warranties are void.
    LOGV2_ERROR(
        5497200, "Failed updating audit filter configuration", "status"_attr = ex.toStatus());
}

void AuditOpObserver::clearAuditConfig(Client* client) {
    updateAuditConfig(client, {OID(), {}, false});
}

void AuditOpObserver::updateAuditConfigFromDisk(OperationContext* opCtx) {
    auto obj = getAuditConfigurationFromDisk(opCtx);
    if (!obj) {
        clearAuditConfig(opCtx->getClient());
        return;
    }

    AuditConfigDocument doc;
    try {
        doc =
            AuditConfigDocument::parse(IDLParserContext{"updateAuditConfigFromDisk"}, obj.value());
    } catch (const DBException& ex) {
        LOGV2_ERROR(5497201,
                    "Failed parsing update operation for runtime audit configuration",
                    "status"_attr = ex.toStatus(),
                    "config"_attr = obj.value());
        return;
    }
    updateAuditConfig(opCtx->getClient(), doc);
}

void AuditOpObserver::onInserts(OperationContext* opCtx,
                                const CollectionPtr& coll,
                                std::vector<InsertStatement>::const_iterator first,
                                std::vector<InsertStatement>::const_iterator last,
                                bool fromMigrate) {
    if (!isConfigNamespace(coll->ns())) {
        return;
    }

    for (auto it = first; it != last; ++it) {
        if (!isAuditDoc(it->doc)) {
            continue;
        }

        AuditConfigDocument doc;
        try {
            doc =
                AuditConfigDocument::parse(IDLParserContext{"AuditOpObserver::onInserts"}, it->doc);
        } catch (const DBException& ex) {
            LOGV2_ERROR(5497210,
                        "Failed parsing insert operation for runtime audit configuration",
                        "status"_attr = ex.toStatus(),
                        "config"_attr = it->doc);
            continue;
        }
        updateAuditConfig(opCtx->getClient(), doc);
    }
}

void AuditOpObserver::onUpdate(OperationContext* opCtx, const OplogUpdateEntryArgs& args) {
    auto updatedDoc = args.updateArgs->updatedDoc;
    if (!isConfigNamespace(args.nss) || !isAuditDoc(updatedDoc) ||
        args.updateArgs->update.isEmpty()) {
        return;
    }

    AuditConfigDocument doc;
    try {
        doc = AuditConfigDocument::parse(IDLParserContext{"AuditOpObserver::onUpdate"}, updatedDoc);
    } catch (const DBException& ex) {
        LOGV2_ERROR(5497211,
                    "Failed parsing update operation for runtime audit configuration",
                    "status"_attr = ex.toStatus(),
                    "config"_attr = updatedDoc);
        return;
    }
    updateAuditConfig(opCtx->getClient(), doc);
}

void AuditOpObserver::aboutToDelete(OperationContext* opCtx,
                                    const NamespaceString& nss,
                                    const UUID& uuid,
                                    const BSONObj& doc) {
    deletingAuditConfigDecoration(opCtx) =
        isAuditingConfigured() && isConfigNamespace(nss) && isAuditDoc(doc);
}

void AuditOpObserver::onDelete(OperationContext* opCtx,
                               const NamespaceString& nss,
                               const UUID& uuid,
                               StmtId stmtId,
                               const OplogDeleteEntryArgs& args) {
    if (deletingAuditConfigDecoration(opCtx)) {
        clearAuditConfig(opCtx->getClient());
    }
}

void AuditOpObserver::onDropDatabase(OperationContext* opCtx, const DatabaseName& dbName) {
    if (!isAuditingConfigured() || (dbName.db() != kConfigDB)) {
        return;
    }

    // Entire config DB deleted, reset to default state.
    clearAuditConfig(opCtx->getClient());
}

repl::OpTime AuditOpObserver::onDropCollection(OperationContext* opCtx,
                                               const NamespaceString& collectionName,
                                               const UUID& uuid,
                                               std::uint64_t numRecords,
                                               CollectionDropType dropType) {
    if (!isAuditingConfigured() || !isConfigNamespace(collectionName)) {
        return {};
    }

    // Entire collection deleted, reset to default state.
    clearAuditConfig(opCtx->getClient());

    return {};
}

void AuditOpObserver::postRenameCollection(OperationContext* opCtx,
                                           const NamespaceString& fromCollection,
                                           const NamespaceString& toCollection,
                                           const UUID& uuid,
                                           const boost::optional<UUID>& dropTargetUUID,
                                           bool stayTemp) {
    if (isAuditingConfigured() && isConfigNamespace(fromCollection)) {
        // Same as collection dropped from a config point of view.
        clearAuditConfig(opCtx->getClient());
    }

    if (isConfigNamespace(toCollection)) {
        // Check for an audit doc in the new version of the settings collection.
        updateAuditConfigFromDisk(opCtx);
    }
}

void AuditOpObserver::onImportCollection(OperationContext* opCtx,
                                         const UUID& importUUID,
                                         const NamespaceString& nss,
                                         long long numRecords,
                                         long long dataSize,
                                         const BSONObj& catalogEntry,
                                         const BSONObj& storageMetadata,
                                         bool isDryRun) {
    if (!isDryRun && (numRecords > 0) && isConfigNamespace(nss)) {
        // Check for an audit doc in the newly imported settings collection.
        updateAuditConfigFromDisk(opCtx);
    }
}

void AuditOpObserver::_onReplicationRollback(OperationContext* opCtx,
                                             const RollbackObserverInfo& rbInfo) {
    if (rbInfo.rollbackNamespaces.count(kSettingsNS)) {
        // Some kind of rollback happend in the settings collection.
        // Just reload from disk to be safe.
        updateAuditConfigFromDisk(opCtx);
    }
}

namespace {
MONGO_INITIALIZER_WITH_PREREQUISITES(AuditOpObserver, ("InitializeGlobalAuditManager"))
(InitializerContext*) {
    if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        // Only config servers and non-sharded configurations need run op observers.
        return;
    }

    auto* am = getGlobalAuditManager();
    if (!am->isEnabled() || !am->getRuntimeConfiguration()) {
        // Runtime audit configuration not enabled.
        return;
    }

    // Invoked prior to storage initialization.
    opObserverRegistrar = [](OpObserverRegistry* registry) {
        registry->addObserver(std::make_unique<AuditOpObserver>());
    };

    // Invoked after storage is initialized.
    initializeManager = [](OperationContext* opCtx) {
        if (isAuditingConfigured()) {
            // We got a configuration during opLog application, just use it.
            return;
        }

        AuditOpObserver::updateAuditConfigFromDisk(opCtx);
    };
}
}  // namespace

}  // namespace audit
}  // namespace mongo
