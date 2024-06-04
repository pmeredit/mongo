/**
 *    Copyright (C) 2021 MongoDB Inc.
 */


#include "audit/audit_op_observer.h"

#include "audit/audit_manager.h"
#include "audit/audit_mongod.h"
#include "audit/audit_options_gen.h"
#include "mongo/db/audit.h"
#include "mongo/db/commands/set_cluster_parameter_invocation.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/s/config/configsvr_coordinator.h"
#include "mongo/db/s/config/configsvr_coordinator_service.h"
#include "mongo/db/s/config/set_cluster_parameter_coordinator_document_gen.h"
#include "mongo/db/shard_role.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {
namespace audit {

namespace {
const auto deletingAuditConfigDecoration = OplogDeleteEntryArgs::declareDecoration<bool>();

bool isAuditConfigurationSet() {
    return getGlobalAuditManager()->isConfigurationSet();
}

constexpr auto kIDField = "_id"_sd;
bool isAuditDoc(BSONObj doc) {
    auto idElem = doc[kIDField];
    return (idElem.type() == String) && (idElem.valueStringData() == kAuditDocID);
}

bool isConfigNamespace(const NamespaceString& nss) {
    return nss == NamespaceString::kConfigSettingsNamespace;
}

boost::optional<BSONObj> getAuditConfigurationFromDisk(OperationContext* opCtx) {
    AutoGetCollectionForReadCommandMaybeLockFree ctx(opCtx,
                                                     NamespaceString::kConfigSettingsNamespace);
    BSONObj res;

    if (Helpers::findOne(opCtx, ctx.getCollection(), BSON("_id" << kAuditDocID), res)) {
        return res.getOwned();
    }

    return boost::none;
}

// If the FCV is either uninitialized (we aren't sure what the real FCV is) or it is initialized but
// the audit config cluster parameter is enabled, we want to skip all OpObserver methods.
inline bool isFCVUninitializedOrTooHigh() {
    const auto fcvSnapshot = serverGlobalParams.featureCompatibility.acquireFCVSnapshot();
    return (!fcvSnapshot.isVersionInitialized()) ||
        feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(fcvSnapshot);
}

const auto _auditInitializer = ServiceContext::declareDecoration<AuditInitializer>();
const ReplicaSetAwareServiceRegistry::Registerer<AuditInitializer> _registerer(
    "AuditInitializerRegistry");
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
    AuditConfigDocument doc{{}, false};
    doc.setGeneration(OID());
    updateAuditConfig(client, doc);
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
                                const std::vector<RecordId>& recordIds,
                                std::vector<bool> fromMigrate,
                                bool defaultFromMigrate,
                                OpStateAccumulator* opAccumulator) {
    // If FCV is uninitialized (meaning we are still in startup) or audit config is a cluster
    // parameter, skip the op observer. If when FCV is initialized, the feature flag is inactive, we
    // will update the audit config from disk at that point.
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
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

void AuditOpObserver::onUpdate(OperationContext* opCtx,
                               const OplogUpdateEntryArgs& args,
                               OpStateAccumulator* opAccumulator) {
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
    auto updatedDoc = args.updateArgs->updatedDoc;
    if (!isConfigNamespace(args.coll->ns()) || !isAuditDoc(updatedDoc) ||
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
                                    const CollectionPtr& coll,
                                    const BSONObj& doc,
                                    OplogDeleteEntryArgs* args,
                                    OpStateAccumulator* opAccumulator) {
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
    deletingAuditConfigDecoration(args) =
        isAuditConfigurationSet() && isConfigNamespace(coll->ns()) && isAuditDoc(doc);
}

void AuditOpObserver::onDelete(OperationContext* opCtx,
                               const CollectionPtr& coll,
                               StmtId stmtId,
                               const BSONObj& doc,
                               const OplogDeleteEntryArgs& args,
                               OpStateAccumulator* opAccumulator) {
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
    if (deletingAuditConfigDecoration(args)) {
        clearAuditConfig(opCtx->getClient());
    }
}

void AuditOpObserver::onDropDatabase(OperationContext* opCtx,
                                     const DatabaseName& dbName,
                                     bool markFromMigrate) {
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
    if (!isAuditConfigurationSet() || !dbName.isConfigDB()) {
        return;
    }

    // Entire config DB deleted, reset to default state.
    clearAuditConfig(opCtx->getClient());
}

repl::OpTime AuditOpObserver::onDropCollection(OperationContext* opCtx,
                                               const NamespaceString& collectionName,
                                               const UUID& uuid,
                                               std::uint64_t numRecords,
                                               CollectionDropType dropType,
                                               bool markFromMigrate) {
    if (isFCVUninitializedOrTooHigh()) {
        return {};
    }
    if (!isAuditConfigurationSet() || !isConfigNamespace(collectionName)) {
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
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
    if (isAuditConfigurationSet() && isConfigNamespace(fromCollection)) {
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
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
    if (!isDryRun && (numRecords > 0) && isConfigNamespace(nss)) {
        // Check for an audit doc in the newly imported settings collection.
        updateAuditConfigFromDisk(opCtx);
    }
}

void AuditOpObserver::onReplicationRollback(OperationContext* opCtx,
                                            const RollbackObserverInfo& rbInfo) {
    if (isFCVUninitializedOrTooHigh()) {
        return;
    }
    if (rbInfo.rollbackNamespaces.count(NamespaceString::kConfigSettingsNamespace)) {
        // Some kind of rollback happend in the settings collection.
        // Just reload from disk to be safe.
        updateAuditConfigFromDisk(opCtx);
    }
}

void AuditInitializer::initialize(OperationContext* opCtx) {
    // Now that FCV is set up, we can safely check whether the feature flag is enabled. If it is
    // not, the source of the audit config is the old audit settings collection, so we read from
    // that collection. The cluster parameter initializer deals with the case when the feature flag
    // is enabled.
    if (!feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
            serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
        AuditOpObserver::updateAuditConfigFromDisk(opCtx);
    }
}

void AuditInitializer::onInitialDataAvailable(OperationContext* opCtx,
                                              bool isMajorityDataAvailable) {
    if (serverGlobalParams.clusterRole.hasExclusively(ClusterRole::ShardServer)) {
        // Only config servers and non-sharded configurations need to initialize here.
        return;
    }

    auto* am = getGlobalAuditManager();
    if (!am->isEnabled() || !am->getRuntimeConfiguration()) {
        // Runtime audit configuration not enabled.
        return;
    }

    // If this is not true, we almost certainly have no non-local databases, so there is no audit
    // config to update from on disk. This case happens when the node is an arbiter.
    if (serverGlobalParams.featureCompatibility.acquireFCVSnapshot().isVersionInitialized()) {
        initialize(opCtx);
    } else {
        // Ensure that our assumption about there being no non-local databases is correct.
        auto const storageEngine = opCtx->getServiceContext()->getStorageEngine();
        auto dbNames = storageEngine->listDatabases();
        bool nonLocalDatabases = std::any_of(
            dbNames.begin(), dbNames.end(), [](auto dbName) { return !dbName.isLocalDB(); });

        invariant(!nonLocalDatabases,
                  "Non-local databases existed when version was uninitialized in "
                  "onInitialDataAvailable!");
    }
}

AuditInitializer* AuditInitializer::get(ServiceContext* serviceContext) {
    return &_auditInitializer(serviceContext);
}

namespace {
MONGO_INITIALIZER_WITH_PREREQUISITES(AuditOpObserver, ("InitializeGlobalAuditManager"))
(InitializerContext*) {
    auto* am = getGlobalAuditManager();
    if (!am->isEnabled() || !am->getRuntimeConfiguration()) {
        // Runtime audit configuration not enabled.
        return;
    }

    if (!serverGlobalParams.clusterRole.hasExclusively(ClusterRole::ShardServer)) {
        // Only config servers and non-sharded configurations need run op observers.
        // Invoked prior to storage initialization.
        opObserverRegistrar = [](OpObserverRegistry* registry) {
            registry->addObserver(std::make_unique<AuditOpObserver>());
        };

        // Invoked after storage is initialized.
        initializeManager = [](OperationContext* opCtx) {
            if (!repl::ReplicationCoordinator::get(opCtx)->getSettings().isReplSet()) {
                // ReplicaSetAwareServices never start if repl is not enabled (ex. standalone), so
                // run the initialization manually here. Note that we don't need to delay
                // initialization here because if repl is disabled, we will have an FCV set here.
                AuditInitializer::initialize(opCtx);
            }
        };
    }

    removeOldConfig =
        [](OperationContext* opCtx) {
            invariant(feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot()));
            // Remove audit config entry from config.settings as we aren't using it anymore. This
            // happens unilaterally on all server types -- config, shard, non-sharded replset,
            // standalone.
            DBDirectClient client(opCtx);
            write_ops::DeleteCommandRequest deleteOp(NamespaceString::kConfigSettingsNamespace);
            deleteOp.setDeletes({[&] {
                write_ops::DeleteOpEntry entry;
                entry.setQ(BSON("_id"
                                << "audit"));
                entry.setMulti(true);
                return entry;
            }()});
            auto res = client.remove(deleteOp);
            write_ops::checkWriteErrors(res);
        };

    // Invoked upon FCV upgrade when the feature flag switches on.
    migrateOldToNew = [](OperationContext* opCtx, boost::optional<Timestamp> changeTimestamp) {
        invariant(serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer) ||
                  !serverGlobalParams.clusterRole.has(ClusterRole::ShardServer));
        // If we have an existing configuration in config.clusterParameters, we want to skip this
        // migration. This can occur if we are upgrading from a failed downgrade.
        {
            DBDirectClient client(opCtx);
            auto clusterParametersAuditConfig =
                client.findOne(NamespaceString::kClusterParametersNamespace,
                               BSON("_id"
                                    << "auditConfig"));
            if (!clusterParametersAuditConfig.isEmpty()) {
                LOGV2_DEBUG(7193010,
                            3,
                            "Existing audit config was present in config.clusterParameters during "
                            "upgrade; skipping migration from config.settings.");
                return;
            }
        }

        auto* am = getGlobalAuditManager();
        auto config = am->getAuditConfig();

        // Null out the generation and timestamp because we are generating a new timestamp
        config.setGeneration(boost::none);
        config.setClusterParameterTime(boost::none);

        // Write config as a cluster parameter. We skip this step on shard servers, and instead run
        // the cluster-wide setClusterParameter operation from the config server -- this will ensure
        // that the cluster time is synchronized across the cluster.
        if (serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer)) {
            // We have to manually run the SetClusterParameterCoordinator because at this point, the
            // auditConfig parameter is disabled due to the FCV being too low, so running
            // setClusterParameter will fail.
            SetClusterParameterCoordinatorDocument coordinatorDoc;
            ConfigsvrCoordinatorId cid(ConfigsvrCoordinatorTypeEnum::kSetClusterParameter);
            // Sub ID is empty string because the tenant is always boost::none for the audit config
            // cluster parameter
            cid.setSubId(""_sd);

            coordinatorDoc.setConfigsvrCoordinatorMetadata({cid});
            coordinatorDoc.setParameter(BSON("auditConfig" << config.toBSON()));
            coordinatorDoc.setTenantId(boost::none);

            const auto service = ConfigsvrCoordinatorService::getService(opCtx);
            const auto instance = service->getOrCreateService(opCtx, coordinatorDoc.toBSON());

            // Ensure we succeed before returning to setFCV
            instance->getCompletionFuture().get(opCtx);
        } else {
            // In this case we are on standalone replset primary or standalone server, run the
            // SetClusterParameterInvocation to set the parameter.
            std::unique_ptr<ServerParameterService> parameterService =
                std::make_unique<ClusterParameterService>();

            DBDirectClient client(opCtx);
            ClusterParameterDBClientService dbService(client);

            SetClusterParameterInvocation invocation{std::move(parameterService), dbService};

            SetClusterParameter setClusterParameter(BSON("auditConfig" << config.toBSON()));
            setClusterParameter.setDbName(DatabaseName::kAdmin);

            WriteConcernOptions writeConcern{WriteConcernOptions::kMajority,
                                             WriteConcernOptions::SyncMode::UNSET,
                                             WriteConcernOptions::kNoTimeout};

            // If the changeTimestamp is null, the invocation will generate a new cluster parameter
            // time to use. We skip validation because, again, the auditConfig parameter is
            // disabled.
            invocation.invoke(opCtx,
                              setClusterParameter,
                              changeTimestamp,
                              boost::none, /* previousTime */
                              writeConcern,
                              true /* skipValidation */);
        }
    };

    // Invoked upon FCV downgrade when featureFlagAuditConfigClusterParameter is being disabled.
    updateAuditConfigOnDowngrade = [](OperationContext* opCtx) {
        {
            // Write a new blank audit config to config.settings. This will, through the
            // AuditOpObserver, reset the in-memory audit config.
            AuditConfigDocument doc{{}, false};
            doc.set_id(kAuditDocID);
            doc.setGeneration(OID());
            auto configSettingsColl =
                acquireCollection(opCtx,
                                  CollectionAcquisitionRequest(
                                      NamespaceString(NamespaceString::kConfigSettingsNamespace),
                                      PlacementConcern{boost::none, ShardVersion::UNSHARDED()},
                                      repl::ReadConcernArgs::get(opCtx),
                                      AcquisitionPrerequisites::kWrite),
                                  MODE_IX);
            WriteUnitOfWork wuow(opCtx);
            Helpers::upsert(opCtx, configSettingsColl, doc.toBSON());
            wuow.commit();
        }
        AuditConfigDocument doc = getGlobalAuditManager()->getAuditConfig();
        invariant(doc.getFilter().isEmpty());
        invariant(!doc.getAuditAuthorizationSuccess());
        invariant(!doc.getClusterParameterTime());
        auto gen = doc.getGeneration();
        invariant(gen && !gen->isSet());
    };
}
}  // namespace
}  // namespace audit
}  // namespace mongo
