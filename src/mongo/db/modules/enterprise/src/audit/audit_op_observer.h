/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include <boost/optional.hpp>
#include <vector>

#include "audit/audit_config_gen.h"
#include "mongo/db/client.h"
#include "mongo/db/op_observer/op_observer_noop.h"
#include "mongo/db/op_observer/op_observer_registry.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/replica_set_aware_service.h"
#include "mongo/db/service_context.h"

namespace mongo {
namespace audit {

/**
 * Update in-memory audit configuration state on insert/update/remove.
 */
class AuditOpObserver final : public OpObserverNoop {
private:
    static void updateAuditConfig(Client* client, const AuditConfigDocument& config);
    static void clearAuditConfig(Client* client);

public:
    static void updateAuditConfigFromDisk(OperationContext* opCtx);

    void onInserts(OperationContext* opCtx,
                   const CollectionPtr& coll,
                   std::vector<InsertStatement>::const_iterator first,
                   std::vector<InsertStatement>::const_iterator last,
                   std::vector<bool> fromMigrate,
                   bool defaultFromMigrate,
                   InsertsOpStateAccumulator* opAccumulator = nullptr) final;
    void onUpdate(OperationContext* opCtx,
                  const OplogUpdateEntryArgs& args,
                  OpStateAccumulator* opAccumulator = nullptr) final;
    void aboutToDelete(OperationContext* opCtx,
                       const CollectionPtr& coll,
                       const BSONObj& doc,
                       OplogDeleteEntryArgs* args,
                       OpStateAccumulator* opAccumulator = nullptr) final;
    void onDelete(OperationContext* opCtx,
                  const CollectionPtr& coll,
                  StmtId stmtId,
                  const OplogDeleteEntryArgs& args,
                  OpStateAccumulator* opAccumulator = nullptr) final;
    void onDropDatabase(OperationContext* opCtx, const DatabaseName& dbName) final;
    repl::OpTime onDropCollection(OperationContext* opCtx,
                                  const NamespaceString& collectionName,
                                  const UUID& uuid,
                                  std::uint64_t numRecords,
                                  CollectionDropType dropType,
                                  bool markFromMigrate) final;
    void postRenameCollection(OperationContext* opCtx,
                              const NamespaceString& fromCollection,
                              const NamespaceString& toCollection,
                              const UUID& uuid,
                              const boost::optional<UUID>& dropTargetUUID,
                              bool stayTemp) final;
    void onImportCollection(OperationContext* opCtx,
                            const UUID& importUUID,
                            const NamespaceString& nss,
                            long long numRecords,
                            long long dataSize,
                            const BSONObj& catalogEntry,
                            const BSONObj& storageMetadata,
                            bool isDryRun) final;

    void onReplicationRollback(OperationContext* opCtx, const RollbackObserverInfo& rbInfo) final;
};

class AuditInitializer : public ReplicaSetAwareService<AuditInitializer> {
    AuditInitializer(const AuditInitializer&) = delete;
    AuditInitializer& operator=(const AuditInitializer&) = delete;

public:
    AuditInitializer() = default;
    ~AuditInitializer() = default;

    static AuditInitializer* get(ServiceContext* serviceContext);

    static void initialize(OperationContext* opCtx);

    // Virtual methods coming from the ReplicaSetAwareService
    void onStartup(OperationContext* opCtx) override final {}

    void onSetCurrentConfig(OperationContext* opCtx) override final {}
    /**
     * Called after startup recovery or initial sync is complete.
     */
    void onInitialDataAvailable(OperationContext* opCtx,
                                bool isMajorityDataAvailable) override final;
    void onBecomeArbiter() override final {}
    void onShutdown() override final {}
    void onStepUpBegin(OperationContext* opCtx, long long term) override final {}
    void onStepUpComplete(OperationContext* opCtx, long long term) override final {}
    void onStepDown() override final {}
    std::string getServiceName() const override final {
        return "AuditInitializer";
    }
};

}  // namespace audit
}  // namespace mongo
