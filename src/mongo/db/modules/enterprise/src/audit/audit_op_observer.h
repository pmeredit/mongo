/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <boost/optional.hpp>
#include <vector>

#include "audit/audit_config_gen.h"
#include "mongo/db/client.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/op_observer_registry.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"

namespace mongo {
namespace audit {

/**
 * Update in-memory audit configuration state on insert/update/remove.
 */
class AuditOpObserver final : public OpObserver {
private:
    static void updateAuditConfig(Client* client, const AuditConfigDocument& config);
    static void clearAuditConfig(Client* client);

public:
    static void updateAuditConfigFromDisk(OperationContext* opCtx);

    void onInserts(OperationContext* opCtx,
                   const NamespaceString& nss,
                   OptionalCollectionUUID uuid,
                   std::vector<InsertStatement>::const_iterator first,
                   std::vector<InsertStatement>::const_iterator last,
                   bool fromMigrate) final;
    void onUpdate(OperationContext* opCtx, const OplogUpdateEntryArgs& args) final;
    void aboutToDelete(OperationContext* opCtx,
                       const NamespaceString& nss,
                       const BSONObj& doc) final;
    void onDelete(OperationContext* opCtx,
                  const NamespaceString& nss,
                  OptionalCollectionUUID uuid,
                  StmtId stmtId,
                  const OplogDeleteEntryArgs& args) final;
    void onDropDatabase(OperationContext* opCtx, const std::string& dbName) final;
    repl::OpTime onDropCollection(OperationContext* opCtx,
                                  const NamespaceString& collectionName,
                                  OptionalCollectionUUID uuid,
                                  std::uint64_t numRecords,
                                  CollectionDropType dropType) final;
    void postRenameCollection(OperationContext* opCtx,
                              const NamespaceString& fromCollection,
                              const NamespaceString& toCollection,
                              OptionalCollectionUUID uuid,
                              OptionalCollectionUUID dropTargetUUID,
                              bool stayTemp) final;
    void onImportCollection(OperationContext* opCtx,
                            const UUID& importUUID,
                            const NamespaceString& nss,
                            long long numRecords,
                            long long dataSize,
                            const BSONObj& catalogEntry,
                            const BSONObj& storageMetadata,
                            bool isDryRun) final;
    void onReplicationRollback(OperationContext* opCtx, const RollbackObserverInfo& rbInfo);

    // Remainder of operations are ignorable.

    void onCreateIndex(OperationContext* opCtx,
                       const NamespaceString& nss,
                       CollectionUUID uuid,
                       BSONObj indexDoc,
                       bool fromMigrate) final {}

    void onStartIndexBuild(OperationContext* opCtx,
                           const NamespaceString& nss,
                           CollectionUUID collUUID,
                           const UUID& indexBuildUUID,
                           const std::vector<BSONObj>& indexes,
                           bool fromMigrate) final {}

    void onStartIndexBuildSinglePhase(OperationContext* opCtx, const NamespaceString& nss) final {}

    void onCommitIndexBuild(OperationContext* opCtx,
                            const NamespaceString& nss,
                            CollectionUUID collUUID,
                            const UUID& indexBuildUUID,
                            const std::vector<BSONObj>& indexes,
                            bool fromMigrate) final {}

    void onAbortIndexBuild(OperationContext* opCtx,
                           const NamespaceString& nss,
                           CollectionUUID collUUID,
                           const UUID& indexBuildUUID,
                           const std::vector<BSONObj>& indexes,
                           const Status& cause,
                           bool fromMigrate) final {}

    void onInternalOpMessage(OperationContext* opCtx,
                             const NamespaceString& nss,
                             const boost::optional<UUID> uuid,
                             const BSONObj& msgObj,
                             const boost::optional<BSONObj> o2MsgObj,
                             const boost::optional<repl::OpTime> preImageOpTime,
                             const boost::optional<repl::OpTime> postImageOpTime,
                             const boost::optional<repl::OpTime> prevWriteOpTimeInTransaction,
                             const boost::optional<OplogSlot> slot) final {}

    void onCreateCollection(OperationContext* opCtx,
                            const CollectionPtr& coll,
                            const NamespaceString& collectionName,
                            const CollectionOptions& options,
                            const BSONObj& idIndex,
                            const OplogSlot& createOpTime) final {}

    void onCollMod(OperationContext* opCtx,
                   const NamespaceString& nss,
                   const UUID& uuid,
                   const BSONObj& collModCmd,
                   const CollectionOptions& oldCollOptions,
                   boost::optional<IndexCollModInfo> indexInfo) final {}

    void onDropIndex(OperationContext* opCtx,
                     const NamespaceString& nss,
                     OptionalCollectionUUID uuid,
                     const std::string& indexName,
                     const BSONObj& indexInfo) final {}

    repl::OpTime preRenameCollection(OperationContext* opCtx,
                                     const NamespaceString& fromCollection,
                                     const NamespaceString& toCollection,
                                     OptionalCollectionUUID uuid,
                                     OptionalCollectionUUID dropTargetUUID,
                                     std::uint64_t numRecords,
                                     bool stayTemp) final {
        return repl::OpTime();
    }

    void onRenameCollection(OperationContext* opCtx,
                            const NamespaceString& fromCollection,
                            const NamespaceString& toCollection,
                            OptionalCollectionUUID uuid,
                            OptionalCollectionUUID dropTargetUUID,
                            std::uint64_t numRecords,
                            bool stayTemp) final {}

    void onApplyOps(OperationContext* opCtx,
                    const std::string& dbName,
                    const BSONObj& applyOpCmd) final {}

    void onEmptyCapped(OperationContext* opCtx,
                       const NamespaceString& collectionName,
                       OptionalCollectionUUID uuid) final {}

    void onUnpreparedTransactionCommit(OperationContext* opCtx,
                                       std::vector<repl::ReplOperation>* statements,
                                       size_t numberOfPreImagesToWrite) final {}

    void onPreparedTransactionCommit(
        OperationContext* opCtx,
        OplogSlot commitOplogEntryOpTime,
        Timestamp commitTimestamp,
        const std::vector<repl::ReplOperation>& statements) noexcept final {}

    void onTransactionPrepare(OperationContext* opCtx,
                              const std::vector<OplogSlot>& reservedSlots,
                              std::vector<repl::ReplOperation>* statements,
                              size_t numberOfPreImagesToWrite) final {}

    void onTransactionAbort(OperationContext* opCtx,
                            boost::optional<OplogSlot> abortOplogEntryOpTime) final {}

    void onMajorityCommitPointUpdate(ServiceContext* service,
                                     const repl::OpTime& newCommitPoint) final {}
};

}  // namespace audit
}  // namespace mongo
