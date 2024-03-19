/**
 * Copyright (C) 2016 MongoDB, Inc.  All Rights Reserved.
 */


#include "mongo/platform/basic.h"

#include "encryption_key_manager.h"

#include "mongo/db/commands/server_status.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/operation_context.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kFTDC


namespace mongo {

/**
 * Server status section for the EncryptionKeyManager.
 *
 * Sample format:
 *
 * encryptionAtRest: {
 *       encryptionEnabled: bool,
 *       encryptionKeyId: int,
 *       encryptionCipherMode: string
 * }
 */
class EncryptionServerStatusSection : public ServerStatusSection {
public:
    using ServerStatusSection::ServerStatusSection;
    bool includeByDefault() const override {
        return true;
    }

    BSONObj generateSection(OperationContext* opCtx,
                            const BSONElement& configElement) const override {
        // This is unsafe to access when the storage engine is shutting down. As the global
        // exclusive lock is held during shutdown, we protect this with a global intent lock of an
        // immediate timeout to not block FTDC.
        Lock::GlobalLock lk(
            opCtx, LockMode::MODE_IS, Date_t::now(), Lock::InterruptBehavior::kLeaveUnlocked);
        if (!lk.isLocked()) {
            LOGV2_DEBUG(4822101, 2, "Failed to retrieve encryptionAtRest statistics");
            return BSONObj();
        }

        BSONObjBuilder result;
        EncryptionHooks* hooks =
            dynamic_cast<EncryptionKeyManager*>(EncryptionHooks::get(opCtx->getServiceContext()));
        if (!hooks || !hooks->enabled()) {
            result.append("encryptionEnabled", false);
        } else {
            EncryptionKeyManager* mgr = EncryptionKeyManager::get(opCtx->getServiceContext());
            result.append("encryptionEnabled", true);
            result.append("encryptionKeyId", mgr->getMasterKeyId().toString());
            result.append("encryptionCipherMode", mgr->getCipherMode());
        }
        return result.obj();
    }
};
auto& encryptionServerStatusSection =
    *ServerStatusSectionBuilder<EncryptionServerStatusSection>("encryptionAtRest").forShard();
}  // namespace mongo
