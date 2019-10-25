/**
 * Copyright (C) 2016 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "encryption_key_manager.h"

#include "mongo/db/commands/server_status.h"
#include "mongo/db/operation_context.h"

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
    EncryptionServerStatusSection() : ServerStatusSection("encryptionAtRest") {}
    bool includeByDefault() const {
        return true;
    }

    BSONObj generateSection(OperationContext* opCtx, const BSONElement& configElement) const {
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
} encryptionServerStatusSection;
}  // namespace mongo
