/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

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
        WiredTigerCustomizationHooks* hooks =
            WiredTigerCustomizationHooks::get(opCtx->getServiceContext());
        if (!hooks->enabled()) {
            result.append("encryptionEnabled", false);
        } else {
            EncryptionKeyManager* mgr = EncryptionKeyManager::get(opCtx->getServiceContext());
            result.append("encryptionEnabled", true);
            result.append("encryptionKeyId", mgr->getMasterKeyId());
            result.append("encryptionCipherMode", mgr->getCipherMode());
        }
        return result.obj();
    }
} encryptionServerStatusSection;
}  // namespace mongo
