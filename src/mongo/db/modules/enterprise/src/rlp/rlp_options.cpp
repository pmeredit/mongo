/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "rlp_options.h"

#include "mongo/base/status.h"
#include "mongo/db/server_options.h"
#include "mongo/logger/log_severity.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace fts {

    namespace moe = optionenvironment;

    RLPGlobalParams rlpGlobalParams;

    namespace {
        const char kBtRootOptionLong[] = "basisTech.RootDirectory";
        const char kBtRootOptionShort[] = "basisTechRootDirectory";

        Status addRLPOptions(moe::OptionSection* options) {

            moe::OptionSection rlpOptions("Rosette Linguistics Platform Options");

            rlpOptions.addOptionChaining(kBtRootOptionLong, kBtRootOptionShort,
                moe::String, "Root directory of a Basis Technology installation, i.e. BT_ROOT");

            Status ret = options->addSection(rlpOptions);

            if (!ret.isOK()) {
                error() << "Failed to add btRoot option section: " << ret.toString();
                return ret;
            }

            return Status::OK();
        }

        Status storeRLPOptions(const moe::Environment& params) {

            if (params.count(kBtRootOptionLong)) {
                rlpGlobalParams.btRoot = params[kBtRootOptionLong].as<std::string>();
            }

            return Status::OK();
        }
    }

    MONGO_MODULE_STARTUP_OPTIONS_REGISTER(RLPOptions)(InitializerContext* context) {
        return addRLPOptions(&moe::startupOptions);
    }

    MONGO_STARTUP_OPTIONS_STORE(RLPOptions)(InitializerContext* context) {
        return storeRLPOptions(moe::startupOptionsParsed);
    }

} // namespace fts
} // namespace mongo
