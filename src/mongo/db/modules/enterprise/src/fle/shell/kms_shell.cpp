/**
 *    Copyright (C) 2019 10gen Inc.
 */

#include "mongo/base/init.h"
#include "mongo/scripting/engine.h"
#include "mongo/shell/shell_utils.h"

namespace mongo {

namespace JSFiles {
extern const JSFile keystore;
}

namespace {

void callback_fn(Scope& scope) {
    scope.execSetup(JSFiles::keystore);
}

MONGO_INITIALIZER(setKeystoreCallback)(InitializerContext*) {
    shell_utils::setEnterpriseShellCallback(mongo::callback_fn);
    return Status::OK();
}

}  // namespace
}  // namespace mongo