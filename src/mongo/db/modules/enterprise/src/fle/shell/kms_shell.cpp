/**
 *    Copyright (C) 2019 10gen Inc.
 */

#include "mongo/base/init.h"
#include "mongo/scripting/engine.h"
#include "mongo/shell/shell_utils.h"

namespace mongo {

namespace JSFiles {
extern const JSFile keyvault;
}

namespace {

void callback_fn(Scope& scope) {
    scope.execSetup(JSFiles::keyvault);
}

MONGO_INITIALIZER(setKeyvaultCallback)(InitializerContext*) {
    shell_utils::setEnterpriseShellCallback(mongo::callback_fn);
    return Status::OK();
}

}  // namespace
}  // namespace mongo
