/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

 
#include "mongo_gssapi.h"

#include "mongo/base/status.h"

namespace mongo {
namespace gssapi {

    Status canonicalizeUserName(const StringData& name, std::string* canonicalName) {
        *canonicalName = name.toString();
        return Status::OK();
    }
    
    Status canonicalizeServerName(const StringData& name, std::string* canonicalName) {
        return Status(ErrorCodes::InternalError, "do not call canonicalizeServerName");
    }
    
    Status tryAcquireServerCredential(const StringData& principalName) {
        return Status::OK();
    }

}  // namespace gssapi
}  // namespace mongo
