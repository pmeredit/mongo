/**
 * Copyright (C) 2019-present MongoDB, Inc.
 */

#include "fts/mongot_options.h"
#include "fts/mongot_options_gen.h"

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/text.h"

namespace mongo {

MongotParams globalMongotParams;

MongotParams::MongotParams() {
    host = kMongotHostDefault;
}

Status MongotParams::onSetHost(const std::string&) {
    globalMongotParams.enabled = true;
    return Status::OK();
}

Status MongotParams::onValidateHost(StringData str) {
    // Unset value is OK
    if (str.empty()) {
        return Status::OK();
    }

    // `mongotHost` must be able to parse into a HostAndPort
    if (auto status = HostAndPort::parse(str); !status.isOK()) {
        return status.getStatus().withContext("mongoHost must be of the form \"host:port\"");
    }

    return Status::OK();
}

}  // namespace mongo
