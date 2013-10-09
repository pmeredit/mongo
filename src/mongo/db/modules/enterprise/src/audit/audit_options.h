/*
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"

namespace mongo {

    namespace optionenvironment {
        class OptionSection;
        class Environment;
    } // namespace optionenvironment

    namespace moe = optionenvironment;

namespace audit {

    struct AuditGlobalParams {
        bool enabled;
        BSONObj auditfilter;

        enum AuditFormat {
            AuditFormatText = 0,
            AuditFormatBson = 1
        };

        AuditFormat auditFormat;

        AuditGlobalParams() : enabled(false), auditFormat(AuditFormatText) {}
    };

    extern AuditGlobalParams auditGlobalParams;

    Status addAuditOptions(moe::OptionSection* options);

    Status storeAuditOptions(const moe::Environment& params,
                             const std::vector<std::string>& args);
} // namespace audit
} // namespace mongo
