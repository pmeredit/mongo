/*
 *    Copyright (C) 2016 MongoDB Inc.
 */


#include "mongo/platform/basic.h"

#include "log_redact_options.h"

#include "encryptdb/log_redact_options_gen.h"
#include "mongo/base/status.h"
#include "mongo/logv2/log_util.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kControl


namespace mongo {

void RedactClientLogDataSetting::append(OperationContext* opCtx,
                                        BSONObjBuilder* b,
                                        StringData name,
                                        const boost::optional<TenantId>&) {
    *b << name << logv2::shouldRedactLogs();
}

Status RedactClientLogDataSetting::set(OperationContext* opCtx,
                                       const BSONElement& newValueElement,
                                       const boost::optional<TenantId>&) {
    bool newValue;
    if (!newValueElement.coerce(&newValue)) {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for redactClientLogData: " << newValueElement};
    }

    logv2::setShouldRedactLogs(newValue);
    return Status::OK();
}

Status RedactClientLogDataSetting::setFromString(OperationContext* opCtx,
                                                 StringData str,
                                                 const boost::optional<TenantId>&) {
    if (str == "true" || str == "1") {
        logv2::setShouldRedactLogs(true);
    } else if (str == "false" || str == "0") {
        logv2::setShouldRedactLogs(false);
    } else {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for redactClientLogData: " << str};
    }

    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(LogRedactOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("security.redactClientLogData")) {
        logv2::setShouldRedactLogs(params["security.redactClientLogData"].as<bool>());
    }
}

}  // namespace mongo
