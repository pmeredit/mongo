/*
 *    Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/platform/basic.h"

#include "log_redact_options.h"

#include "encryptdb/log_redact_options_gen.h"
#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

void RedactClientLogDataSetting::append(OperationContext* opCtx,
                                        BSONObjBuilder& b,
                                        const std::string& name) {
    b << name << logger::globalLogDomain()->shouldRedactLogs();
}

Status RedactClientLogDataSetting::set(const BSONElement& newValueElement) {
    bool newValue;
    if (!newValueElement.coerce(&newValue)) {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for redactClientLogData: " << newValueElement};
    }

    logger::globalLogDomain()->setShouldRedactLogs(newValue);
    return Status::OK();
}

Status RedactClientLogDataSetting::setFromString(const std::string& str) {
    if (str == "true" || str == "1") {
        logger::globalLogDomain()->setShouldRedactLogs(true);
    } else if (str == "false" || str == "0") {
        logger::globalLogDomain()->setShouldRedactLogs(false);
    } else {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for redactClientLogData: " << str};
    }

    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(LogRedactOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("security.redactClientLogData")) {
        logger::globalLogDomain()->setShouldRedactLogs(
            params["security.redactClientLogData"].as<bool>());
    }

    return Status::OK();
}

}  // namespace mongo
