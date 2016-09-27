/*
 *    Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/platform/basic.h"

#include "log_redact_options.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

namespace {
Status storeLogRedactOptions(const moe::Environment& params) {
    if (params.count("security.redactClientLogData")) {
        logger::globalLogDomain()->setShouldRedactLogs(
            params["security.redactClientLogData"].as<bool>());
    }

    return Status::OK();
}
}  // namespace

class RedactClientLogDataSetting : public ServerParameter {
public:
    RedactClientLogDataSetting()
        : ServerParameter(ServerParameterSet::getGlobal(), "redactClientLogData", false, true) {}

    virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name) {
        b << name << logger::globalLogDomain()->shouldRedactLogs();
    }

    virtual Status set(const BSONElement& newValueElement) {
        bool newValue;
        if (!newValueElement.coerce(&newValue))
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Invalid value for redactClientLogData: "
                                                    << newValueElement);
        logger::globalLogDomain()->setShouldRedactLogs(newValue);
        return Status::OK();
    }

    virtual Status setFromString(const std::string& str) {
        if (str == "true" || str == "1") {
            logger::globalLogDomain()->setShouldRedactLogs(true);
        } else if (str == "false" || str == "0") {
            logger::globalLogDomain()->setShouldRedactLogs(false);
        } else {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Invalid value for redactClientLogData: "
                                                    << str);
        }
        return Status::OK();
    }
} redactClientLogDataSetting;

MONGO_STARTUP_OPTIONS_STORE(LogRedactOptions)(InitializerContext* context) {
    return storeLogRedactOptions(moe::startupOptionsParsed);
}

}  // namespace mongo
