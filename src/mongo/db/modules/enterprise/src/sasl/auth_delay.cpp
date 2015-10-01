/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace {

const Milliseconds kMaxAuthFailedDelay(5000);

const std::string authFailedDelayServerParameter = "authFailedDelayMs";
class ExportedAuthFailedDelayParameter
    : public ExportedServerParameter<int, ServerParameterType::kStartupAndRuntime> {
public:
    ExportedAuthFailedDelayParameter()
        : ExportedServerParameter<int, ServerParameterType::kStartupAndRuntime>(
              ServerParameterSet::getGlobal(),
              authFailedDelayServerParameter,
              &saslGlobalParams.authFailedDelay) {}

    virtual Status validate(const int& newValue) {
        if (newValue < 0 || newValue > kMaxAuthFailedDelay.count()) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "Invalid value for authFailedDelayMs: " << newValue);
        }
        return Status::OK();
    }
} authFailedDelayParam;

}  // namespace
}  // namespace mongo
