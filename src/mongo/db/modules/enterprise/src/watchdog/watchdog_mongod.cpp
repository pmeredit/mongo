/**
 * Copyright (C) 2017 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "watchdog_mongod.h"

#include "../audit/audit_options.h"
#include "mongo/base/init.h"
#include "mongo/config.h"
#include "mongo/db/client.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_noop.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/log.h"
#include "mongo/util/tick_source_mock.h"
#include "watchdog.h"

namespace mongo {

// Run the watchdog checks at a fixed interval regardless of user choice for monitoring period.
constexpr Seconds watchdogCheckPeriod = Seconds{10};

namespace {

const auto getWatchdogMonitor =
    ServiceContext::declareDecoration<std::unique_ptr<WatchdogMonitor>>();

// A boolean variable to track whether the watchdog was enabled at startup.
// Defaults to true because set parameters are handled before we start the watchdog if needed.
bool watchdogEnabled{true};

WatchdogMonitor* getGlobalWatchdogMonitor() {
    if (!hasGlobalServiceContext()) {
        return nullptr;
    }

    return getWatchdogMonitor(getGlobalServiceContext()).get();
}

AtomicInt32 localPeriodSeconds(watchdogPeriodSecondsDefault);

class ExportedWatchdogPeriodParameter
    : public ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime> {
public:
    ExportedWatchdogPeriodParameter()
        : ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime>(
              ServerParameterSet::getGlobal(), "watchdogPeriodSeconds", &localPeriodSeconds) {}

    virtual Status validate(const std::int32_t& potentialNewValue) {
        if (potentialNewValue < 60 && potentialNewValue != -1) {
            return Status(ErrorCodes::BadValue,
                          "watchdogPeriodSeconds must be greater than or equal to 60s");
        }

        // If the watchdog was not enabled at startup, disallow changes the period.
        if (!watchdogEnabled) {
            return Status(
                ErrorCodes::BadValue,
                "watchdogPeriodSeconds cannot be changed at runtime if it was not set at startup");
        }

        auto monitor = getGlobalWatchdogMonitor();
        if (monitor) {
            monitor->setPeriod(Seconds(potentialNewValue));
        }

        return Status::OK();
    }

} exportedWatchdogPeriodParameter;

}  // namespace

/**
 * Server status section for the Watchdog.
 *
 * Sample format:
 *
 * watchdog: {
 *       generation: int,
 * }
 */
class WatchdogServerStatusSection : public ServerStatusSection {
public:
    WatchdogServerStatusSection() : ServerStatusSection("watchdog") {}
    bool includeByDefault() const {
        // Only include this by default if the watchdog is on
        return watchdogEnabled;
    }

    BSONObj generateSection(OperationContext* opCtx, const BSONElement& configElement) const {
        BSONObjBuilder result;

        WatchdogMonitor* watchdog = getWatchdogMonitor(opCtx->getServiceContext()).get();

        result.append("checkGeneration", watchdog->getCheckGeneration());
        result.append("monitorGeneration", watchdog->getMonitorGeneration());
        result.append("monitorPeriod", localPeriodSeconds.load());

        return result.obj();
    }
} watchdogServerStatusSection;

void startWatchdog() {
    // Check three paths if set
    // 1. storage directory - optional for inmemory?
    // 2. log path - optional
    // 3. audit path - optional

    Seconds period{localPeriodSeconds.load()};
    if (period < Seconds::zero()) {
        // Skip starting the watchdog if the user has not asked for it.
        watchdogEnabled = false;
        return;
    }

    watchdogEnabled = true;

    std::vector<std::unique_ptr<WatchdogCheck>> checks;

    auto dataCheck =
        stdx::make_unique<DirectoryCheck>(boost::filesystem::path(storageGlobalParams.dbpath));

    checks.push_back(std::move(dataCheck));

    // Add a check for the journal if it is not disabled
    if (storageGlobalParams.dur) {
        auto journalDirectory = boost::filesystem::path(storageGlobalParams.dbpath);
        journalDirectory /= "journal";

        auto journalCheck = stdx::make_unique<DirectoryCheck>(journalDirectory);

        checks.push_back(std::move(journalCheck));
    }

    // If the user specified a log path, also monitor that directory.
    // This may be redudant with the dbpath check but there is not easy way to confirm they are
    // duplicate.
    if (!serverGlobalParams.logpath.empty()) {
        boost::filesystem::path logFile(serverGlobalParams.logpath);
        auto logPath = logFile.parent_path();

        auto logCheck = stdx::make_unique<DirectoryCheck>(logPath);
        checks.push_back(std::move(logCheck));
    }

    // If the user specified an audit path, also monitor that directory.
    // This may be redudant with the dbpath check but there is not easy way to confirm they are
    // duplicate.
    if (!audit::auditGlobalParams.auditPath.empty()) {
        boost::filesystem::path auditFile(audit::auditGlobalParams.auditPath);
        auto auditPath = auditFile.parent_path();

        auto auditCheck = stdx::make_unique<DirectoryCheck>(auditPath);
        checks.push_back(std::move(auditCheck));
    }


    auto monitor = stdx::make_unique<WatchdogMonitor>(
        std::move(checks), watchdogCheckPeriod, period, watchdogTerminate);

    // Install the new WatchdogMonitor
    auto& staticMonitor = getWatchdogMonitor(getGlobalServiceContext());

    staticMonitor = std::move(monitor);

    staticMonitor->start();
}

}  // namespace mongo
