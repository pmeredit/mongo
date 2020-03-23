/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <memory>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/bson/json.h"
#include "mongo/db/client.h"
#include "mongo/db/initialize_server_global_state.h"
#include "mongo/db/log_process_details.h"
#include "mongo/db/repl/replication_coordinator_noop.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine_lock_file.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/wire_version.h"
#include "mongo/logv2/log.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/transport/transport_layer_manager.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/cmdline_utils/censor_cmdline.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/exit.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"
#include "mongo/util/version.h"

#include "cryptd_options.h"
#include "cryptd_service_entry_point.h"
#include "cryptd_watchdog.h"
#include "fle/cryptd/cryptd_options_gen.h"


namespace mongo {
namespace {

#ifdef _WIN32
const ntservice::NtServiceDefaultStrings defaultServiceStrings = {
    L"MongoCryptD", L"MongoDB FLE Crypto", L"MongoDB Field Level Encryption Daemon"};
#endif

void initWireSpec() {
    WireSpec& spec = WireSpec::instance();

    // For MongoCryptd, we set the minimum wire version to be 4.2
    spec.incomingInternalClient.minWireVersion = SHARDED_TRANSACTIONS;
    spec.incomingInternalClient.maxWireVersion = LATEST_WIRE_VERSION;

    spec.outgoing.minWireVersion = SHARDED_TRANSACTIONS;
    spec.outgoing.maxWireVersion = LATEST_WIRE_VERSION;

    spec.isInternalClient = true;
}

void createLockFile(ServiceContext* serviceContext) {
    auto& lockFile = StorageEngineLockFile::get(serviceContext);

    boost::filesystem::path orig_file(serverGlobalParams.pidFile);
    boost::filesystem::path file(boost::filesystem::absolute(orig_file));

    LOGV2(24225,
          "Using lock file: {file_generic_string}",
          "file_generic_string"_attr = file.generic_string());
    try {
        lockFile.emplace(file.parent_path().generic_string(), file.filename().generic_string());
    } catch (const std::exception& ex) {
        LOGV2_ERROR(24230,
                    "Unable to determine status of lock file in the data directory "
                    "{file_generic_string}: {ex_what}",
                    "file_generic_string"_attr = file.generic_string(),
                    "ex_what"_attr = ex.what());
        _exit(EXIT_FAILURE);
    }

    const auto openStatus = lockFile->open();
    if (!openStatus.isOK()) {
        LOGV2_ERROR(24231,
                    "Failed to open pid file, exiting: {openStatus}",
                    "openStatus"_attr = openStatus);
        _exit(EXIT_FAILURE);
    }

    CryptdPidFile pidFile;
    pidFile.setPort(serverGlobalParams.port);
    pidFile.setPid(ProcessId::getCurrent().asInt64());

    auto str = tojson(pidFile.toBSON(), JsonStringFormat::LegacyStrict);

    const auto writeStatus = lockFile->writeString(str);
    if (!writeStatus.isOK()) {
        LOGV2_ERROR(24232,
                    "Failed to write pid file, exiting: {writeStatus}",
                    "writeStatus"_attr = writeStatus);
        _exit(EXIT_FAILURE);
    }
}

void shutdownTask() {
    // This client initiation pattern is only to be used here, with plans to eliminate this pattern
    // down the line.
    if (!haveClient())
        Client::initThread(getThreadName());

    auto const client = Client::getCurrent();
    auto const serviceContext = client->getServiceContext();

    serviceContext->setKillAllOperations();

    // Shutdown watchdog before service entry point since it has a reference to the entry point
    shutdownIdleWatchdog(serviceContext);

    // Shutdown the TransportLayer so that new connections aren't accepted
    if (auto tl = serviceContext->getTransportLayer()) {
        LOGV2_OPTIONS(24226,
                      {logComponentV1toV2(logger::LogComponent::kNetwork)},
                      "shutdown: going to close listening sockets...");
        tl->shutdown();
    }

    serviceContext->setKillAllOperations();

    if (serviceContext->getServiceEntryPoint()) {
        serviceContext->getServiceEntryPoint()->endAllSessions(transport::Session::kEmptyTagMask);
        serviceContext->getServiceEntryPoint()->shutdown(Seconds{10});
    }

    auto& lockFile = StorageEngineLockFile::get(serviceContext);
    if (lockFile) {
        lockFile->clearPidAndUnlock();
    }

    LOGV2_OPTIONS(24227, {logComponentV1toV2(logger::LogComponent::kControl)}, "now exiting");
}

ExitCode initAndListen() {
    Client::initThread("initandlisten");

#ifndef _WIN32
    boost::filesystem::path socketFile(serverGlobalParams.socket);
    socketFile /= "mongocryptd.sock";
#endif

    auto serviceContext = getGlobalServiceContext();
    {
        ProcessId pid = ProcessId::getCurrent();
        logv2::DynamicAttributes attrs;

        attrs.add("pid", pid.toNative());
        attrs.add("port", serverGlobalParams.port);
#ifndef _WIN32
        std::string socketFileStr;
        if (!serverGlobalParams.noUnixSocket) {
            socketFileStr = socketFile.generic_string();
            attrs.add("socketFile", socketFileStr);
        }
#endif
        const bool is32bit = sizeof(int*) == 4;
        attrs.add("architecture", is32bit ? "32-bit"_sd : "64-bit"_sd);
        std::string hostName = getHostNameCached();
        attrs.add("host", hostName);
        LOGV2_OPTIONS(4615669, {logv2::LogComponent::kControl}, "MongoCryptD starting", attrs);
    }

    if (kDebugBuild)
        LOGV2_OPTIONS(24228,
                      {logComponentV1toV2(logger::LogComponent::kControl)},
                      "DEBUG build (which is slower)");

#ifdef _WIN32
    VersionInfoInterface::instance().logTargetMinOS();
#endif

    logProcessDetails();

    createLockFile(serviceContext);

    startIdleWatchdog(serviceContext,
                      mongoCryptDGlobalParams.idleShutdownTimeout,
                      serviceContext->getServiceEntryPoint());

    // Aggregations which include a $changeStream stage must read the current FCV during parsing. If
    // the FCV is not initialized, this will hit an invariant. We therefore initialize it here.
    serverGlobalParams.featureCompatibility.setVersion(
        ServerGlobalParams::FeatureCompatibility::Version::kFullyUpgradedTo44);

    // $changeStream aggregations also check for the presence of a ReplicationCoordinator at parse
    // time, since change streams can only run on a replica set. If no such co-ordinator is present,
    // then $changeStream will assume that it is running on a standalone mongoD, and will return a
    // non-sequitur error to the user.
    repl::ReplicationCoordinator::set(
        serviceContext, std::make_unique<repl::ReplicationCoordinatorNoOp>(serviceContext));

    serverGlobalParams.serviceExecutor = "synchronous";

    // Enable local TCP/IP by default since some drivers (i.e. C#), do not support unix domain
    // sockets
    // Bind only to localhost, ignore any defaults or command line options
    serverGlobalParams.bind_ips.clear();
    serverGlobalParams.bind_ips.push_back("127.0.0.1");

    // Not all machines have ipv6 so users have to opt-in.
    if (serverGlobalParams.enableIPv6) {
        serverGlobalParams.bind_ips.push_back("::1");
    }

    serverGlobalParams.port = mongoCryptDGlobalParams.port;

#ifndef _WIN32
    if (!serverGlobalParams.noUnixSocket) {
        serverGlobalParams.bind_ips.push_back(socketFile.generic_string());
        // Set noUnixSocket so that TransportLayer does not create a unix domain socket by default
        // and instead just uses the one we tell it to use.
        serverGlobalParams.noUnixSocket = true;
    }
#endif

    auto tl =
        transport::TransportLayerManager::createWithConfig(&serverGlobalParams, serviceContext);
    Status status = tl->setup();
    if (!status.isOK()) {
        LOGV2_ERROR(
            24233, "Failed to setup the transport layer: {status}", "status"_attr = redact(status));
        return EXIT_NET_ERROR;
    }

    serviceContext->setTransportLayer(std::move(tl));

    status = serviceContext->getServiceExecutor()->start();
    if (!status.isOK()) {
        LOGV2_ERROR(24234,
                    "Failed to start the service executor: {status}",
                    "status"_attr = redact(status));
        return EXIT_NET_ERROR;
    }

    status = serviceContext->getServiceEntryPoint()->start();
    if (!status.isOK()) {
        LOGV2_ERROR(24235,
                    "Failed to start the service entry point: {status}",
                    "status"_attr = redact(status));
        return EXIT_NET_ERROR;
    }

    status = serviceContext->getTransportLayer()->start();
    if (!status.isOK()) {
        LOGV2_ERROR(
            24236, "Failed to start the transport layer: {status}", "status"_attr = redact(status));
        return EXIT_NET_ERROR;
    }

    serviceContext->notifyStartupComplete();

#ifndef _WIN32
    mongo::signalForkSuccess();
#else
    if (ntservice::shouldStartService()) {
        ntservice::reportStatus(SERVICE_RUNNING);
        LOGV2(24229, "Service running");
    }
#endif

    MONGO_IDLE_THREAD_BLOCK;
    return waitForShutdown();
}

#ifdef _WIN32
ExitCode initService() {
    return initAndListen();
}
#endif

MONGO_INITIALIZER_GENERAL(ForkServer, ("EndStartupOptionHandling"), ("default"))
(InitializerContext* context) {
    mongo::forkServerOrDie();
    return Status::OK();
}

int CryptDMain(int argc, char** argv, char** envp) {

    registerShutdownTask(shutdownTask);

    setupSignalHandlers();
    runGlobalInitializersOrDie(argc, argv, envp);
    startSignalProcessingThread(LogFileStatus::kNoLogFileToRotate);

    initWireSpec();

    setGlobalServiceContext(ServiceContext::make());
    auto serviceContext = getGlobalServiceContext();
    serviceContext->setServiceEntryPoint(std::make_unique<ServiceEntryPointCryptD>(serviceContext));

#ifdef _WIN32
    ntservice::configureService(initService,
                                moe::startupOptionsParsed,
                                defaultServiceStrings,
                                std::vector<std::string>(),
                                std::vector<std::string>(argv, argv + argc));
#endif  // _WIN32

    cmdline_utils::censorArgvArray(argc, argv);

    if (!initializeServerGlobalState(serviceContext, PidFileWrite::kNoWrite))
        quickExit(EXIT_FAILURE);

    // Per SERVER-7434, startSignalProcessingThread must run after any forks (i.e.
    // initializeServerGlobalState) and before the creation of any other threads
    startSignalProcessingThread();

#ifdef _WIN32
    if (ntservice::shouldStartService()) {
        ntservice::startService();
        // exits directly and so never reaches here either.
    }
#endif  // _WIN32

    return initAndListen();
}

}  // namespace
}  // namespace mongo

#ifdef _WIN32
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF-16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through the argv() and envp() members.  This enables CrytpDMain()
// to process UTF-8 encoded arguments and environment variables without regard to platform.
int wmain(int argc, wchar_t* argvW[], wchar_t* envpW[]) {
    mongo::WindowsCommandLine wcl(argc, argvW, envpW);
    int exitCode = mongo::CryptDMain(argc, wcl.argv(), wcl.envp());
    mongo::quickExit(exitCode);
}
#else
int main(int argc, char* argv[], char** envp) {
    int exitCode = mongo::CryptDMain(argc, argv, envp);
    mongo::quickExit(exitCode);
}
#endif
