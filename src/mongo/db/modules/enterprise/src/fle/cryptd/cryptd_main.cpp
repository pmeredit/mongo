/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/db/client.h"
#include "mongo/db/initialize_server_global_state.h"
#include "mongo/db/log_process_details.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine_lock_file.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/wire_version.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/transport/transport_layer_manager.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/cmdline_utils/censor_cmdline.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/text.h"
#include "mongo/util/version.h"

#include "cryptd_options.h"
#include "cryptd_service_entry_point.h"
#include "cryptd_watchdog.h"


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

    log() << "Using lock file: " << file.generic_string();
    try {
        lockFile.emplace(file.parent_path().generic_string(), file.filename().generic_string());
    } catch (const std::exception& ex) {
        error() << "Unable to determine status of lock file in the data directory "
                << file.generic_string() << ": " << ex.what(),
            _exit(EXIT_FAILURE);
    }

    const auto openStatus = lockFile->open();
    if (!openStatus.isOK()) {
        error() << "Failed to open pid file, exiting";
        _exit(EXIT_FAILURE);
    }

    const auto writeStatus = lockFile->writePid();
    if (!writeStatus.isOK()) {
        error() << "Failed to write pid file, exiting";
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
        log(logger::LogComponent::kNetwork) << "shutdown: going to close listening sockets...";
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

    log(logger::LogComponent::kControl) << "now exiting";
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
        LogstreamBuilder l = log(logger::LogComponent::kControl);
        l << "MongoCryptD starting : pid=" << pid;
#ifdef _WIN32
        l << " port=" << serverGlobalParams.port;
#else
        l << " socketFile=" << socketFile.generic_string();
#endif

        const bool is32bit = sizeof(int*) == 4;
        l << (is32bit ? " 32" : " 64") << "-bit host=" << getHostNameCached();
    }

    DEV log(logger::LogComponent::kControl) << "DEBUG build (which is slower)";

#ifdef _WIN32
    VersionInfoInterface::instance().logTargetMinOS();
#endif

    logProcessDetails();

    createLockFile(serviceContext);

    startIdleWatchdog(serviceContext,
                      mongoCryptDGlobalParams.idleShutdownTimeout,
                      serviceContext->getServiceEntryPoint());

    serverGlobalParams.serviceExecutor = "synchronous";
#ifdef _WIN32
    // Bind only to localhost, ignore any defaults or command line options
    serverGlobalParams.bind_ips.clear();
    serverGlobalParams.bind_ips.push_back("127.0.0.1");
    serverGlobalParams.enableIPv6 = true;
    serverGlobalParams.bind_ips.push_back("::1");
    serverGlobalParams.port = mongoCryptDGlobalParams.port;
#else
    serverGlobalParams.bind_ips.push_back(socketFile.generic_string());
#endif
    // Set noUnixSocket so that TransportLayer does not create a unix domain socket by default
    // and instead just uses the one we tell it to use.
    serverGlobalParams.noUnixSocket = true;

    auto tl =
        transport::TransportLayerManager::createWithConfig(&serverGlobalParams, serviceContext);
    uassertStatusOK(tl->setup());

    serviceContext->setTransportLayer(std::move(tl));

    Status status = serviceContext->getServiceExecutor()->start();
    if (!status.isOK()) {
        error() << "Failed to start the service executor: " << redact(status);
        return EXIT_NET_ERROR;
    }

    status = serviceContext->getServiceEntryPoint()->start();
    if (!status.isOK()) {
        error() << "Failed to start the service entry point: " << redact(status);
        return EXIT_NET_ERROR;
    }

    status = serviceContext->getTransportLayer()->start();
    if (!status.isOK()) {
        error() << "Failed to start the transport layer: " << redact(status);
        return EXIT_NET_ERROR;
    }

    serviceContext->notifyStartupComplete();

#ifndef _WIN32
    mongo::signalForkSuccess();
#else
    if (ntservice::shouldStartService()) {
        ntservice::reportStatus(SERVICE_RUNNING);
        log() << "Service running";
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

    if (!initializeServerGlobalState(serviceContext))
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
