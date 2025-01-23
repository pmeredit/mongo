/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "audit_manager.h"

#include "audit/audit_options.h"
#include "mongo/util/assert_util.h"
#include <boost/filesystem.hpp>

#include "audit/audit_config_gen.h"
#include "audit/audit_key_manager_kmip.h"
#include "audit/audit_key_manager_local.h"
#include "audit/audit_options_gen.h"
#include "audit/mongo/audit_mongo.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "mongo/base/init.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_util.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/startup_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace moe = mongo::optionenvironment;

namespace mongo {
namespace audit {

namespace {
AuditManager globalAuditManager;
}  // namespace

AuditManager* getGlobalAuditManager() {
    return &globalAuditManager;
}

AuditManager::AuditManager() {
    // Make a runtime configuration be always available.
    _config = std::make_shared<RuntimeConfiguration>();
}

void AuditManager::setAuditAuthorizationSuccess(bool val) {
    uassert(ErrorCodes::BadValue,
            "auditAuthorizationSuccess may not be changed via set parameter when "
            "runtime audit configuration is enabled",
            !_runtimeConfiguration);

    _config->auditAuthorizationSuccess.store(val);
}

void AuditManager::setConfiguration(Client* client, const AuditConfigDocument& config) {
    uassert(ErrorCodes::RuntimeAuditConfigurationNotEnabled,
            "Unable to update runtime audit configuration when it has not been enabled",
            _enabled && _runtimeConfiguration);

    stdx::unique_lock<stdx::mutex> lk(_setConfigurationMutex);

    auto filterBSON = config.getFilter().getOwned();
    auto filter = parseFilter(filterBSON);

    // auditConfigure events are always emitted, regardless of filter settings.
    auto interface = AuditInterface::get(client->getServiceContext());
    interface->logConfigEvent(client, config);

    // Swap in the new configuration.
    auto newConfig = std::make_shared<RuntimeConfiguration>();
    newConfig->filterBSON = std::move(filterBSON);
    newConfig->filter = std::move(filter);
    newConfig->auditAuthorizationSuccess.store(config.getAuditAuthorizationSuccess());
    newConfig->timestamp = config.getClusterParameterTime();

    std::atomic_exchange(&_config, newConfig);  // NOLINT
    LOGV2(5497401, "Updated runtime audit configuration", "config"_attr = config);
}

AuditConfigDocument AuditManager::getAuditConfig() const {
    // Snapshot configuration at a point in time.
    auto current = std::atomic_load(&_config);

    AuditConfigDocument config;
    config.setClusterParameterTime(current->timestamp);
    config.setFilter(current->filterBSON.getOwned());
    config.setAuditAuthorizationSuccess(current->auditAuthorizationSuccess.load());

    return config;
}
std::unique_ptr<MatchExpression> AuditManager::parseFilter(BSONObj filter) {
    invariant(filter.isOwned());
    // We pass in a null OperationContext pointer here, since we do not have access to an
    // OperationContext. MatchExpressionParser::parse() only requires an OperationContext for
    // parsing $expr, which we explicitly disallow here.
    auto expCtx = ExpressionContextBuilder{}.ns(NamespaceString::kEmpty).build();
    StatusWithMatchExpression parseResult =
        MatchExpressionParser::parse(filter,
                                     std::move(expCtx),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kBanAllSpecialFeatures);

    uassertStatusOK(parseResult.getStatus().withContext("Failed to parse auditFilter"));
    return std::move(parseResult.getValue());
}

void AuditManager::_setDestinationFromConfig(const moe::Environment& params) {

    if (!params.count("auditLog.destination")) {
        // No destination means no auditing, period.
        return;
    }

    auto destination = params["auditLog.destination"].as<std::string>();
    if (destination == "file") {
        uassert(ErrorCodes::BadValue,
                "auditLog.format must be specified when auditLog.destination is to a file",
                params.count("auditLog.format"));
        auto format = params["auditLog.format"].as<std::string>();

        uassert(ErrorCodes::BadValue,
                "auditLog.path must be specified when auditLog.destination is to a file",
                params.count("auditLog.path"));
        auto path = params["auditLog.path"].as<std::string>();

#ifdef _WIN32
        if (params.count("install") || params.count("reinstall")) {
            uassert(ErrorCodes::BadValue,
                    "auditLog.path requires an absolute file path with Windows services",
                    boost::filesystem::path(path).is_absolute());
        }
#endif

        if (format == "JSON") {
            _format = AuditFormat::AuditFormatJsonFile;
        } else if (format == "BSON") {
            _format = AuditFormat::AuditFormatBsonFile;
        } else {
            uasserted(ErrorCodes::BadValue, "Invalid value for auditLog.format");
        }
        _path = boost::filesystem::absolute(path, serverGlobalParams.cwd).string();
    } else {
        uassert(ErrorCodes::BadValue,
                "auditLog.format and auditLog.path are only allowed when "
                "auditLog.destination is 'file'",
                !params.count("auditLog.format") && !params.count("auditLog.path"));
        uassert(ErrorCodes::BadValue,
                "Audit path must not be specified with syslog or console destinations",
                !params.count("auditLog.path"));

        if (destination == "syslog") {
#ifdef _WIN32
            uasserted(ErrorCodes::BadValue, "syslog not available on Windows");
#endif
            _format = AuditFormat::AuditFormatSyslog;
        } else if (destination == "console") {
            _format = AuditFormat::AuditFormatConsole;
        } else if (destination == "mock") {
            _format = AuditFormat::AuditFormatMock;
        } else {
            uasserted(ErrorCodes::BadValue, "invalid auditLog destination");
        }
    }

    _enabled = true;
}

void AuditManager::initialize(const moe::Environment& params) {
    invariant(!_enabled);

    _setDestinationFromConfig(params);
    if (!isEnabled()) {
        return;
    }

    if (params.count("auditLog.filter")) {
        BSONObj filter;
        try {
            filter = fromjson(params["auditLog.filter"].as<std::string>());
        } catch (const DBException& e) {
            uasserted(ErrorCodes::BadValue, str::stream() << "Bad auditFilter: " << e.what());
        }

        _config->filter = parseFilter(filter);
        _config->filterBSON = std::move(filter);
    }

    if (params.count("auditLog.runtimeConfiguration") &&
        params["auditLog.runtimeConfiguration"].as<bool>()) {
        uassert(
            ErrorCodes::BadValue,
            "auditLog.filter must not be configured when runtime audit configuration is enabled",
            !params.count("auditLog.filter"));

        if (params.count("setParameter")) {
            auto sp = params["setParameter"].as<std::map<std::string, std::string>>();
            uassert(ErrorCodes::BadValue,
                    "setParameter.auditAuthorizationSuccess must not be configured when runtime "
                    "audit configuration is enabled",
                    sp.find("auditAuthorizationSuccess") == sp.end());
        }

        _runtimeConfiguration = true;
    }

    _encryptionEnabled = (params.count("auditLog.localAuditKeyFile") ||
                          params.count("auditLog.auditEncryptionKeyIdentifier"));

    if (params.count("auditLog.compressionMode") &&
        params["auditLog.compressionMode"].as<std::string>() != "" &&
        params["auditLog.compressionMode"].as<std::string>() != "none") {
        uassert(ErrorCodes::BadValue,
                "auditLog.compressionMode is only allowed if audit log encryption is enabled",
                _encryptionEnabled);
        uassert(ErrorCodes::BadValue,
                "auditLog.compressionMode is only allowed if auditLog.destination is 'file'",
                isFileDestination());
        uassert(ErrorCodes::BadValue,
                "auditLog.compressionMode must be set as zstd",
                params["auditLog.compressionMode"].as<std::string>() == "zstd");
        _compressionEnabled = true;
    }

    if (params.count("auditLog.schema")) {
        _schema = uassertStatusOK(
            parseAuditSchema(moe::startupOptionsParsed["auditLog.schema"].as<std::string>()));
    }

    if (gAuditEncryptKeyWithKMIPGet) {
        uassert(ErrorCodes::BadValue,
                "setParameter.auditEncryptKeyWithKMIPGet is only allowed if audit log "
                "encryption is enabled!",
                _encryptionEnabled);
        uassert(ErrorCodes::BadValue,
                "setParameter.auditEncryptKeyWithKMIPGet is only allowed if "
                "auditLog.destination is 'file'",
                isFileDestination());
    }

    if (!gAuditEncryptionHeaderMetadataFile.empty()) {
        uassert(ErrorCodes::BadValue,
                "setParameter.auditEncryptionHeaderMetadataFile is only allowed if audit log "
                "encryption is enabled!",
                _encryptionEnabled);
        uassert(ErrorCodes::BadValue,
                "setParameter.auditEncryptionHeaderMetadataFile is only allowed if "
                "auditLog.destination is 'file'",
                isFileDestination());
        _headerMetadataPath =
            boost::filesystem::absolute(gAuditEncryptionHeaderMetadataFile, serverGlobalParams.cwd)
                .string();
    }

    _initializeAuditLog(params);
}

void AuditManager::resetConfiguration(Client* client) {
    if (_enabled && _runtimeConfiguration) {
        setConfiguration(client,
                         {{}, false /* auditAuthorizationSuccess */, LogicalTime::kUninitialized});
    }
}

const AuditEncryptionCompressionManager* AuditManager::getAuditEncryptionCompressionManager() {
    invariant(getEncryptionEnabled());
    if (!_ac) {
        // lazily create.
        std::unique_ptr<AuditKeyManager> keyManager;

        switch (_managerType) {
            case ManagerType::kLocal:
                keyManager = std::make_unique<AuditKeyManagerLocal>(_localAuditKeyFile);
                break;
            case ManagerType::kKMIPGet:
                keyManager = std::make_unique<AuditKeyManagerKMIPGet>(_auditEncryptionKeyUID);
                break;
            case ManagerType::kKMIPEncrypt:
                keyManager = std::make_unique<AuditKeyManagerKMIPEncrypt>(
                    _auditEncryptionKeyUID,
                    gAuditEncryptionWithKMS ? KeyStoreIDFormat::kmsConfigStruct
                                            : KeyStoreIDFormat::kmipKeyIdentifier);
                break;
            default:
                // If encryption enabled, _managerType must be valid
                MONGO_UNREACHABLE;
        }

        _ac = std::make_unique<AuditEncryptionCompressionManager>(std::move(keyManager),
                                                                  getCompressionEnabled());
    }
    return _ac.get();
}

// Only to be called on startup - if an initial rotate is attempted but does
// not succeed, throw an exception
void rotateAuditLog() {
    if (!globalAuditManager.isEnabled() || !globalAuditManager.isFileDestination()) {
        return;
    }

    uassertStatusOK(logv2::rotateLogs(serverGlobalParams.logRenameOnRotate,
                                      logv2::kAuditLogTag,
                                      [](Status s) -> void { uassertStatusOK(s); }));
}

namespace {
MONGO_INITIALIZER_WITH_PREREQUISITES(InitializeGlobalAuditManager,
                                     ("AllCompressorsRegistered",
                                      "EndStartupOptionHandling",
                                      "MatchExpressionParser",
                                      "PathlessOperatorMap",
                                      "CryptographyInitialized"))
(InitializerContext* context) {
    globalAuditManager.initialize(moe::startupOptionsParsed);
}

void initializeAuditInterface(ServiceContext* svcCtx) {
    // This initializer expects an audit interface to already be in place.
    invariant(AuditInterface::get(svcCtx) != nullptr);

    auto options = moe::startupOptionsParsed;
    if (!options.count("auditLog.destination")) {
        // Leave existing interface if there is no audit destination
        return;
    }

    auto schema = globalAuditManager.getSchema();
    if (schema == AuditSchema::kMongo) {
        audit::AuditInterface::set(svcCtx, std::make_unique<audit::AuditMongo>());
    } else {
        invariant(schema == AuditSchema::kOCSF);
        audit::AuditInterface::set(svcCtx, std::make_unique<audit::AuditOCSF>());
    }
}

// Run after the community's initialization so that we always take precedence.
const ServiceContext::ConstructorActionRegisterer serviceExecutorSynchronousRegisterer{
    "initializeAuditInterface", {"initializeNoopAuditInterface"}, initializeAuditInterface};

}  // namespace

}  // namespace audit
}  // namespace mongo
