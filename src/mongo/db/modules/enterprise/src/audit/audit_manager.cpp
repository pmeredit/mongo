/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_manager.h"

#include <boost/filesystem.hpp>

#include "mongo/base/init.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/startup_options.h"

namespace moe = mongo::optionenvironment;

namespace mongo {
namespace audit {

namespace {
AuditManager globalAuditManager;
}

AuditManager* getGlobalAuditManager() {
    return &globalAuditManager;
}

AuditManager::~AuditManager() {
    if (_filter) {
        // Allow this to leak on shutdown.
        _filter.release();
    }
}

void AuditManager::_setFilter(BSONObj filter) {
    // We pass in a null OperationContext pointer here, since we do not have access to an
    // OperationContext. MatchExpressionParser::parse() only requires an OperationContext for
    // parsing $expr, which we explicitly disallow here.
    auto ownedFilter = filter.getOwned();
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(
        nullptr /* opCtx */, std::unique_ptr<CollatorInterface>(nullptr), NamespaceString("")));
    StatusWithMatchExpression parseResult =
        MatchExpressionParser::parse(ownedFilter,
                                     std::move(expCtx),
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kBanAllSpecialFeatures);

    uassertStatusOK(parseResult.getStatus().withContext("Failed to parse auditFilter"));
    _filterBSON = std::move(ownedFilter);
    _filter = std::move(parseResult.getValue());
}

void AuditManager::_setDestinationFromConfig() {
    const auto& params = moe::startupOptionsParsed;

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
            _format = AuditFormatJsonFile;
        } else if (format == "BSON") {
            _format = AuditFormatBsonFile;
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
            _format = AuditFormatSyslog;
        } else if (destination == "console") {
            _format = AuditFormatConsole;
        } else {
            uasserted(ErrorCodes::BadValue, "invalid auditLog destination");
        }
    }

    _enabled = true;
}

void AuditManager::initialize() {
    invariant(!_enabled);

    _setDestinationFromConfig();
    if (!isEnabled()) {
        return;
    }

    const auto& params = moe::startupOptionsParsed;
    if (params.count("auditLog.filter")) {
        BSONObj filter;
        try {
            filter = fromjson(params["auditLog.filter"].as<std::string>());
        } catch (const DBException& e) {
            uasserted(ErrorCodes::BadValue, str::stream() << "Bad auditFilter: " << e.what());
        }

        _setFilter(filter);
    }
}

namespace {
MONGO_INITIALIZER_WITH_PREREQUISITES(InitializeGlobalAuditManager,
                                     ("EndStartupOptionHandling",
                                      "MatchExpressionParser",
                                      "PathlessOperatorMap"))
(InitializerContext* context) {
    globalAuditManager.initialize();
}
}  // namespace

}  // namespace audit
}  // namespace mongo
