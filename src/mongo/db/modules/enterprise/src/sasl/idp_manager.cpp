/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "sasl/idp_manager.h"

#include <memory>
#include <regex>
#include <vector>

#include "mongo/bson/json.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/util/pcre.h"
#include "sasl/oidc_parameters_gen.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {
using SharedIdentityProvider = IDPManager::SharedIdentityProvider;

namespace {
IDPManager globalIDPManager;
}  // namespace

IDPManager::IDPManager() {
    _providers = std::make_shared<std::vector<SharedIdentityProvider>>();
}

IDPManager* IDPManager::get() {
    return &globalIDPManager;
}

Status IDPManager::updateConfigurations(OperationContext* opCtx,
                                        const std::vector<IDPConfiguration>& cfgs) {
    auto newProviders = std::make_shared<ProviderList>();

    std::transform(cfgs.cbegin(),
                   cfgs.cend(),
                   std::back_inserter(*newProviders),
                   [](const auto& cfg) { return std::make_shared<IdentityProvider>(cfg); });

    auto oldProviders = std::atomic_exchange(&_providers, std::move(newProviders));  // NOLINT
    if (opCtx && !oldProviders->empty()) {
        // If there were not providers previously, then we have no users to invalidate.
        AuthorizationManager::get(opCtx->getServiceContext())->invalidateUserCache(opCtx);
    }

    return Status::OK();
}

Date_t IDPManager::getNextRefreshTime() const {
    auto nextRefresh = Date_t::max();

    auto providers = _providers;
    for (const auto& provider : *providers) {
        auto refresh = provider->getNextRefreshTime();
        if (refresh < nextRefresh) {
            nextRefresh = std::move(refresh);
        }
    }

    return nextRefresh;
}

Status IDPManager::_doRefreshIDPs(OperationContext* opCtx,
                                  const boost::optional<std::set<StringData>>& issuerNames,
                                  RefreshOption option) {
    std::map<StringData, Status> statuses;

    auto providers = _providers;
    bool invalidate = false;
    for (auto& provider : *providers) {
        if (issuerNames && !issuerNames->count(provider->getIssuer())) {
            continue;
        }

        auto swInvalidate = provider->refreshKeys(option);
        if (!swInvalidate.isOK()) {
            using T = decltype(statuses)::value_type;
            statuses.insert(T(provider->getIssuer(), std::move(swInvalidate.getStatus())));
            continue;
        }
        invalidate |= swInvalidate.getValue();
    }

    if (invalidate) {
        auto* am = AuthorizationManager::get(opCtx->getServiceContext());
        am->invalidateUsersFromDB(opCtx, "$external"_sd);
    }

    if (!statuses.empty()) {
        if (statuses.size() == 1) {
            auto& status = *statuses.begin();
            return status.second.withContext(str::stream() << "Failed to refresh IdentityProvider '"
                                                           << status.first << "'");
        }

        StringBuilder msg;
        msg << "One or more IDPs failed to refresh propertly: [";
        for (const auto& status : statuses) {
            msg << " '" << status.first << "' : {" << status.second << "},";
        }
        msg << " ]";

        // Actual codes may differ, but we have to choose one.
        return {statuses.begin()->second.code(), msg.str()};
    }

    return Status::OK();
}

StatusWith<SharedIdentityProvider> IDPManager::selectIDP(
    const boost::optional<StringData>& principalNameHint) try {
    // Pull shared_ptr into local to avoid getting caught by a concurrent change.
    auto providers = _providers;

    uassert(ErrorCodes::BadValue, "No identity providers registered", !providers->empty());

    if (principalNameHint == boost::none) {
        uassert(ErrorCodes::BadValue,
                "Unable to determine identity provider without principal name hint",
                providers->size() == 1);
        return providers->front();
    }

    auto principalNameHintStr = principalNameHint->toString();
    for (const auto& provider : *providers) {
        auto optMatchPattern = provider->getConfig().getMatchPattern();
        if (!optMatchPattern ||
            pcre::Regex(optMatchPattern->toString()).matchView(principalNameHintStr)) {
            return provider;
        }
    }

    uasserted(ErrorCodes::BadValue,
              str::stream() << "No identity provider found using the hint '" << principalNameHint
                            << "' provided");

    MONGO_UNREACHABLE;
} catch (const DBException& ex) {
    return ex.toStatus();
}

StatusWith<SharedIdentityProvider> IDPManager::getIDP(StringData issuerName) try {
    auto providers = _providers;
    for (const auto& provider : *providers) {
        if (provider->getConfig().getIssuer() == issuerName) {
            return provider;
        }
    }

    uasserted(ErrorCodes::NoSuchKey,
              str::stream() << "Unknown Identity Provider '" << issuerName << "'");

    MONGO_UNREACHABLE;
} catch (const DBException& ex) {
    return ex.toStatus();
}

void IDPManager::serialize(BSONArrayBuilder* builder) const {
    auto providers = _providers;
    for (const auto& provider : *providers) {
        BSONObjBuilder idpBuilder(builder->subobjStart());
        provider->serialize(&idpBuilder);
    }
}

namespace {

constexpr auto kOIDCMechanismName = "MONGODB-OIDC"_sd;
bool oidcIsEnabled() {
    if (!gFeatureFlagOIDC.isEnabledAndIgnoreFCV()) {
        return false;
    }

    const auto& mechs = saslGlobalParams.authenticationMechanisms;
    return std::any_of(
        mechs.cbegin(), mechs.cend(), [](const auto& mech) { return mech == kOIDCMechanismName; });
}

Status validateSetParameterAction(const boost::optional<TenantId>& tenantId) {
    if (!oidcIsEnabled()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Authentication mechanism '" << kOIDCMechanismName
                              << "' is not enabled"};
    }

    if (tenantId != boost::none) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "OIDC configuration may not be set/read on a tenant"};
    }

    return Status::OK();
}

void uassertNonEmptyString(const IDPConfiguration& idp, StringData value, StringData name) {
    uassert(ErrorCodes::BadValue,
            str::stream() << "Field '" << name << "' for issuer '" << idp.getIssuer()
                          << "' must be non-empty",
            !value.empty());
}

void uassertNonEmptyString(const IDPConfiguration& idp,
                           const boost::optional<StringData>& value,
                           StringData name) {
    uassert(ErrorCodes::BadValue,
            str::stream() << "Missing required field '" << name << "' for issuer '"
                          << idp.getIssuer() << "'",
            value != boost::none);
    uassertNonEmptyString(idp, value.get(), name);
}

void uassertVectorNonEmptyString(const IDPConfiguration& idp,
                                 const std::vector<StringData>& values,
                                 StringData name) {
    for (const auto& value : values) {
        uassertNonEmptyString(idp, value, name);
    }
}

void uassertValidURL(const IDPConfiguration& idp, StringData value, StringData name) {
    uassertNonEmptyString(idp, value, name);

    if (getTestCommandsEnabled() && value.startsWith("http://"_sd)) {
        return;
    }

    uassert(ErrorCodes::BadValue,
            str::stream() << "Field '" << name << "' for issuer '" << idp.getIssuer()
                          << "' must be an https:// URL",
            value.startsWith("https://"_sd));
}

std::vector<IDPConfiguration> parseConfigFromBSONObj(BSONArray config) {
    const auto numIDPs = config.nFields();
    std::set<StringData> issuers;

    std::vector<IDPConfiguration> ret;
    for (const auto& elem : config) {
        uassert(ErrorCodes::BadValue,
                "OIDC configuration must be an array of objects",
                elem.type() == Object);

        auto idp = IDPConfiguration::parseOwned(IDLParserContext("IDPConfiguration"),
                                                elem.Obj().getOwned());

        uassertNonEmptyString(idp, idp.getIssuer(), IDPConfiguration::kIssuerFieldName);
        uassert(ErrorCodes::BadValue,
                str::stream() << "Duplicate configuration for issuer '" << idp.getIssuer() << "'",
                issuers.count(idp.getIssuer()) == 0);
        issuers.insert(idp.getIssuer());

        uassertNonEmptyString(idp, idp.getAudience(), IDPConfiguration::kAudienceFieldName);
        uassertNonEmptyString(idp, idp.getClientId(), IDPConfiguration::kClientIdFieldName);
        uassertNonEmptyString(
            idp, idp.getPrincipalName(), IDPConfiguration::kPrincipalNameFieldName);
        uassertNonEmptyString(
            idp, idp.getAuthorizationClaim(), IDPConfiguration::kAuthorizationClaimFieldName);
        uassertValidURL(idp, idp.getJWKS(), IDPConfiguration::kJWKSFieldName);

        if (numIDPs > 1) {
            uassertNonEmptyString(
                idp, idp.getAuthNamePrefix(), IDPConfiguration::kAuthNamePrefixFieldName);
            uassertNonEmptyString(
                idp, idp.getMatchPattern(), IDPConfiguration::kMatchPatternFieldName);
        }

        if (auto optSecret = idp.getClientSecret()) {
            uassertNonEmptyString(idp, *optSecret, IDPConfiguration::kClientSecretFieldName);
        }
        if (auto optScopes = idp.getRequestScopes()) {
            uassertVectorNonEmptyString(idp, *optScopes, IDPConfiguration::kRequestScopesFieldName);
        }
        if (auto optLogClaims = idp.getLogClaims()) {
            uassertVectorNonEmptyString(idp, *optLogClaims, IDPConfiguration::kLogClaimsFieldName);
        } else {
            idp.setLogClaims(std::vector({"iss"_sd, "sub"_sd}));
        }

        uassert(ErrorCodes::BadValue,
                "When parameter 'authURL' is specified, 'tokenURL' must be specified as well",
                idp.getAuthURL().has_value() == idp.getTokenURL().has_value());
        uassert(ErrorCodes::BadValue,
                "At least one of 'authURL' and/or 'deviceAuthURL' must be provided",
                idp.getAuthURL() || idp.getDeviceAuthURL());
        if (auto authURL = idp.getAuthURL()) {
            uassertValidURL(idp, *authURL, IDPConfiguration::kAuthURLFieldName);
        }
        if (auto tokenURL = idp.getTokenURL()) {
            uassertValidURL(idp, *tokenURL, IDPConfiguration::kTokenURLFieldName);
        }
        if (auto deviceAuthURL = idp.getDeviceAuthURL()) {
            uassertValidURL(idp, *deviceAuthURL, IDPConfiguration::kDeviceAuthURLFieldName);
        }

        const auto pollsecs = idp.getJWKSPollSecs();
        uassert(ErrorCodes::BadValue,
                str::stream() << "Invalid refresh period '" << pollsecs
                              << "', must be greater than '"
                              << IdentityProvider::kRefreshMinPeriodSecs << "'",
                pollsecs >= IdentityProvider::kRefreshMinPeriodSecs);

        ret.push_back(std::move(idp));
    }

    return ret;
}

Status setConfigFromBSONObj(BSONArray config) try {
    auto newConfig = parseConfigFromBSONObj(config);

    // At runtime, we expect Client::getCurrent()->getOperationContext() will succeed.
    // If no client is available, try to fetch the globalServiceContext to make one.
    // If the client has no operation context, make one.
    // If there is no ServiceContext available, we're in startup and the IDPManager won't need one.
    OperationContext* opCtx = nullptr;
    ServiceContext::UniqueClient clientHolder;
    ServiceContext::UniqueOperationContext opCtxHolder;
    auto* client = Client::getCurrent();
    if (!client && hasGlobalServiceContext()) {
        clientHolder = getGlobalServiceContext()->makeClient("IDPManager::setConfigFromBSONObj");
        client = clientHolder.get();
        fassert(7070297, client);
    }
    if (client) {
        opCtx = client->getOperationContext();
        if (!opCtx) {
            opCtxHolder = client->makeOperationContext();
            opCtx = opCtxHolder.get();
            fassert(7070296, opCtx);
        }
    }

    auto* idpManager = IDPManager::get();
    auto status = idpManager->updateConfigurations(opCtx, std::move(newConfig));
    if (!status.isOK()) {
        return status;
    }

    LOGV2_DEBUG(7070204, 3, "Loaded new OIDC IDP definitions", "numIDPs"_attr = idpManager->size());
    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}
}  // namespace

void OIDCIdentityProvidersParameter::append(OperationContext* opCtx,
                                            BSONObjBuilder* builder,
                                            StringData fieldName,
                                            const boost::optional<TenantId>& tenantId) {
    if (!validateSetParameterAction(tenantId).isOK()) {
        return;
    }

    BSONArrayBuilder listBuilder(builder->subarrayStart(fieldName));
    IDPManager::get()->serialize(&listBuilder);
}

Status OIDCIdentityProvidersParameter::set(const BSONElement& elem,
                                           const boost::optional<TenantId>& tenantId) {
    if (auto status = validateSetParameterAction(tenantId); !status.isOK()) {
        return status;
    }

    if (elem.type() != Array) {
        return {ErrorCodes::BadValue, str::stream() << "OIDC configuration must be a BSON object"};
    }

    return setConfigFromBSONObj(BSONArray(elem.Obj()));
}

Status OIDCIdentityProvidersParameter::setFromString(StringData str,
                                                     const boost::optional<TenantId>& tenantId) {
    if (auto status = validateSetParameterAction(tenantId); !status.isOK()) {
        return status;
    }

    BSONObj obj;
    try {
        obj = fromjson(str);
    } catch (const DBException& ex) {
        return ex.toStatus().withContext("Failed parsing OIDC parameters from JSON");
    }

    if (!obj.couldBeArray()) {
        return {ErrorCodes::BadValue, str::stream() << "OIDC configuration must be an array"};
    }

    return setConfigFromBSONObj(BSONArray(obj));
}

Status OIDCIdentityProvidersParameter::validate(const BSONElement& elem,
                                                const boost::optional<TenantId>& tenantId) const
    try {
    if (auto status = validateSetParameterAction(tenantId); !status.isOK()) {
        return status;
    }

    if (elem.type() != Array) {
        return {ErrorCodes::BadValue, str::stream() << "OIDC configuration must be an array"};
    }

    // Parse config, but don't actually set it anywhere.
    parseConfigFromBSONObj(BSONArray(elem.Obj()));
    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

}  // namespace mongo::auth
