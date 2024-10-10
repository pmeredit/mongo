/**
 * Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "sasl/idp_manager.h"

#include <memory>
#include <regex>
#include <vector>

#include "mongo/bson/json.h"
#include "mongo/client/authenticate.h"
#include "mongo/crypto/jwks_fetcher_impl.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/exit.h"
#include "mongo/util/pcre.h"
#include "mongo/util/system_clock_source.h"
#include "sasl/oidc_parameters_gen.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {
using namespace fmt::literals;
using SharedIdentityProvider = IDPManager::SharedIdentityProvider;
using namespace fmt::literals;

namespace {

class JWKSFetcherFactoryImpl : public JWKSFetcherFactory {
public:
    // TODO (SERVER-90136) Move globalIDPManager to a ServiceContext decoration,
    // initialized after FastClockSource is set.
    // Once we unroll that, we can enforece always having an instance of FastClockSource.
    static ClockSource* getClockSource() {
        if (hasGlobalServiceContext()) {
            auto clock = getGlobalServiceContext()->getFastClockSource();
            if (MONGO_likely(clock)) {
                // Background thread with fast clock source is available, prefer it.
                return clock;
            }
        }

        // We're likely in initial statup, no fast clock source available.
        // Fall back on SystemClockSource which is only a litte slower.
        return SystemClockSource::get();
    }

    std::unique_ptr<crypto::JWKSFetcher> makeJWKSFetcher(StringData issuer) const final {
        return std::make_unique<crypto::JWKSFetcherImpl>(getClockSource(), issuer);
    }
};

IDPManager globalIDPManager(std::make_unique<JWKSFetcherFactoryImpl>());

stdx::mutex refreshIntervalMutex;
stdx::condition_variable refreshIntervalChanged;

StringDataMap<std::vector<IDPConfiguration*>> groupIDPConfigurationsByIssuer(
    std::vector<IDPConfiguration>& configs) {
    StringDataMap<std::vector<IDPConfiguration*>> groupedConfigs;
    for (auto& cfg : configs) {
        groupedConfigs[cfg.getIssuer()].push_back(&cfg);
    }
    return groupedConfigs;
}

}  // namespace

IDPManager::IDPManager(std::unique_ptr<JWKSFetcherFactory> typeFactory) {
    _typeFactory = std::move(typeFactory);
    _providerCatalog = std::make_shared<IdentityProviderCatalog>();
}

IDPManager* IDPManager::get() {
    return &globalIDPManager;
}

void IDPManager::initialize() {
    // Start JWKSetRefreshJob.
    if (_hasInitializedKeyRefresher.swap(1) == 0) {
        _keyRefresher.go();
    }
}

bool IDPManager::isOIDCEnabled() {
    const auto& mechs = saslGlobalParams.authenticationMechanisms;
    return std::any_of(
        mechs.cbegin(), mechs.cend(), [](const auto& mech) { return mech == kMechanismMongoOIDC; });
}

std::size_t IDPManager::size() const {
    return _providerCatalog->providersByConfigOrder.size();
}

void IDPManager::updateConfigurations(OperationContext* opCtx,
                                      const std::vector<IDPConfiguration>& cfgs) {
    auto newCatalog = std::make_shared<IdentityProviderCatalog>();
    StringDataMap<SharedIDPJWKSRefresher> refreshersByIssuer;

    // Create IDPJWKSRefresher for each unique issuer, then
    // create IdentityProviders for each IDPConfiguration.
    for (auto& cfg : cfgs) {
        auto& refresher = refreshersByIssuer[cfg.getIssuer()];
        if (!refresher) {
            refresher = std::make_shared<IDPJWKSRefresher>(*_typeFactory, cfg);
        }
        newCatalog->providersByConfigOrder.push_back(
            std::make_shared<IdentityProvider>(refresher, cfg));
    }

    // Index providers by issuer for faster lookup
    for (auto& idp : newCatalog->providersByConfigOrder) {
        newCatalog->providersByIssuerAndAudience[idp->getIssuer()][idp->getAudience()] = idp;
    }

    auto oldCatalog = std::atomic_exchange(&_providerCatalog, std::move(newCatalog));  // NOLINT
    if (opCtx && !oldCatalog->providersByConfigOrder.empty()) {
        // If there were not providers previously, then we have no users to invalidate.
        opCtx->getServiceContext()->applyToAllServices(
            [](Service* service) { AuthorizationManager::get(service)->invalidateUserCache(); });
    }
}

Date_t IDPManager::getNextRefreshTime() const {
    auto nextRefresh = Date_t{stdx::chrono::system_clock::time_point::max()};

    auto catalog = std::atomic_load(&_providerCatalog);  // NOLINT
    for (const auto& [_, providers] : catalog->providersByIssuerAndAudience) {
        // Retrieving the next refresh time from the first IdentityProvider is fine because
        // all IdentityProviders under the same issuer share the same key refresher.
        auto refresh = providers.begin()->second->getKeyRefresher()->getNextRefreshTime();
        if (refresh < nextRefresh) {
            nextRefresh = std::move(refresh);
        }
    }
    return nextRefresh;
}

void IDPManager::_flushIDPSJWKS() {
    auto catalog = std::atomic_load(&_providerCatalog);  // NOLINT
    for (auto& [_, providers] : catalog->providersByIssuerAndAudience) {
        // Calling flushJWKManagerKeys from the first IdentityProvider's key refresher is
        // fine because all IdentityProviders under the same issuer share the same key refresher.
        providers.begin()->second->getKeyRefresher()->flushJWKManagerKeys(*_typeFactory);
    }
}

Status IDPManager::refreshAllIDPs(OperationContext* opCtx,
                                  RefreshOption option,
                                  bool invalidateOnFailure) {
    return _doRefreshIDPs(opCtx, boost::none, option, invalidateOnFailure);
}

Status IDPManager::refreshIDPs(OperationContext* opCtx,
                               const std::set<StringData>& issuerNames,
                               RefreshOption option,
                               bool invalidateOnFailure) {
    return _doRefreshIDPs(opCtx, issuerNames, option, invalidateOnFailure);
}

Status IDPManager::_doRefreshIDPs(OperationContext* opCtx,
                                  const boost::optional<std::set<StringData>>& issuerNames,
                                  RefreshOption option,
                                  bool invalidateOnFailure) {
    std::vector<std::pair<StringData, Status>> statuses;

    ScopeGuard userInvalidation([&] {
        opCtx->getServiceContext()->applyToAllServices([](Service* service) {
            AuthorizationManager::get(service)->invalidateUsersFromDB(DatabaseName::kExternal);
        });
    });

    auto catalog = std::atomic_load(&_providerCatalog);  // NOLINT
    bool invalidateUsers = false;
    for (auto& [issuer, providers] : catalog->providersByIssuerAndAudience) {
        if (issuerNames && !issuerNames->count(issuer)) {
            continue;
        }

        auto swInvalidate =
            providers.begin()->second->getKeyRefresher()->refreshKeys(*_typeFactory, option);
        if (!swInvalidate.isOK()) {
            statuses.emplace_back(issuer, swInvalidate.getStatus());

            // We should invalidate users and flush keys when a refresh was forced and a failure
            // occured.
            if (option == RefreshOption::kNow && invalidateOnFailure) {
                invalidateUsers = true;
                _flushIDPSJWKS();
                break;
            }
        } else {
            invalidateUsers |= swInvalidate.getValue();
        }
    }
    if (!invalidateUsers) {
        userInvalidation.dismiss();
    }

    if (!statuses.empty()) {
        if (statuses.size() == 1) {
            auto& [issuer, status] = statuses.front();
            return status.withContext("Failed to refresh IdentityProvider '{}'"_format(issuer));
        }

        StringBuilder msg;
        msg << "One or more IDPs failed to refresh properly: [";
        for (const auto& [issuer, status] : statuses) {
            msg << " '" << issuer << "' : {" << status << "},";
        }
        msg << " ]";

        // Actual codes may differ, but we have to choose one.
        return {statuses.front().second.code(), msg.str()};
    }

    return Status::OK();
}

StatusWith<SharedIdentityProvider> IDPManager::selectIDP(
    const boost::optional<StringData>& principalNameHint) try {
    // Pull shared_ptr into local to avoid getting caught by a concurrent change.
    auto catalog = std::atomic_load(&_providerCatalog);  // NOLINT
    auto& providers = catalog->providersByConfigOrder;

    uassert(ErrorCodes::BadValue, "No identity providers registered", !providers.empty());

    /* If a client does not present a hint, we may attempt to guess an IdP to return metadata
     * about. If there is a single human flow IdP, we will return it. If there are zero or more
     * than one, we must fail because the answer is ambiguous.
     */
    if (principalNameHint == boost::none) {
        auto defaultProvider = providers.end();
        for (auto candidate = providers.begin(); candidate != providers.end(); candidate++) {
            if ((*candidate)->getConfig().getSupportsHumanFlows()) {
                uassert(
                    ErrorCodes::BadValue,
                    "Unable to determine identity provider, because multiple providers are known",
                    defaultProvider == providers.end());
                defaultProvider = candidate;
            }
        }
        uassert(ErrorCodes::BadValue,
                "Unable to determine identity provider, no provider supported human flows",
                defaultProvider != providers.end());
        return *defaultProvider;
    }

    auto principalNameHintStr = principalNameHint->toString();
    for (const auto& provider : providers) {
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

StatusWith<SharedIdentityProvider> IDPManager::getIDP(StringData issuerName,
                                                      StringData audienceName) try {
    auto catalog = std::atomic_load(&_providerCatalog);  // NOLINT

    auto issLookupItr = catalog->providersByIssuerAndAudience.find(issuerName);
    uassert(ErrorCodes::NoSuchKey,
            "Unknown Identity Provider '{}'"_format(issuerName),
            issLookupItr != catalog->providersByIssuerAndAudience.end());

    auto& providersByAudience = issLookupItr->second;
    auto audLookupItr = providersByAudience.find(audienceName);

    uassert(
        ErrorCodes::NoSuchKey,
        "Unknown audience name '{}' for Identity Provider '{}'"_format(audienceName, issuerName),
        audLookupItr != providersByAudience.end());

    return audLookupItr->second;
} catch (const DBException& ex) {
    return ex.toStatus();
}

void IDPManager::serializeConfig(BSONArrayBuilder* builder) const {
    auto catalog = std::atomic_load(&_providerCatalog);  // NOLINT
    for (const auto& provider : catalog->providersByConfigOrder) {
        BSONObjBuilder idpBuilder(builder->subobjStart());
        provider->serializeConfig(&idpBuilder);
    }
}

void IDPManager::serializeJWKSets(
    BSONObjBuilder* builder, const boost::optional<std::set<StringData>>& identityProviders) const {
    auto catalog = std::atomic_load(&_providerCatalog);  // NOLINT
    for (auto& [issuer, providers] : catalog->providersByIssuerAndAudience) {
        if (!identityProviders || identityProviders->count(issuer)) {
            BSONObjBuilder subObjBuilder(builder->subobjStart(issuer));
            // Calling serializeJWKSet from the first IdentityProvider's key refresher is
            // fine because all IdentityProviders under the same issuer share the same key
            // refresher.
            providers.begin()->second->getKeyRefresher()->serializeJWKSet(&subObjBuilder);
            subObjBuilder.doneFast();
        }
    }
}

namespace {

Status validateSetParameterAction(const boost::optional<TenantId>& tenantId) {
    if (!IDPManager::isOIDCEnabled()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Authentication mechanism '" << kMechanismMongoOIDC
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

// authNamePrefix must be a non-empty string made up of alnum, hyphens, and/or underscores
void uassertValidAuthNamePrefix(const IDPConfiguration& idp) {
    constexpr auto fieldName = IDPConfiguration::kAuthNamePrefixFieldName;
    const auto& prefix = idp.getAuthNamePrefix();

    uassertNonEmptyString(idp, prefix, fieldName);
    for (const auto ch : prefix) {
        uassert(ErrorCodes::BadValue,
                "Field '{}' for issuer '{}' must contain only alphanumerics, hyphens, "
                "and/or underscores. Encountered '{}'"_format(fieldName, idp.getIssuer(), ch),
                std::isalnum(ch) || (ch == '-') || (ch == '_'));
    }
}

// Asserts IDPConfigurations with the same issuer have unique audiences and have the same JWKS
// refresh settings.
void uassertSameIssuerConfigsAreValid(std::vector<IDPConfiguration>& configs) {
    auto configsByIssuer = groupIDPConfigurationsByIssuer(configs);

    for (auto& [_, groupedConfigs] : configsByIssuer) {
        if (groupedConfigs.size() < 2) {
            continue;
        }

        auto first = groupedConfigs.front();
        StringDataSet audiences = {first->getAudience()};

        for (auto itr = groupedConfigs.begin() + 1; itr != groupedConfigs.end(); ++itr) {
            uassert(
                ErrorCodes::BadValue,
                "IDP configurations with issuer '{}' must have the same JWKSPollSecs value"_format(
                    first->getIssuer()),
                (*itr)->getJWKSPollSecs() == first->getJWKSPollSecs());

            uassert(ErrorCodes::BadValue,
                    "Duplicate configuration for issuer-audience pair ('{}', '{}')"_format(
                        (*itr)->getIssuer(), (*itr)->getAudience()),
                    audiences.insert((*itr)->getAudience()).second);
        }
    }
}

Status setConfigFromBSONObj(BSONArray config) try {
    auto newConfig = IDPManager::parseConfigFromBSONObj(config);

    // At runtime, we expect Client::getCurrent()->getOperationContext() will succeed.
    // If no client is available, try to fetch the globalServiceContext to make one.
    // If the client has no operation context, make one.
    // If there is no ServiceContext available, we're in startup and the IDPManager won't need one.
    OperationContext* opCtx = nullptr;
    ServiceContext::UniqueClient clientHolder;
    ServiceContext::UniqueOperationContext opCtxHolder;
    auto* client = Client::getCurrent();
    if (!client && hasGlobalServiceContext()) {
        // This client is killable. If interrupted, we will catch the exception thrown and return
        // it.
        clientHolder =
            getGlobalServiceContext()->getService()->makeClient("IDPManager::setConfigFromBSONObj");
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
    idpManager->updateConfigurations(opCtx, std::move(newConfig));

    LOGV2_DEBUG(7070204, 3, "Loaded new OIDC IDP definitions", "numIDPs"_attr = idpManager->size());

    // Wake up JWKS refresher so it recomputes the next refresh time.
    stdx::unique_lock<stdx::mutex> lock(refreshIntervalMutex);
    refreshIntervalChanged.notify_all();

    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

}  // namespace

std::vector<IDPConfiguration> IDPManager::parseConfigFromBSONObj(BSONArray config) {
    using namespace fmt::literals;

    std::vector<IDPConfiguration> parsedObjects;
    parsedObjects.reserve(config.nFields());
    for (const auto& elem : config) {
        uassert(ErrorCodes::BadValue,
                "OIDC configuration must be an array of objects",
                elem.type() == Object);

        parsedObjects.emplace_back(IDPConfiguration::parseOwned(
            IDLParserContext("IDPConfiguration"), elem.Obj().getOwned()));
    }

    const size_t numHumanFlowIdPs = std::count_if(
        parsedObjects.begin(), parsedObjects.end(), [](const IDPConfiguration& config) {
            return config.getSupportsHumanFlows();
        });

    bool observedLastMatchPattern = false;
    for (auto& idp : parsedObjects) {
        const auto fcvSnapshot = serverGlobalParams.featureCompatibility.acquireFCVSnapshot();

        uassertNonEmptyString(idp, idp.getIssuer(), IDPConfiguration::kIssuerFieldName);
        uassertValidURL(idp, idp.getIssuer(), IDPConfiguration::kIssuerFieldName);
        uassertNonEmptyString(idp, idp.getAudience(), IDPConfiguration::kAudienceFieldName);

        uassertNonEmptyString(
            idp, idp.getPrincipalName(), IDPConfiguration::kPrincipalNameFieldName);

        // useAuthorizationClaim cannot be set to false if the feature flag is
        // disabled.
        uassert(ErrorCodes::BadValue,
                "Unrecognized field 'useAuthorizationClaim'",
                idp.getUseAuthorizationClaim() ||
                    gFeatureFlagOIDCInternalAuthorization.isEnabled(fcvSnapshot));

        // If the OIDC internal authorization feature flag is disabled, then authorizationClaim must
        // be specified. Otherwise, authorizationClaim must be specified if useAuthorizationClaim is
        // true.
        if (!gFeatureFlagOIDCInternalAuthorization.isEnabled(fcvSnapshot) ||
            idp.getUseAuthorizationClaim()) {
            uassertNonEmptyString(
                idp, idp.getAuthorizationClaim(), IDPConfiguration::kAuthorizationClaimFieldName);
        }

        // supportsHumanFlows cannot be set to false if the feature flag is disabled.
        uassert(ErrorCodes::BadValue,
                "Unrecognized field 'supportsHumanFlows'",
                idp.getSupportsHumanFlows() ||
                    gFeatureFlagOIDCInternalAuthorization.isEnabled(fcvSnapshot));

        // If the OIDC internal authorization feature flag is disabled, then clientId must
        // be specified. Otherwise, clientId must be specified if supportsHumanFlows is
        // true.
        if (!gFeatureFlagOIDCInternalAuthorization.isEnabled(fcvSnapshot) ||
            idp.getSupportsHumanFlows()) {
            uassertNonEmptyString(idp, idp.getClientId(), IDPConfiguration::kClientIdFieldName);
        }

        uassertValidAuthNamePrefix(idp);

        // Entries without matchPatterns must be sorted last.
        if (!idp.getMatchPattern()) {
            observedLastMatchPattern = true;
        }
        if (observedLastMatchPattern && idp.getMatchPattern()) {
            uasserted(
                ErrorCodes::BadValue,
                "All IdPs without matchPatterns must be listed after those with matchPatterns");
        }

        // An entry may have an empty matchPattern if it's intended for machine flows, *or* if it is
        // the only IdP intended for human flows.
        uassert(ErrorCodes::BadValue,
                "Required matchValue",
                idp.getMatchPattern() || !idp.getSupportsHumanFlows() || numHumanFlowIdPs == 1);

        if (auto optScopes = idp.getRequestScopes()) {
            uassertVectorNonEmptyString(idp, *optScopes, IDPConfiguration::kRequestScopesFieldName);
        }
        if (auto optLogClaims = idp.getLogClaims()) {
            uassertVectorNonEmptyString(idp, *optLogClaims, IDPConfiguration::kLogClaimsFieldName);
        } else {
            idp.setLogClaims(std::vector({"iss"_sd, "sub"_sd}));
        }

        const auto pollsecs = idp.getJWKSPollSecs();
        if (pollsecs.count() != 0) {
            uassert(ErrorCodes::BadValue,
                    str::stream() << "Invalid refresh period " << pollsecs
                                  << ", must be greater than "
                                  << IDPJWKSRefresher::kRefreshMinPeriod << ", or exactly 0",
                    pollsecs >= IDPJWKSRefresher::kRefreshMinPeriod);
        }
    }

    uassertSameIssuerConfigsAreValid(parsedObjects);

    return parsedObjects;
}

void OIDCIdentityProvidersParameter::append(OperationContext* opCtx,
                                            BSONObjBuilder* builder,
                                            StringData fieldName,
                                            const boost::optional<TenantId>& tenantId) {
    if (!validateSetParameterAction(tenantId).isOK()) {
        return;
    }

    BSONArrayBuilder listBuilder(builder->subarrayStart(fieldName));
    IDPManager::get()->serializeConfig(&listBuilder);
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
    IDPManager::parseConfigFromBSONObj(BSONArray(elem.Obj()));
    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

void JWKSetRefreshJob::run() {
    Client::initThread(name(), getGlobalServiceContext()->getService());
    auto* idpManager = IDPManager::get();

    while (!globalInShutdownDeprecated()) {
        Date_t wakeupTime;
        {
            // refreshIntervalChanged allows the job to wake up if an IDP reconfig causes the next
            // refresh time to potentially change.
            stdx::unique_lock<stdx::mutex> lock(refreshIntervalMutex);
            do {
                wakeupTime = idpManager->getNextRefreshTime();
                refreshIntervalChanged.wait_until(lock, wakeupTime.toSystemTimePoint());
            } while (wakeupTime > Date_t::now());
        }

        LOGV2_DEBUG(7119500, 1, "Refreshing JWKSets of configured IDPs");
        // This loop will repeat until we don't get interrupted while refreshing. We make a new
        // opCtx each loop because if we get interrupted, the opCtx should be treated as dead and a
        // new one must be created. Also, creating a new opCtx with each loop means we don't have to
        // worry about whether we are checking for interrupts frequently enough, which is a concern
        // if we had one opCtx for the whole job.
        while (!globalInShutdownDeprecated()) {
            auto opCtx = cc().makeOperationContext();
            try {
                auto status = idpManager->refreshAllIDPs(opCtx.get());
                if (!status.isOK()) {
                    LOGV2_WARNING(7119501,
                                  "Could not successfully refresh IDPs",
                                  "error"_attr = redact(status));
                }
                break;
            } catch (const DBException& ex) {
                auto interruptStatus = opCtx->checkForInterruptNoAssert();
                if (!interruptStatus.isOK()) {
                    // When interrupted, we should retry (as long as we are not yet shutting down).
                    LOGV2_DEBUG(
                        7857300, 3, "JWKSetRefresher was interrupted, retrying with new opctx");
                    continue;
                } else {
                    LOGV2_WARNING(7857301,
                                  "JWKSetRefresher encountered an unexpected error, retrying "
                                  "after refresh interval",
                                  "error"_attr = redact(ex.toStatus()));
                    break;
                }
            }
        }
    }
}

}  // namespace mongo::auth
