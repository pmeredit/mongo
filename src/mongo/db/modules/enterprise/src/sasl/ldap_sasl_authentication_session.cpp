/*
 *    Copyright (C) 2016 MongoDB Inc.
 */


#include <vector>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/size.hpp>

#include "mongo/base/secure_allocator.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/sasl_mechanism_policies.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"
#include "mongo/db/curop.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"

#include "../ldap/ldap_manager.h"
#include "../ldap/ldap_options.h"
#include "../ldap/name_mapping/internal_to_ldap_user_name_mapper.h"
#include "cyrus_sasl_authentication_session.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {

namespace {

// Checks the globally static set of SASL mechanisms used to bind to an LDAP server.
// Returns a not-OK Status if MongoDB cannot use the mechanisms for accepting PLAIN
// authentication attempts.
Status supportedBindConfiguration() {
    // If we are not using saslauthd, we must allowlist the authentication
    // mechanisms we're willing to bind to LDAP servers with.
    std::set<std::string> permittedMechanisms{"DIGEST-MD5", "PLAIN"};
#ifdef _WIN32
    // On Windows, the platform provided GSSAPI mechanism uses client provided plaintext
    // passwords to acquire TGTs.
    permittedMechanisms.emplace("GSSAPI");
#endif

    std::vector<std::string> mechanisms;
    std::vector<std::string> difference;
    boost::split(mechanisms, globalLDAPParams->bindSASLMechanisms, boost::is_any_of(","));
    std::set_difference(mechanisms.begin(),
                        mechanisms.end(),
                        permittedMechanisms.begin(),
                        permittedMechanisms.end(),
                        std::inserter(difference, difference.begin()));

    if (!difference.empty()) {
        return Status(ErrorCodes::BadValue, "Unsupported outbound LDAP SASL bind mechanism");
    }

    return Status::OK();
}

}  // namespace

struct LDAPPLAINServerMechanism : MakeServerMechanism<PLAINPolicy> {
    LDAPPLAINServerMechanism(std::string authenticationDatabase)
        : MakeServerMechanism<PLAINPolicy>(std::move(authenticationDatabase)) {}

    boost::optional<unsigned int> currentStep() const override {
        return (unsigned int)1;
    }
    boost::optional<unsigned int> totalSteps() const override {
        return (unsigned int)1;
    }

private:
    StatusWith<std::tuple<bool, std::string>> stepImpl(OperationContext* opCtx,
                                                       StringData input) final;
};

StatusWith<std::tuple<bool, std::string>> LDAPPLAINServerMechanism::stepImpl(
    OperationContext* opCtx, StringData inputData) {

    Status status = supportedBindConfiguration();
    if (!status.isOK()) {
        return status;
    }

    // Expecting user input on the form: [authz-id]\0authn-id\0pwd
    std::string input = inputData.toString();

    // TODO: make PLAIN use the same code
    SecureString pwd = "";
    try {
        size_t firstNull = inputData.find('\0');
        if (firstNull == std::string::npos) {
            return Status(
                ErrorCodes::AuthenticationFailed,
                str::stream()
                    << "Incorrectly formatted PLAIN client message, missing first NULL delimiter");
        }
        size_t secondNull = inputData.find('\0', firstNull + 1);
        if (secondNull == std::string::npos) {
            return Status(
                ErrorCodes::AuthenticationFailed,
                str::stream()
                    << "Incorrectly formatted PLAIN client message, missing second NULL delimiter");
        }

        std::string authorizationIdentity = input.substr(0, firstNull);
        ServerMechanismBase::_principalName =
            input.substr(firstNull + 1, (secondNull - firstNull) - 1);
        if (ServerMechanismBase::_principalName.empty()) {
            return Status(ErrorCodes::AuthenticationFailed,
                          str::stream()
                              << "Incorrectly formatted PLAIN client message, empty username");
        } else if (!authorizationIdentity.empty() &&
                   authorizationIdentity != ServerMechanismBase::_principalName) {
            return Status(ErrorCodes::AuthenticationFailed,
                          str::stream()
                              << "SASL authorization identity must match authentication identity");
        }

        pwd = SecureString(input.substr(secondNull + 1).c_str());
        if (pwd->empty()) {
            return Status(ErrorCodes::AuthenticationFailed,
                          str::stream()
                              << "Incorrectly formatted PLAIN client message, empty password");
        }
    } catch (const std::out_of_range&) {
        return Status(ErrorCodes::AuthenticationFailed,
                      str::stream() << "Incorrectly formatted PLAIN client message");
    }

    status = LDAPManager::get(opCtx->getServiceContext())
                 ->verifyLDAPCredentials(ServerMechanismBase::_principalName,
                                         pwd,
                                         opCtx->getServiceContext()->getTickSource(),
                                         CurOp::get(opCtx)->getUserAcquisitionStats());
    if (!status.isOK()) {
        return status;
    }

    return std::make_tuple(true, std::string());
}

struct LDAPPLAINServerFactory : MakeServerFactory<LDAPPLAINServerMechanism> {
    using MakeServerFactory<LDAPPLAINServerMechanism>::MakeServerFactory;
    static constexpr bool isInternal = false;
    bool canMakeMechanismForUser(const User* user) const final {
        auto credentials = user->getCredentials();
        return credentials.isExternal;
    }
};


/**
 * The PLAIN mechanism will somtimes use the LDAP implementation,
 * othertimes it will use Cyrus SASL.
 *
 * Shim this proxy factory into place to dispatch as appropriate.
 */
class PLAINServerFactoryProxy : public ServerFactoryBase {
public:
    using policy_type = LDAPPLAINServerFactory::policy_type;
    static constexpr bool isInternal = LDAPPLAINServerFactory::isInternal;

    explicit PLAINServerFactoryProxy(Service* service)
        : ServerFactoryBase(service), _service(service), _cyrus(_service), _ldap(_service) {
        // This proxy assumes the targets have matching policy/mechanism types.
        static_assert(std::is_same<LDAPPLAINServerFactory::policy_type,
                                   CyrusPlainServerFactory::policy_type>::value,
                      "PLAINServerFactoryProxy targets have differing policy types");
        static_assert(LDAPPLAINServerFactory::isInternal == CyrusPlainServerFactory::isInternal,
                      "PLAINServerFactoryProxy targets have differing externality");

        // Supported configuration will be evaluated later during authentication.
        // This catches incorrect configurations at startup.
        const bool cyrus = useCyrus(service);
        LOGV2_DEBUG(4492600, 2, "Registering LDAP PLAIN SASL factory", "useCyrus"_attr = cyrus);

        if (!cyrus) {
            uassertStatusOK(supportedBindConfiguration());
        }
    }

    static bool useCyrus(Service* service) {
        return LDAPManager::get(service->getServiceContext())->useCyrusForAuthN();
    }

    StringData mechanismName() const final {
        return policy_type::getName();
    }

    SecurityPropertySet properties() const final {
        return policy_type::getProperties();
    }

    int securityLevel() const final {
        return policy_type::securityLevel();
    }

    bool canMakeMechanismForUser(const User* user) const final {
        if (useCyrus(_service)) {
            return _cyrus.canMakeMechanismForUser(user);
        } else {
            return _ldap.canMakeMechanismForUser(user);
        }
    }

    bool isInternalAuthMech() const final {
        return false;
    }

    ServerMechanismBase* createImpl(std::string authenticationDatabase) final {
        if (useCyrus(_service)) {
            return _cyrus.createImpl(std::move(authenticationDatabase));
        } else {
            return _ldap.createImpl(std::move(authenticationDatabase));
        }
    }

private:
    Service* _service = nullptr;
    CyrusPlainServerFactory _cyrus;
    LDAPPLAINServerFactory _ldap;
};


namespace {

Service::ConstructorActionRegisterer ldapRegisterer{
    "PLAINServerMechanismProxy",
    {"CreateSASLServerMechanismRegistry", "SetLDAPManagerImpl"},
    {"ValidateSASLServerMechanismRegistry"},
    [](Service* service) {
        auto& registry = SASLServerMechanismRegistry::get(service);
        registry.registerFactory<PLAINServerFactoryProxy>();
    }};

}  // namespace
}  // namespace mongo
