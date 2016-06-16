/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/bson/json.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/memory.h"

#include "ldap_manager_impl.h"
#include "ldap_options.h"

namespace mongo {

/* Make a LDAPRunnerImpl pointer a decoration on the global ServiceContext */
MONGO_INITIALIZER_WITH_PREREQUISITES(SetLDAPManagerImpl, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                std::move(globalLDAPParams->bindPassword),
                                globalLDAPParams->bindMethod,
                                globalLDAPParams->bindSASLMechanisms,
                                globalLDAPParams->useOSDefaults);
    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                            globalLDAPParams->serverURIs);

    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        globalLDAPParams->userAcquisitionQueryTemplate);
    massertStatusOK(swQueryParameters.getStatus());

    BSONObj expression;
    try {
        expression = fromjson(globalLDAPParams->userToDNMapping);
    } catch (DBException& e) {
        e.addContext(
            "Failed to parse JSON description of the relationship between "
            "MongoDB usernames and LDAP DNs");
        throw e;
    }
    auto swMapper = InternalToLDAPUserNameMapper::createNameMapper(std::move(expression));
    massertStatusOK(swMapper.getStatus());

    auto manager = stdx::make_unique<LDAPManagerImpl>(std::move(bindOptions),
                                                      std::move(connectionOptions),
                                                      std::move(swQueryParameters.getValue()),
                                                      std::move(swMapper.getValue()));
    LDAPManager::set(getGlobalServiceContext(), std::move(manager));

    return Status::OK();
}
}  // mongo
