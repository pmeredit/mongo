// ldap_runtime_parameters.cpp

/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 */

#include "mongo/platform/basic.h"

#include "mongo/base/parse_number.h"
#include "mongo/db/service_context.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"

#include "ldap/ldap_runtime_parameters_gen.h"
#include "ldap_connection_options.h"
#include "ldap_manager.h"
#include "ldap_options.h"
#include "ldap_query_config.h"
#include "name_mapping/internal_to_ldap_user_name_mapper.h"

namespace mongo {
void LDAPServersSetting::append(OperationContext* opCtx,
                                BSONObjBuilder* b,
                                StringData name,
                                const boost::optional<TenantId>&) {
    *b << name << joinLdapHost(LDAPManager::get(getGlobalServiceContext())->getHosts(), ',');
}

Status LDAPServersSetting::setFromString(StringData str, const boost::optional<TenantId>&) {
    auto swURIs = LDAPConnectionOptions::parseHostURIs(str.toString());
    if (!swURIs.isOK()) {
        return swURIs.getStatus();
    }
    LDAPManager::get(getGlobalServiceContext())->setHosts(std::move(swURIs.getValue()));
    return Status::OK();
}

void LDAPTimeoutSetting::append(OperationContext* opCtx,
                                BSONObjBuilder* b,
                                StringData name,
                                const boost::optional<TenantId>&) {
    *b << name << LDAPManager::get(getGlobalServiceContext())->getTimeout().count();
}

Status LDAPTimeoutSetting::set(const BSONElement& newValueElement,
                               const boost::optional<TenantId>&) {
    int newValue;
    Status coerceStatus = newValueElement.tryCoerce(&newValue);
    if (!coerceStatus.isOK() || newValue <= 0) {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for connection timeout: " << newValueElement};
    }

    LDAPManager::get(getGlobalServiceContext())->setTimeout(Milliseconds(newValue));
    return Status::OK();
}

Status LDAPTimeoutSetting::setFromString(StringData str, const boost::optional<TenantId>&) {
    int newValue;
    Status status = NumberParser{}(str, &newValue);
    if (!status.isOK()) {
        return status;
    }

    if (newValue < 0) {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for connection timeout: " << newValue};
    }

    LDAPManager::get(getGlobalServiceContext())->setTimeout(Milliseconds(newValue));
    return Status::OK();
}

void LDAPRetrySetting::append(OperationContext* opCtx,
                              BSONObjBuilder* b,
                              StringData name,
                              const boost::optional<TenantId>&) {
    *b << name << LDAPManager::get(getGlobalServiceContext())->getRetryCount();
}

Status LDAPRetrySetting::set(const BSONElement& newValueElement, const boost::optional<TenantId>&) {
    int newValue;
    Status coerceStatus = newValueElement.tryCoerce(&newValue);
    if (!coerceStatus.isOK() || newValue < 0) {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for retry count: " << newValueElement};
    }

    LDAPManager::get(getGlobalServiceContext())->setRetryCount(newValue);
    return Status::OK();
}

Status LDAPRetrySetting::setFromString(StringData str, const boost::optional<TenantId>&) {
    int newValue;
    Status status = NumberParser{}(str, &newValue);
    if (!status.isOK()) {
        return status;
    }

    if (newValue < 0) {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for retry count: " << newValue};
    }

    LDAPManager::get(getGlobalServiceContext())->setRetryCount(newValue);
    return Status::OK();
}

void LDAPBindDNSetting::append(OperationContext* opCtx,
                               BSONObjBuilder* b,
                               StringData name,
                               const boost::optional<TenantId>&) {
    *b << name << LDAPManager::get(getGlobalServiceContext())->getBindDN();
}

Status LDAPBindDNSetting::setFromString(StringData str, const boost::optional<TenantId>&) {
    LDAPManager::get(getGlobalServiceContext())->setBindDN(str.toString());
    return Status::OK();
}

Status LDAPBindPasswordSetting::set(const BSONElement& newValueElement,
                                    const boost::optional<TenantId>& tenantId) {
    static const Status badTypeStatus(ErrorCodes::BadValue,
                                      "LDAP bind password must be a string or array of strings"_sd);
    if (newValueElement.type() == String) {
        return setFromString(newValueElement.String(), tenantId);
    } else if (newValueElement.type() == Array) {
        std::vector<SecureString> passwords;
        for (const auto& elem : newValueElement.Obj()) {
            if (elem.type() != String) {
                return badTypeStatus;
            }

            passwords.emplace_back(elem.checkAndGetStringData().rawData());
        }

        LDAPManager::get(getGlobalServiceContext())->setBindPasswords(std::move(passwords));
        return Status::OK();
    }

    return badTypeStatus;
}

Status LDAPBindPasswordSetting::setFromString(StringData str, const boost::optional<TenantId>&) {
    LDAPManager::get(getGlobalServiceContext())
        ->setBindPasswords({SecureString(str.toString().c_str())});
    return Status::OK();
}

void LDAPUserToDNMappingSetting::append(OperationContext* opCtx,
                                        BSONObjBuilder* b,
                                        StringData name,
                                        const boost::optional<TenantId>&) {
    *b << name << LDAPManager::get(getGlobalServiceContext())->getUserToDNMapping();
}

Status LDAPUserToDNMappingSetting::setFromString(StringData str, const boost::optional<TenantId>&) {
    auto swMapper = InternalToLDAPUserNameMapper::createNameMapper(str.toString());
    if (swMapper.getStatus() != Status::OK()) {
        return swMapper.getStatus();
    }

    LDAPManager::get(getGlobalServiceContext())->setUserNameMapper(std::move(swMapper.getValue()));
    return Status::OK();
}

void LDAPQueryTemplateSetting::append(OperationContext* opCtx,
                                      BSONObjBuilder* b,
                                      StringData name,
                                      const boost::optional<TenantId>&) {
    *b << name << LDAPManager::get(getGlobalServiceContext())->getQueryTemplate();
}

Status LDAPQueryTemplateSetting::setFromString(StringData str, const boost::optional<TenantId>&) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithUserNameAndAttributeTranform(str.toString());
    if (swQueryParameters.getStatus() != Status::OK()) {
        return swQueryParameters.getStatus();
    }

    LDAPManager::get(getGlobalServiceContext())
        ->setQueryConfig(std::move(swQueryParameters.getValue()));
    return Status::OK();
}
}  // namespace mongo
