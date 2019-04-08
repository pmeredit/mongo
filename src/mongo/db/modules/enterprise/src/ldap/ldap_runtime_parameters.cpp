// ldap_runtime_parameters.cpp

/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 */

#include "mongo/platform/basic.h"

#include "ldap/ldap_runtime_parameters_gen.h"
#include "mongo/db/service_context.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/parse_log_component_settings.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"

#include "ldap_connection_options.h"
#include "ldap_manager.h"
#include "ldap_options.h"
#include "ldap_query_config.h"
#include "name_mapping/internal_to_ldap_user_name_mapper.h"

namespace mongo {

void LDAPServersSetting::append(OperationContext* opCtx,
                                BSONObjBuilder& b,
                                const std::string& name) {
    b << name << StringSplitter::join(LDAPManager::get(getGlobalServiceContext())->getHosts(), ",");
}

Status LDAPServersSetting::setFromString(const std::string& str) {
    auto swURIs = LDAPConnectionOptions::parseHostURIs(str);
    if (!swURIs.isOK()) {
        return swURIs.getStatus();
    }
    LDAPManager::get(getGlobalServiceContext())->setHosts(std::move(swURIs.getValue()));
    return Status::OK();
}

void LDAPTimeoutSetting::append(OperationContext* opCtx,
                                BSONObjBuilder& b,
                                const std::string& name) {
    b << name << LDAPManager::get(getGlobalServiceContext())->getTimeout().count();
}

Status LDAPTimeoutSetting::set(const BSONElement& newValueElement) {
    int newValue;
    if (!newValueElement.coerce(&newValue) || newValue < 0) {
        return {ErrorCodes::BadValue,
                str::stream() << "Invalid value for connection timeout: " << newValueElement};
    }

    LDAPManager::get(getGlobalServiceContext())->setTimeout(Milliseconds(newValue));
    return Status::OK();
}

Status LDAPTimeoutSetting::setFromString(const std::string& str) {
    int newValue;
    Status status = parseNumberFromString(str, &newValue);
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

void LDAPBindDNSetting::append(OperationContext* opCtx,
                               BSONObjBuilder& b,
                               const std::string& name) {
    b << name << LDAPManager::get(getGlobalServiceContext())->getBindDN();
}

Status LDAPBindDNSetting::setFromString(const std::string& str) {
    LDAPManager::get(getGlobalServiceContext())->setBindDN(str);
    return Status::OK();
}

Status LDAPBindPasswordSetting::setFromString(const std::string& str) {
    LDAPManager::get(getGlobalServiceContext())->setBindPassword(SecureString(str.c_str()));
    return Status::OK();
}

void LDAPUserToDNMappingSetting::append(OperationContext* opCtx,
                                        BSONObjBuilder& b,
                                        const std::string& name) {
    b << name << LDAPManager::get(getGlobalServiceContext())->getUserToDNMapping();
}

Status LDAPUserToDNMappingSetting::setFromString(const std::string& str) {
    auto swMapper = InternalToLDAPUserNameMapper::createNameMapper(str);
    if (swMapper.getStatus() != Status::OK()) {
        return swMapper.getStatus();
    }

    LDAPManager::get(getGlobalServiceContext())->setUserNameMapper(std::move(swMapper.getValue()));
    return Status::OK();
}

void LDAPQueryTemplateSetting::append(OperationContext* opCtx,
                                      BSONObjBuilder& b,
                                      const std::string& name) {
    b << name << LDAPManager::get(getGlobalServiceContext())->getQueryTemplate();
}

Status LDAPQueryTemplateSetting::setFromString(const std::string& str) {
    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserName(str);
    if (swQueryParameters.getStatus() != Status::OK()) {
        return swQueryParameters.getStatus();
    }

    LDAPManager::get(getGlobalServiceContext())
        ->setQueryConfig(std::move(swQueryParameters.getValue()));
    return Status::OK();
}

}  // namespace mongo
