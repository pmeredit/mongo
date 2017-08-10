// ldap_runtime_parameters.cpp

/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 */

#include "mongo/platform/basic.h"

#include "mongo/db/server_parameters.h"
#include "mongo/db/service_context.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/parse_log_component_settings.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/text.h"

#include "ldap_connection_options.h"
#include "ldap_manager.h"
#include "ldap_options.h"
#include "ldap_query_config.h"
#include "name_mapping/internal_to_ldap_user_name_mapper.h"

namespace mongo {
namespace {

class LDAPServersSetting : public ServerParameter {
public:
    LDAPServersSetting()
        : ServerParameter(ServerParameterSet::getGlobal(), "ldapServers", false, true) {}

    virtual void append(OperationContext* opCtx, BSONObjBuilder& b, const std::string& name) {
        b << name
          << StringSplitter::join(LDAPManager::get(getGlobalServiceContext())->getHosts(), ",");
    }

    virtual Status set(const BSONElement& newValueElement) {
        try {
            return setFromString(newValueElement.String());
        } catch (const DBException& msg) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream()
                              << "Invalid value for ldapServers via setParameter command: "
                              << newValueElement
                              << ", exception: "
                              << msg.what());
        }
    }

    virtual Status setFromString(const std::string& str) {
        auto swURIs = LDAPConnectionOptions::parseHostURIs(str);
        if (!swURIs.isOK()) {
            return swURIs.getStatus();
        }
        LDAPManager::get(getGlobalServiceContext())->setHosts(std::move(swURIs.getValue()));
        return Status::OK();
    }
} ldapServersSetting;

class LDAPTimeoutSetting : public ServerParameter {
public:
    LDAPTimeoutSetting()
        : ServerParameter(ServerParameterSet::getGlobal(), "ldapTimeoutMS", false, true) {}

    virtual void append(OperationContext* opCtx, BSONObjBuilder& b, const std::string& name) {
        b << name << LDAPManager::get(getGlobalServiceContext())->getTimeout().count();
    }

    virtual Status set(const BSONElement& newValueElement) {
        int newValue;
        if (!newValueElement.coerce(&newValue) || newValue < 0)
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Invalid value for connection timeout: "
                                                    << newValueElement);
        LDAPManager::get(getGlobalServiceContext())->setTimeout(Milliseconds(newValue));
        return Status::OK();
    }

    virtual Status setFromString(const std::string& str) {
        int newValue;
        Status status = parseNumberFromString(str, &newValue);
        if (!status.isOK())
            return status;
        if (newValue < 0)
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Invalid value for connection timeout: "
                                                    << newValue);
        LDAPManager::get(getGlobalServiceContext())->setTimeout(Milliseconds(newValue));
        return Status::OK();
    }
} ldapTimeoutSetting;

class LDAPBindDNSetting : public ServerParameter {
public:
    LDAPBindDNSetting()
        : ServerParameter(ServerParameterSet::getGlobal(), "ldapQueryUser", false, true) {}

    virtual void append(OperationContext* opCtx, BSONObjBuilder& b, const std::string& name) {
        b << name << LDAPManager::get(getGlobalServiceContext())->getBindDN();
    }

    virtual Status set(const BSONElement& newValueElement) {
        try {
            return setFromString(newValueElement.String());
        } catch (const DBException& msg) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream()
                              << "Invalid value for ldapQueryUser via setParameter command: "
                              << newValueElement
                              << ", exception: "
                              << msg.what());
        }
    }

    virtual Status setFromString(const std::string& str) {
        LDAPManager::get(getGlobalServiceContext())->setBindDN(str);
        return Status::OK();
    }
} ldapBindDNSetting;

class LDAPBindPasswordSetting : public ServerParameter {
public:
    LDAPBindPasswordSetting()
        : ServerParameter(ServerParameterSet::getGlobal(), "ldapQueryPassword", false, true) {}

    virtual void append(OperationContext* opCtx, BSONObjBuilder& b, const std::string& name) {
        b << name << "###";
    }

    virtual Status set(const BSONElement& newValueElement) {
        try {
            return setFromString(newValueElement.String());
        } catch (const DBException& msg) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream()
                              << "Invalid value for ldapQueryPassword via setParameter command: "
                              << newValueElement
                              << ", exception: "
                              << msg.what());
        }
    }

    virtual Status setFromString(const std::string& str) {
        LDAPManager::get(getGlobalServiceContext())->setBindPassword(SecureString(str.c_str()));
        return Status::OK();
    }
} ldapBindPasswordSetting;

class LDAPUserToDNMappingSetting : public ServerParameter {
public:
    LDAPUserToDNMappingSetting()
        : ServerParameter(ServerParameterSet::getGlobal(), "ldapUserToDNMapping", false, true) {}

    virtual void append(OperationContext* opCtx, BSONObjBuilder& b, const std::string& name) {
        b << name << LDAPManager::get(getGlobalServiceContext())->getUserToDNMapping();
    }

    virtual Status set(const BSONElement& newValueElement) {
        try {
            return setFromString(newValueElement.String());
        } catch (const DBException& msg) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream()
                              << "Invalid value for ldapUserToDNMapping via setParameter command: "
                              << newValueElement
                              << ", exception: "
                              << msg.what());
        }
    }

    virtual Status setFromString(const std::string& str) {
        auto swMapper = InternalToLDAPUserNameMapper::createNameMapper(str);
        if (swMapper.getStatus() != Status::OK()) {
            return swMapper.getStatus();
        }

        LDAPManager::get(getGlobalServiceContext())
            ->setUserNameMapper(std::move(swMapper.getValue()));
        return Status::OK();
    }
} ldapUserToDNMappingSetting;

class LDAPQueryTemplateSetting : public ServerParameter {
public:
    LDAPQueryTemplateSetting()
        : ServerParameter(ServerParameterSet::getGlobal(), "ldapAuthzQueryTemplate", false, true) {}

    virtual void append(OperationContext* opCtx, BSONObjBuilder& b, const std::string& name) {
        b << name << LDAPManager::get(getGlobalServiceContext())->getQueryTemplate();
    }

    virtual Status set(const BSONElement& newValueElement) {
        try {
            return setFromString(newValueElement.String());
        } catch (const DBException& msg) {
            return Status(
                ErrorCodes::BadValue,
                mongoutils::str::stream()
                    << "Invalid value for ldapAuthzQueryTemplate via setParameter command: "
                    << newValueElement
                    << ", exception: "
                    << msg.what());
        }
    }

    virtual Status setFromString(const std::string& str) {
        auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserName(str);
        if (swQueryParameters.getStatus() != Status::OK()) {
            return swQueryParameters.getStatus();
        }

        LDAPManager::get(getGlobalServiceContext())
            ->setQueryConfig(std::move(swQueryParameters.getValue()));
        return Status::OK();
    }
} ldapQueryTemplateSetting;
}
}
