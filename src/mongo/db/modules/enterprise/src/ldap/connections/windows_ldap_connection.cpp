/**
 *    Copyright (C) 2016 MongoDB Inc.
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "windows_ldap_connection.h"

// clang-format off
#include <ntldap.h>
#include <rpc.h>
#include <string>
#include <winldap.h>
#include <winber.h>  // winldap.h must be included before
// clang-format on

#include "ldap_connection_helpers.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/client/cyrus_sasl_client_session.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/text.h"
#include "../ldap_connection_options.h"
#include "../ldap_query.h"

namespace mongo {

namespace {
const std::wstring kLDAP(L"ldap://");
const std::wstring kLDAPS(L"ldaps://");
}

WindowsLDAPConnection::WindowsLDAPConnection(LDAPConnectionOptions options)
    : LDAPConnection(std::move(options)) {
    _timeoutSeconds = durationCount<Seconds>(_options.timeout);
}

WindowsLDAPConnection::~WindowsLDAPConnection() {
    Status status = disconnect();
    if (!status.isOK()) {
        error() << "LDAP unbind failed: " << status;
    }
}

Status WindowsLDAPConnection::_resultCodeToStatus(ULONG statusCode,
                                                  StringData functionName,
                                                  StringData failureHint) {
    if (statusCode == LDAP_SUCCESS) {
        return Status::OK();
    }

    WCHAR* errorString;
    ULONG errorWhileGettingError = ldap_get_option(_session, LDAP_OPT_ERROR_STRING, &errorString);
    if (errorWhileGettingError != LDAP_SUCCESS || !errorString) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Unable to get error string after native LDAP operation <"
                                    << functionName << ">, \"Failed to " << failureHint << "\". ("
                                    << errorWhileGettingError << "/"
                                    << toUtf8String(ldap_err2string(errorWhileGettingError))
                                    << ")");
    }

    return Status(ErrorCodes::OperationFailed,
                  str::stream() << "Native LDAP operation <" << functionName << "> \"Failed to "
                                << failureHint << "\". (" << statusCode << "/"
                                << toUtf8String(ldap_err2string(statusCode))
                                << "): " << toUtf8String(errorString));
}

Status WindowsLDAPConnection::_resultCodeToStatus(StringData functionName, StringData failureHint) {
    ULONG error;
    ULONG errorWhileGettingError = ldap_get_option(_session, LDAP_OPT_ERROR_NUMBER, &error);
    if (errorWhileGettingError != LDAP_SUCCESS) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Unable to get error code after native LDAP operation <"
                                    << functionName << ">. \"Failed to " << failureHint << "\". ("
                                    << errorWhileGettingError << "/"
                                    << toUtf8String(ldap_err2string(errorWhileGettingError))
                                    << ")");
    }

    return _resultCodeToStatus(error, functionName, failureHint);
}


Status WindowsLDAPConnection::connect() {
    // Allocate the underlying connection object
    std::wstring hostName = toWideString(_options.hostURIs.c_str());
    if (hostName.find(kLDAP) == 0) {
        hostName = hostName.substr(kLDAP.size());
        _session = ldap_initW(const_cast<wchar_t*>(hostName.c_str()), LDAP_PORT);
    } else if (hostName.find(kLDAPS) == 0) {
        hostName = hostName.substr(kLDAPS.size());
        _session = ldap_sslinitW(const_cast<wchar_t*>(hostName.c_str()), LDAP_SSL_PORT, 1);
    } else {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Couldn't parse LDAP URL: " << _options.hostURIs);
    }

    if (!_session) {
        Status status =
            _resultCodeToStatus(hostName.find(kLDAP) == 0 ? "ldap_initW" : "ldap_sslinitW",
                                "initialize LDAP session to server");
        if (!status.isOK()) {
            return status;
        }
        return Status(ErrorCodes::OperationFailed, "Failed to initialize LDAP session to server");
    }

    // Configure the connection object
    const ULONG version = LDAP_VERSION3;
    if (ldap_set_option(_session, LDAP_OPT_PROTOCOL_VERSION, (void*)&version) != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed, "Failed to upgrade LDAP protocol version");
    }

    if (ldap_set_option(_session, LDAP_OPT_TIMELIMIT, (void*)&_timeoutSeconds) != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed, "Failed to set LDAP timeout");
    }

    if (ldap_set_option(_session, LDAP_OPT_SEND_TIMEOUT, (void*)&_timeoutSeconds) != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed, "Failed to set LDAP network timeout");
    }

    // TODO: Microsoft doesn't support LDAP_OPT_REBIND_FN, which
    // we'd need to rebind to new servers on referals. We might
    // be able to make that work using LDAP_OPT_REFERRAL_CALLBACK.

    // Open the connection
    auto connectSuccess = ldap_connect(_session, NULL);
    return _resultCodeToStatus(connectSuccess, "ldap_connect", "connect to LDAP server");
}

Status WindowsLDAPConnection::bindAsUser(const LDAPBindOptions& options) {
    // Microsoft gives us two LDAP bind functions. ldap_bind_s lets us feed in a token representing
    // the auth scheme. ldap_sasl_bind_s lets us manually run a SASL dance. ldap_sasl_bind_s would
    // let us support some more schemes, and could do true SASL proxying. But it looks like this
    // method doesn't support all possible SASL mechanisms, and client-first mechanisms seem to fail
    // because we can't provide it empty client SASL credentials. We use ldap_bind_s, because it has
    // tigher integration with Windows. ldap_bind_s does not support async for mechanisms other than
    // simple.

    std::wstring user;
    std::wstring pwd;
    SEC_WINNT_AUTH_IDENTITY cred;
    if (!options.useLDAPConnectionDefaults) {
        user = toNativeString(options.bindDN.c_str());
        pwd = toNativeString(options.password->c_str());
        cred.Flags = SEC_WINNT_AUTH_IDENTITY_UNICODE;
        cred.User = reinterpret_cast<unsigned short*>(const_cast<wchar_t*>(user.c_str()));
        cred.UserLength = user.size();
        cred.Domain = nullptr;
        cred.DomainLength = 0;
        cred.Password = reinterpret_cast<unsigned short*>(const_cast<wchar_t*>(pwd.c_str()));
        cred.PasswordLength = pwd.size();
    }

    ULONG result;
    if (options.authenticationChoice == LDAPBindType::kSimple) {
        if (options.useLDAPConnectionDefaults) {
            return Status(ErrorCodes::IllegalOperation,
                          "Cannot use default username and password with simple LDAP bind");
        }

        result =
            ldap_bind_sW(_session,
                         const_cast<wchar_t*>(toNativeString(options.bindDN.c_str()).c_str()),
                         const_cast<wchar_t*>(toNativeString(options.password->c_str()).c_str()),
                         LDAP_AUTH_SIMPLE);

        return _resultCodeToStatus(result, "ldap_bind_sW", "to perform simple bind");
    } else if (options.authenticationChoice == LDAPBindType::kSasl &&
               options.saslMechanisms == "DIGEST-MD5") {
        if (options.useLDAPConnectionDefaults) {
            result = ldap_bind_sW(_session, nullptr, nullptr, LDAP_AUTH_DIGEST);

        } else {
            result =
                ldap_bind_sW(_session, nullptr, reinterpret_cast<PWCHAR>(&cred), LDAP_AUTH_DIGEST);
        }

        return _resultCodeToStatus(result, "ldap_bind_sW", "to perform DIGEST-MD5 SASL bind");
    } else if (options.authenticationChoice == LDAPBindType::kSasl &&
               options.saslMechanisms == "GSSAPI") {
        // Per Microsoft's documentation, this option "Sets or retrieves the preferred SASL binding
        // method prior to binding using the LDAP_AUTH_NEGOTIATE flag." But, this is not true, and
        // it causes LDAP_AUTH_DIGEST to use GSSAPI as well. Also, there doesn't seem to be a way to
        // turn this off after it's been set. This may cause problems with connection pooling.
        TCHAR* newSaslMethod = L"GSSAPI";
        if (ldap_set_option(_session, LDAP_OPT_SASL_METHOD, (void*)&newSaslMethod) !=
            LDAP_SUCCESS) {
            return Status(ErrorCodes::OperationFailed, "Failed to set SASL method");
        }

        if (options.useLDAPConnectionDefaults) {
            result = ldap_bind_sW(_session, nullptr, nullptr, LDAP_AUTH_NEGOTIATE);
        } else {
            result = ldap_bind_sW(
                _session, nullptr, reinterpret_cast<PWCHAR>(&cred), LDAP_AUTH_NEGOTIATE);
        }

        return _resultCodeToStatus(result, "ldap_bind_sW", "to perform GSSAPI SASL bind");
    }

    return Status(ErrorCodes::OperationFailed,
                  str::stream() << "Unknown bind operation requested: " << options.toCleanString());
}

StatusWith<LDAPEntityCollection> WindowsLDAPConnection::query(LDAPQuery query) {
    LOG(3) << "Performing native Windows LDAP query: " << query;

    // Convert the attribute vector to a mutable null terminated array of wchar_t* strings by
    // converting all query attributes to wchar_t, copying them to a vector, then obtaining the
    // underlying
    // data buffer through std::vector::data.
    size_t requestedAttributesSize = query.getAttributes().size();
    std::vector<wchar_t*> requestedAttributes;
    ON_BLOCK_EXIT([](std::vector<wchar_t*> reqAttr) {
        for (wchar_t* attribute : reqAttr) {
            delete[] attribute;
        }
    }, requestedAttributes);

    requestedAttributes.reserve(requestedAttributesSize + 1);
    for (size_t i = 0; i < requestedAttributesSize; ++i) {
        requestedAttributes.push_back(new wchar_t[query.getAttributes()[i].size() + 1]);

        memcpy(requestedAttributes.back(),
               toWideString(query.getAttributes()[i].c_str()).c_str(),
               sizeof(wchar_t) * (query.getAttributes()[i].size() + 1));
    }
    requestedAttributes.push_back(nullptr);

    LDAPMessage* result = nullptr;
    ON_BLOCK_EXIT(ldap_msgfree, result);
    l_timeval timeout{_timeoutSeconds, 0};

    // Note the const_cast here. The function takes consted PWSTR typedefs, which contains a
    // pointer. A consted typedef of a pointer makes the pointer constant, not what it points to.
    // This function thus takes a constant pointer to a mutable region of string memory.
    // This is fine, because the string returned by toWideString is a temporary.
    int err = ldap_search_ext_s(
        _session,
        query.getBaseDN().empty() ? NULL : const_cast<const PWSTR>(
                                               toWideString(query.getBaseDN().c_str()).c_str()),
        mapScopeToLDAP(query.getScope()),
        query.getFilter().empty() ? NULL : const_cast<const PWSTR>(
                                               toWideString(query.getFilter().c_str()).c_str()),
        requestedAttributes.data(),
        0,
        nullptr,
        nullptr,
        &timeout,
        0,
        &result);

    if (err != LDAP_SUCCESS) {
        std::string queryError = std::string("to perform LDAP query ") + query.toString();
        return _resultCodeToStatus(err, "ldap_search_ext_s", queryError);
    }

    // Process the data recieved by the query, and put into data structure for return
    // Parse each entity from the response
    LDAPMessage* entry = ldap_first_entry(_session, result);
    if (!entry) {
        Status status = _resultCodeToStatus("ldap_first_entry", "getting LDAP entry from results");
        if (!status.isOK()) {
            return status;
        }
        warning() << "LDAP query succeeded, but no results were returned.";
    }

    LDAPEntityCollection results;
    while (entry) {
        wchar_t* entryDN = ldap_get_dn(_session, entry);
        if (!entryDN) {
            error() << "Failed to get the DN for a result from the LDAP query.";
            return _resultCodeToStatus("ldap_get_dn", "getting DN for entry");
        }
        ON_BLOCK_EXIT(ldap_memfree, entryDN);

        LOG(3) << "From query result, got an entry with DN: " << toUtf8String(entryDN);
        LDAPAttributeKeyValuesMap attributeValueMap;

        // For each entity in the response, parse each attribute it contained
        BerElement* element = nullptr;
        wchar_t* attribute = ldap_first_attribute(_session, entry, &element);
        ON_BLOCK_EXIT([=](BerElement* element) { ber_free(element, 0); }, element);
        while (attribute) {
            ON_BLOCK_EXIT(ldap_memfree, attribute);
            LOG(3) << "From entry, got attribute " << toUtf8String(attribute);

            berval** values = ldap_get_values_len(_session, entry, attribute);
            if (values == nullptr) {
                error() << "LDAP query succeeeded, but failed to extract values for attribute";
                return _resultCodeToStatus("ldap_get_values_len", "getting values for attribute");
            }

            LDAPAttributeValues valueStore;
            ON_BLOCK_EXIT(ldap_value_free_len, values);

            // For each attribute contained by the entity, parse each value it contained.
            // Attributes in LDAP are multi-valued
            while (*values) {
                LDAPAttributeValue strValue((*values)->bv_val, (*values)->bv_len);
                valueStore.emplace_back(std::move(strValue));
                ++values;
            }

            attributeValueMap.emplace(
                LDAPAttributeKey(toUtf8String(std::wstring(attribute).c_str())),
                std::move(valueStore));
            attribute = ldap_next_attribute(_session, entry, element);
        }

        results.emplace(std::piecewise_construct,
                        std::forward_as_tuple(toUtf8String(entryDN)),
                        std::forward_as_tuple(attributeValueMap));
        entry = ldap_next_entry(_session, entry);
    }

    return results;
}

Status WindowsLDAPConnection::disconnect() {
    if (!_session) {
        return Status::OK();
    }

    return _resultCodeToStatus(ldap_unbind_s(_session), "ldap_unbind_s", "unbind from LDAP");
}
}
