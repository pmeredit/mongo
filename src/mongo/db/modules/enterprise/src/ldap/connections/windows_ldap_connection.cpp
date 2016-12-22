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
#include <type_traits>
#include <winldap.h>
#include <winber.h>  // winldap.h must be included before
// clang-format on

#include "../ldap_connection_options.h"
#include "../ldap_query.h"
#include "ldap_connection_helpers.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/client/cyrus_sasl_client_session.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/text.h"

namespace mongo {

namespace {

/**
 * This class is used as a template argument to instantiate an LDAPSessionHolder with all the
 * Windows specific types and functions necessary to open and interact with a session. This
 * is used to deduplicate code shared between RFC 1823 compliant LDAP APIs.
 */
class WindowsLDAPSessionParams {
public:
    using SessionType = LDAP;
    using MessageType = LDAPMessage;
    using BerElementType = BerElement;
    using BerValueType = BerValue;
    using ErrorCodeType = ULONG;
    using LibraryCharType = wchar_t;
    using TimeoutType = l_timeval;

    static const auto LDAP_success = LDAP_SUCCESS;
    static const auto LDAP_OPT_error_code = LDAP_OPT_ERROR_NUMBER;
    static const auto LDAP_OPT_error_string = LDAP_OPT_ERROR_STRING;

    static constexpr auto ldap_err2string = ::ldap_err2string;
    static constexpr auto ldap_get_option = ::ldap_get_option;
    static constexpr auto ldap_msgfree = ::ldap_msgfree;
    static constexpr auto ldap_memfree = ::ldap_memfree;
    static constexpr auto ldap_value_free_len = ::ldap_value_free_len;
    static constexpr auto ldap_count_entries = ::ldap_count_entries;
    static constexpr auto ldap_first_entry = ::ldap_first_entry;
    static constexpr auto ldap_get_values_len = ::ldap_get_values_len;
    static constexpr auto ldap_get_dn = ::ldap_get_dn;
    static constexpr auto ldap_first_attribute = ::ldap_first_attribute;
    static constexpr auto ber_free = ::ber_free;
    static constexpr auto ldap_search_ext_s = ::ldap_search_ext_s;

    static std::string toNativeString(const LibraryCharType* str) {
        return toUtf8String(str);
    }
    static std::wstring toLibraryString(const char* str) {
        return toWideString(str);
    }
};
}

class WindowsLDAPConnection::WindowsLDAPConnectionPIMPL
    : public LDAPSessionHolder<WindowsLDAPSessionParams> {};

WindowsLDAPConnection::WindowsLDAPConnection(LDAPConnectionOptions options)
    : LDAPConnection(std::move(options)), _pimpl(stdx::make_unique<WindowsLDAPConnectionPIMPL>()) {
    _timeoutSeconds = durationCount<Seconds>(_options.timeout);
}

WindowsLDAPConnection::~WindowsLDAPConnection() {
    Status status = disconnect();
    if (!status.isOK()) {
        error() << "LDAP unbind failed: " << status;
    }
}

Status WindowsLDAPConnection::connect() {
    // Allocate the underlying connection object
    _pimpl->getSession() = nullptr;

    std::wstring hosts = toWideString(StringSplitter::join(_options.hosts, " ").c_str());

    if (_options.transportSecurity == LDAPTransportSecurityType::kNone) {
        _pimpl->getSession() = ldap_initW(const_cast<wchar_t*>(hosts.c_str()), LDAP_PORT);
    } else if (_options.transportSecurity == LDAPTransportSecurityType::kTLS) {
        _pimpl->getSession() = ldap_sslinitW(const_cast<wchar_t*>(hosts.c_str()), LDAP_SSL_PORT, 1);
    } else {
        return Status(ErrorCodes::OperationFailed, "Unrecognized protocol security mechanism");
    }

    if (!_pimpl->getSession()) {
        // The ldap_*initW functions don't actually make a connection to the server until
        // ldap_connect is called. Failure here should not be gracefully recovered from.
        Status status = _pimpl->resultCodeToStatus(
            _options.transportSecurity == LDAPTransportSecurityType::kNone ? "ldap_initW"
                                                                           : "ldap_sslinitW",
            "initialize LDAP session to server");
        if (!status.isOK()) {
            return status;
        }
        return Status(ErrorCodes::OperationFailed, "Failed to initialize LDAP session to server");
    }

    // Configure the connection object
    const ULONG version = LDAP_VERSION3;
    if (ldap_set_option(_pimpl->getSession(), LDAP_OPT_PROTOCOL_VERSION, (void*)&version) !=
        LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed, "Failed to upgrade LDAP protocol version");
    }

    if (ldap_set_option(_pimpl->getSession(), LDAP_OPT_TIMELIMIT, (void*)&_timeoutSeconds) !=
        LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed, "Failed to set LDAP timeout");
    }

    if (ldap_set_option(_pimpl->getSession(), LDAP_OPT_SEND_TIMEOUT, (void*)&_timeoutSeconds) !=
        LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed, "Failed to set LDAP network timeout");
    }

    // TODO: Microsoft doesn't support LDAP_OPT_REBIND_FN, which
    // we'd need to rebind to new servers on referals. We might
    // be able to make that work using LDAP_OPT_REFERRAL_CALLBACK.

    // Open the connection
    auto connectSuccess = ldap_connect(_pimpl->getSession(), NULL);
    return _pimpl->resultCodeToStatus(connectSuccess, "ldap_connect", "connect to LDAP server");
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
            ldap_bind_sW(_pimpl->getSession(),
                         const_cast<wchar_t*>(toNativeString(options.bindDN.c_str()).c_str()),
                         const_cast<wchar_t*>(toNativeString(options.password->c_str()).c_str()),
                         LDAP_AUTH_SIMPLE);

        return _pimpl->resultCodeToStatus(result, "ldap_bind_sW", "to perform simple bind");
    } else if (options.authenticationChoice == LDAPBindType::kSasl &&
               options.saslMechanisms == "DIGEST-MD5") {
        if (options.useLDAPConnectionDefaults) {
            result = ldap_bind_sW(_pimpl->getSession(), nullptr, nullptr, LDAP_AUTH_DIGEST);

        } else {
            result = ldap_bind_sW(
                _pimpl->getSession(), nullptr, reinterpret_cast<PWCHAR>(&cred), LDAP_AUTH_DIGEST);
        }

        return _pimpl->resultCodeToStatus(
            result, "ldap_bind_sW", "to perform DIGEST-MD5 SASL bind");
    } else if (options.authenticationChoice == LDAPBindType::kSasl &&
               options.saslMechanisms == "GSSAPI") {
        // Per Microsoft's documentation, this option "Sets or retrieves the preferred SASL binding
        // method prior to binding using the LDAP_AUTH_NEGOTIATE flag." But, this is not true, and
        // it causes LDAP_AUTH_DIGEST to use GSSAPI as well. Also, there doesn't seem to be a way to
        // turn this off after it's been set. This may cause problems with connection pooling.
        auto newSaslMethod = const_cast<WCHAR*>(L"GSSAPI");
        if (ldap_set_option(_pimpl->getSession(), LDAP_OPT_SASL_METHOD, (void*)&newSaslMethod) !=
            LDAP_SUCCESS) {
            return Status(ErrorCodes::OperationFailed, "Failed to set SASL method");
        }

        if (options.useLDAPConnectionDefaults) {
            result = ldap_bind_sW(_pimpl->getSession(), nullptr, nullptr, LDAP_AUTH_NEGOTIATE);
        } else {
            result = ldap_bind_sW(_pimpl->getSession(),
                                  nullptr,
                                  reinterpret_cast<PWCHAR>(&cred),
                                  LDAP_AUTH_NEGOTIATE);
        }

        return _pimpl->resultCodeToStatus(result, "ldap_bind_sW", "to perform GSSAPI SASL bind");
    }

    return Status(ErrorCodes::OperationFailed,
                  str::stream() << "Unknown bind operation requested: " << options.toCleanString());
}

StatusWith<LDAPEntityCollection> WindowsLDAPConnection::query(LDAPQuery query) {
    l_timeval timeout{_timeoutSeconds, 0};
    return _pimpl->query(std::move(query), &timeout);
}

Status WindowsLDAPConnection::disconnect() {
    if (!_pimpl->getSession()) {
        return Status::OK();
    }

    return _pimpl->resultCodeToStatus(
        ldap_unbind_s(_pimpl->getSession()), "ldap_unbind_s", "unbind from LDAP");
}
}
