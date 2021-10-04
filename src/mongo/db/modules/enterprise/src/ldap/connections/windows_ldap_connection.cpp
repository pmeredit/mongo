/**
 *    Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "mongo/logv2/log.h"
#include "windows_ldap_connection.h"

// clang-format off
#include <ntldap.h>
#include <rpc.h>
#include <string>
#include <type_traits>
#include <winldap.h>
#include <winber.h>  // winldap.h must be included before
// clang-format on

#include <memory>

#include "../ldap_connection_options.h"
#include "../ldap_host.h"
#include "../ldap_query.h"
#include "ldap_connection_helpers.h"
#include "ldap_connection_reaper.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/client/cyrus_sasl_client_session.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"
#include "mongo/util/tick_source.h"

namespace mongo {

namespace {

// These failpoints are present in both OpenLDAPConnection and WindowsLDAPConnection to simulate
// timeouts during network operations.
MONGO_FAIL_POINT_DEFINE(ldapConnectionTimeoutHang);
MONGO_FAIL_POINT_DEFINE(ldapBindTimeoutHang);
MONGO_FAIL_POINT_DEFINE(ldapSearchTimeoutHang);
MONGO_FAIL_POINT_DEFINE(ldapLivenessCheckTimeoutHang);

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

    static constexpr bool kLDAP_OPT_ERROR_STRINGNeedsFree = false;

    static constexpr auto LDAP_success = LDAP_SUCCESS;
    static constexpr auto LDAP_insufficient_access = LDAP_INSUFFICIENT_RIGHTS;
    static constexpr auto LDAP_NO_SUCH_attribute = LDAP_NO_SUCH_ATTRIBUTE;
    static constexpr auto LDAP_NO_SUCH_object = LDAP_NO_SUCH_OBJECT;
    static constexpr auto LDAP_OPT_error_code = LDAP_OPT_ERROR_NUMBER;
    static constexpr auto LDAP_OPT_error_string = LDAP_OPT_ERROR_STRING;

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
}  // namespace

class WindowsLDAPConnection::WindowsLDAPConnectionPIMPL
    : public LDAPSessionHolder<WindowsLDAPSessionParams> {};

WindowsLDAPConnection::WindowsLDAPConnection(LDAPConnectionOptions options,
                                             std::shared_ptr<LDAPConnectionReaper> reaper,
                                             TickSource* tickSource,
                                             UserAcquisitionStats* userAcquisitionStats)
    : LDAPConnection(std::move(options)),
      _pimpl(std::make_unique<WindowsLDAPConnectionPIMPL>()),
      _reaper(std::move(reaper)),
      _tickSource(tickSource),
      _userAcquisitionStats(userAcquisitionStats) {
    _timeoutSeconds = durationCount<Seconds>(_connectionOptions.timeout);
}

WindowsLDAPConnection::~WindowsLDAPConnection() {
    Status status = disconnect(_tickSource, _userAcquisitionStats);
    if (!status.isOK()) {
        LOGV2_ERROR(24256, "LDAP unbind failed: {status}", "status"_attr = status);
    }
}

Status WindowsLDAPConnection::connect() {
    // Allocate the underlying connection object
    _pimpl->getSession() = nullptr;

    std::wstring hosts = toWideString(joinLdapHostAndPort(_connectionOptions.hosts, ' ').c_str());

    if (_connectionOptions.hosts[0].isSSL()) {
        _pimpl->getSession() = ldap_sslinitW(const_cast<wchar_t*>(hosts.c_str()), LDAP_SSL_PORT, 1);
    } else {
        _pimpl->getSession() = ldap_initW(const_cast<wchar_t*>(hosts.c_str()), LDAP_PORT);
    }

    if (!_pimpl->getSession()) {
        // The ldap_*initW functions don't actually make a connection to the server until
        // ldap_connect is called. Failure here should not be gracefully recovered from.
        Status status = _pimpl->resultCodeToStatus(
            _connectionOptions.hosts[0].isSSL() ? "ldap_sslinitW" : "ldap_initW",
            "initialize LDAP session to a server");

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

    // We use querying the root DSE to check server health, so we need to disable cacheing this
    // info.
    if (ldap_set_option(_pimpl->getSession(), LDAP_OPT_ROOTDSE_CACHE, LDAP_OPT_OFF) !=
        LDAP_SUCCESS) {
        return {ErrorCodes::OperationFailed, "Failed to disable root DSE cache"};
    }

    // TODO: Microsoft doesn't support LDAP_OPT_REBIND_FN, which
    // we'd need to rebind to new servers on referals. We might
    // be able to make that work using LDAP_OPT_REFERRAL_CALLBACK.

    // If ldapConnectionTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in the
    // network call.
    ldapConnectionTimeoutHang.execute(
        [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

    // Open the connection
    auto connectSuccess = ldap_connect(_pimpl->getSession(), nullptr);
    return _pimpl->resultCodeToStatus(connectSuccess, "ldap_connect", "connect to LDAP server");
}

Status WindowsLDAPConnection::bindAsUser(const LDAPBindOptions& options,
                                         TickSource* tickSource,
                                         UserAcquisitionStats* userAcquisitionStats) {
    // Microsoft gives us two LDAP bind functions. ldap_bind_s lets us feed in a token representing
    // the auth scheme. ldap_sasl_bind_s lets us manually run a SASL dance. ldap_sasl_bind_s would
    // let us support some more schemes, and could do true SASL proxying. But it looks like this
    // method doesn't support all possible SASL mechanisms, and client-first mechanisms seem to fail
    // because we can't provide it empty client SASL credentials. We use ldap_bind_s, because it has
    // tigher integration with Windows. ldap_bind_s does not support async for mechanisms other than
    // simple.

    UserAcquisitionStatsHandle userAcquisitionStatsHandle =
        UserAcquisitionStatsHandle(userAcquisitionStats, tickSource, kBind);

    // Store the bindOptions in the private member field to protect against cases where the argument
    // goes out of scope.
    _bindOptions = options;

    std::wstring user;
    std::wstring pwd;
    SEC_WINNT_AUTH_IDENTITY cred;
    if (!_bindOptions->useLDAPConnectionDefaults) {
        user = toNativeString(_bindOptions->bindDN.c_str());
        pwd = toNativeString(_bindOptions->password->c_str());
        cred.Flags = SEC_WINNT_AUTH_IDENTITY_UNICODE;
        cred.User = reinterpret_cast<unsigned short*>(const_cast<wchar_t*>(user.c_str()));
        cred.UserLength = user.size();
        cred.Domain = nullptr;
        cred.DomainLength = 0;
        cred.Password = reinterpret_cast<unsigned short*>(const_cast<wchar_t*>(pwd.c_str()));
        cred.PasswordLength = pwd.size();
    }

    ULONG result;
    Status resultStatus(ErrorCodes::OperationFailed,
                        str::stream() << "Unknown bind operation requested: "
                                      << _bindOptions->toCleanString());

    if (_bindOptions->authenticationChoice == LDAPBindType::kSimple) {
        if (_bindOptions->useLDAPConnectionDefaults) {
            return Status(ErrorCodes::IllegalOperation,
                          "Cannot use default username and password with simple LDAP bind");
        }

        // If ldapBindTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in the
        // network call.
        ldapBindTimeoutHang.execute(
            [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

        result = ldap_bind_sW(
            _pimpl->getSession(),
            const_cast<wchar_t*>(toNativeString(_bindOptions->bindDN.c_str()).c_str()),
            const_cast<wchar_t*>(toNativeString(_bindOptions->password->c_str()).c_str()),
            LDAP_AUTH_SIMPLE);

        resultStatus = _pimpl->resultCodeToStatus(result, "ldap_bind_sW", "to perform simple bind");
    } else if (_bindOptions->authenticationChoice == LDAPBindType::kSasl &&
               _bindOptions->saslMechanisms == "DIGEST-MD5") {
        if (_bindOptions->useLDAPConnectionDefaults) {
            // If ldapBindTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in
            // the network call.
            ldapBindTimeoutHang.execute(
                [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });
            result = ldap_bind_sW(_pimpl->getSession(), nullptr, nullptr, LDAP_AUTH_DIGEST);

        } else {
            // If ldapBindTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in
            // the network call.
            ldapBindTimeoutHang.execute(
                [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });
            result = ldap_bind_sW(
                _pimpl->getSession(), nullptr, reinterpret_cast<PWCHAR>(&cred), LDAP_AUTH_DIGEST);
        }

        resultStatus =
            _pimpl->resultCodeToStatus(result, "ldap_bind_sW", "to perform DIGEST-MD5 SASL bind");
    } else if (_bindOptions->authenticationChoice == LDAPBindType::kSasl &&
               _bindOptions->saslMechanisms == "GSSAPI") {
        // Per Microsoft's documentation, this option "Sets or retrieves the preferred SASL binding
        // method prior to binding using the LDAP_AUTH_NEGOTIATE flag." But, this is not true, and
        // it causes LDAP_AUTH_DIGEST to use GSSAPI as well. Also, there doesn't seem to be a way to
        // turn this off after it's been set. This may cause problems with connection pooling.
        auto newSaslMethod = const_cast<WCHAR*>(L"GSSAPI");
        if (ldap_set_option(_pimpl->getSession(), LDAP_OPT_SASL_METHOD, (void*)&newSaslMethod) !=
            LDAP_SUCCESS) {
            return Status(ErrorCodes::OperationFailed, "Failed to set SASL method");
        }

        if (_bindOptions->useLDAPConnectionDefaults) {
            // If ldapBindTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in
            // the network call.
            ldapBindTimeoutHang.execute(
                [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });
            result = ldap_bind_sW(_pimpl->getSession(), nullptr, nullptr, LDAP_AUTH_NEGOTIATE);
        } else {
            // If ldapBindTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in
            // the network call.
            ldapBindTimeoutHang.execute(
                [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });
            result = ldap_bind_sW(_pimpl->getSession(),
                                  nullptr,
                                  reinterpret_cast<PWCHAR>(&cred),
                                  LDAP_AUTH_NEGOTIATE);
        }

        resultStatus =
            _pimpl->resultCodeToStatus(result, "ldap_bind_sW", "to perform GSSAPI SASL bind");
    }

    if (resultStatus.isOK()) {
        _boundUser = _bindOptions->bindDN;
    }

    return resultStatus;
}

boost::optional<std::string> WindowsLDAPConnection::currentBoundUser() const {
    return _boundUser;
}

Status WindowsLDAPConnection::checkLiveness(TickSource* tickSource,
                                            UserAcquisitionStats* userAcquisitionStats) {
    l_timeval timeout{static_cast<LONG>(_timeoutSeconds), 0};

    // If ldapLivenessCheckTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in
    // the network call.
    ldapLivenessCheckTimeoutHang.execute(
        [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

    return _pimpl->checkLiveness(&timeout, tickSource, userAcquisitionStats);
}

StatusWith<LDAPEntityCollection> WindowsLDAPConnection::query(
    LDAPQuery query, TickSource* tickSource, UserAcquisitionStats* userAcquisitionStats) {
    l_timeval timeout{static_cast<LONG>(_timeoutSeconds), 0};

    // If ldapSearchTimeoutHang is set, then sleep for "delay" seconds to simulate a hang in the
    // network call.
    ldapSearchTimeoutHang.execute(
        [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

    return _pimpl->query(std::move(query), &timeout, tickSource, userAcquisitionStats);
}

Status WindowsLDAPConnection::disconnect(TickSource* tickSource,
                                         UserAcquisitionStats* userAcquisitionStats) {
    if (!_pimpl->getSession()) {
        return Status::OK();
    }

    _reaper->reap(_pimpl->getSession(), tickSource);

    _pimpl->getSession() = nullptr;

    return Status::OK();
}

void disconnectLDAPConnection(LDAP* ldap, TickSource* tickSource) {
    LDAPSessionHolder<WindowsLDAPSessionParams> session(ldap);
    UserAcquisitionStats userAcquisitionStats = UserAcquisitionStats();
    UserAcquisitionStatsHandle userAcquisitionStatsHandle =
        UserAcquisitionStatsHandle(&userAcquisitionStats, tickSource, kUnbind);
    auto status =
        session.resultCodeToStatus(ldap_unbind_s(ldap), "ldap_unbind_s", "unbind from LDAP");
    if (!status.isOK()) {
        LOGV2_ERROR(5531601, "Unable to unbind from LDAP", "__error__"_attr = status);
    }
    userAcquisitionStatsHandle.recordTimerEnd();
}

}  // namespace mongo
