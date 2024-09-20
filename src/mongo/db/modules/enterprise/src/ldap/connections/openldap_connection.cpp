/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/config.h"
#include "mongo/platform/basic.h"

#include "openldap_connection.h"

#include <boost/algorithm/string/join.hpp>
#include <lber.h>
#include <ldap.h>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#if MONGO_CONFIG_SSL_PROVIDER == MONGO_CONFIG_SSL_PROVIDER_APPLE
#define OPENSSL_VERSION_NUMBER 0
#define OPENSSL_VERSION_TEXT "0.0.0"
#else
#include <openssl/opensslv.h>
#endif  // MONGO_CONFIG_SSL_PROVIDER == MONGO_CONFIG_SSL_PROVIDER_APPLE
#include <sasl/sasl.h>
#include <utility>

#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/net/sockaddr.h"
#include "mongo/util/str.h"
#include "mongo/util/tick_source.h"
#include "mongo/util/tick_source_mock.h"
#include "mongo/util/time_support.h"

#include "../ldap_connection_options.h"
#include "../ldap_options.h"
#include "../ldap_query.h"
#include "ldap/ldap_parameters_gen.h"
#include "ldap_connection_helpers.h"
#include "ldap_connection_reaper.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {
namespace {

// These failpoints are present in both OpenLDAPConnection and WindowsLDAPConnection to simulate
// timeouts during network operations.
MONGO_FAIL_POINT_DEFINE(disableNativeLDAPTimeout);
MONGO_FAIL_POINT_DEFINE(ldapConnectionDelay);
MONGO_FAIL_POINT_DEFINE(ldapBindDelay);
MONGO_FAIL_POINT_DEFINE(ldapSearchDelay);
MONGO_FAIL_POINT_DEFINE(ldapLivenessCheckDelay);
MONGO_FAIL_POINT_DEFINE(ldapNetworkTimeoutOnBind);
MONGO_FAIL_POINT_DEFINE(ldapNetworkTimeoutOnQuery);
MONGO_FAIL_POINT_DEFINE(ldapBindTimeoutHangIndefinitely);
MONGO_FAIL_POINT_DEFINE(ldapReferralFail);


/**
 * This class is used as a template argument to instantiate an LDAPSessionHolder with all the
 * OpenLDAP specific types and functions necessary to open and interact with a session. This
 * is used to deduplicate code shared between RFC 1823 compliant LDAP APIs.
 */
class OpenLDAPSessionParams {
public:
    using SessionType = LDAP;
    using MessageType = LDAPMessage;
    using BerElementType = BerElement;
    using BerValueType = BerValue;
    using ErrorCodeType = int;
    using LibraryCharType = char;
    using TimeoutType = struct timeval;

    static constexpr bool kLDAP_OPT_ERROR_STRINGNeedsFree = true;

    static constexpr auto LDAP_success = LDAP_SUCCESS;
    static constexpr auto LDAP_NO_SUCH_attribute = LDAP_NO_SUCH_ATTRIBUTE;
    static constexpr auto LDAP_NO_SUCH_object = LDAP_NO_SUCH_OBJECT;
    static constexpr auto LDAP_OPT_error_code = LDAP_OPT_RESULT_CODE;
    static constexpr auto LDAP_OPT_error_string = LDAP_OPT_ERROR_STRING;
    static constexpr auto LDAP_down = LDAP_SERVER_DOWN;
    static constexpr auto LDAP_timeout = LDAP_TIMEOUT;

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

    static bool isSecurityError(ErrorCodeType code) {
        return LDAP_SECURITY_ERROR(code);
    }

    static std::string toNativeString(const LibraryCharType* str) {
        return std::string(str);
    }
    static std::string toLibraryString(std::string str) {
        return str;
    }
};

/**
 * Protects OpenLDAP's global state.
 * libldap functions manipulating session structures are threadsafe so long as they are
 * only accessible from one thread at a time. However, functions manipulating library
 * state, like those which create new sessions, are not threadsafe. Therefore, we must
 * wrap session creation with a mutex.
 */
static stdx::mutex libldapGlobalMutex;

/**
 * Locking OpenLDAPGlobalMutex locks a global mutex if setNeedsGlobalLock was called. Otherwise,
 * it is a no-op. This is intended to synchronize access to libldap, under known thread-unsafe
 * conditions.
 */
class OpenLDAPGlobalMutex {
public:
    void lock();
    void unlock();

private:
    bool _bypassLock = false;
    std::once_flag _init;
    void initLockType();
};

static OpenLDAPGlobalMutex conditionalMutex;

/**
 * This C-like callback function is used to acquire SASL authentication parameters.
 * Authentication will almost always require some additional information, like the
 * username and password we wish to authenticate with. Cyrus SASL, the library that
 * libldap uses to perform SASL authentication, allows us to provide it a function
 * which it will call when it needs a piece of information that it doesn't have.
 *
 * @param session The LDAP session performing the authentication.
 * @param flags SASL flags which are supposed to tell us if we're supposed to open a visual prompt
 *              for information. As we're a daemon, we should probably ignore this.
 * @param defaults An LDAPBindOptions* containing all the information we've provided for binding.
 * @param interact A sasl_interact_t* containing a series of entries consisting of an enum
 *                 describing what Cyrus SASL is asking for, a pointer we can set to point to
 *                 the address of our answer, and a size we must set to be the size of our answer.
 */
int saslInteract(LDAP* session, unsigned flags, void* defaults, void* interact) {
    try {
        sasl_interact_t* request = static_cast<sasl_interact_t*>(interact);
        auto* conn = static_cast<OpenLDAPConnection*>(defaults);
        invariant(conn->bindOptions());

        while (request && request->id != SASL_CB_LIST_END) {
            switch (request->id) {
                case SASL_CB_AUTHNAME:
                    request->result = conn->bindOptions()->bindDN.c_str();
                    request->len = conn->bindOptions()->bindDN.size();
                    break;
                case SASL_CB_PASS:
                    request->result = conn->bindOptions()->password->c_str();
                    request->len = conn->bindOptions()->password->size();
                    break;
                default:
                    break;
            }
            ++request;
        }

        return LDAP_SUCCESS;
    } catch (...) {
        LOGV2_ERROR(24053,
                    "Unexpected exception caught in OpenLDAPConnection's saslInteract: {status}",
                    "status"_attr = exceptionToStatus());
        return LDAP_OPERATIONS_ERROR;
    }
}

/**
 * This C-like function binds to the LDAP server located at the url.
 * We call this function when initially binding to an LDAP server.
 * Additionally, when querying an LDAP server, we will sometimes be referred to another server.
 * This function is passed to ldap_set_rebind_proc, so that libldap knows to call it again against
 * servers we've been refered to.
 */
std::tuple<Status, int> openLDAPBindFunction(
    LDAP* session, LDAP_CONST char* url, ber_tag_t request, ber_int_t msgid, void* params) {
    try {
        auto* conn = static_cast<OpenLDAPConnection*>(params);
        LDAPSessionHolder<OpenLDAPSessionParams> sessionHolder(session, conn->getId());
        const auto& bindOptions = conn->bindOptions();
        invariant(bindOptions);

        // Log the peer address if the connection has already been used; otherwise, this is still
        // unknown and will be logged by the TCP connection callback.
        logv2::DynamicAttributes attrs;
        attrs.add("ldapSessionId", conn->getId());
        attrs.add("ldapURL", url);
        attrs.addDeepCopy("bindOptions", bindOptions->toCleanString());

        const auto& peerAddr = conn->getPeerSockAddr().getAddr();
        if (peerAddr.empty() || peerAddr == "(NONE)") {
            attrs.add("peerAddr", "<unknown>");
        } else {
            attrs.add("peerAddr", peerAddr);
        }

        LOGV2_DEBUG(24050, 1, "Binding to LDAP server", attrs);

        int ret;
        const std::string failureHint = str::stream() << "failed to bind to LDAP server at " << url;
        Status status{ErrorCodes::InternalError, "Status was not set before use"_sd};

        if (bindOptions->authenticationChoice == LDAPBindType::kSasl) {
            ret = ldap_sasl_interactive_bind_s(session,
                                               nullptr,
                                               bindOptions->saslMechanisms.c_str(),
                                               nullptr,
                                               nullptr,
                                               LDAP_SASL_QUIET,
                                               &saslInteract,
                                               conn);
            status =
                sessionHolder.resultCodeToStatus(ret, "ldap_sasl_interactive_bind_s", failureHint);
        } else if (bindOptions->authenticationChoice == LDAPBindType::kSimple) {
            // Unfortunately, libldap wants a non-const password. Copy the password to remove risk
            // of it scribbling over our memory.
            SecureVector<char> passwordCopy(bindOptions->password->begin(),
                                            bindOptions->password->end());
            berval passwd;
            passwd.bv_val = passwordCopy->data();
            passwd.bv_len = passwordCopy->size();

            ret = ldap_sasl_bind_s(session,
                                   bindOptions->bindDN.c_str(),
                                   LDAP_SASL_SIMPLE,
                                   &passwd,
                                   nullptr,
                                   nullptr,
                                   nullptr);
            status = sessionHolder.resultCodeToStatus(ret, "ldap_sasl_bind_s", failureHint);
        } else {
            LOGV2_ERROR(24054,
                        "Attempted to bind to LDAP server with unrecognized bind type.",
                        "ldapSessionId"_attr = conn->getId(),
                        "unrecognizedBindType"_attr =
                            authenticationChoiceToString(bindOptions->authenticationChoice),
                        "peerAddr"_attr = conn->getPeerSockAddr());
            return std::make_tuple(Status(ErrorCodes::OperationFailed, "Unrecognized bind type"),
                                   LDAP_OPERATIONS_ERROR);
        }

        if (status.isOK()) {
            LOGV2_DEBUG(9297100, 1, "LDAP bind succeeded", attrs);
        } else {
            LOGV2_ERROR(24055,
                        "Failed to bind to LDAP",
                        "ldapSessionId"_attr = conn->getId(),
                        "ldapURL"_attr = url,
                        "status"_attr = status,
                        "bindOptions"_attr = bindOptions->toCleanString(),
                        "peerAddr"_attr = conn->getPeerSockAddr());
        }
        return std::make_tuple(status, ret);
    } catch (...) {
        Status status = exceptionToStatus();
        auto* conn = static_cast<OpenLDAPConnection*>(params);
        LOGV2_ERROR(24056,
                    "Failed to bind to LDAP server",
                    "ldapSessionId"_attr = conn->getId(),
                    "ldapURL"_attr = url,
                    "status"_attr = status,
                    "peerAddr"_attr = conn->getPeerSockAddr());
        return std::make_tuple(Status(ErrorCodes::OperationFailed, "Failed to bind to LDAP server"),
                               LDAP_OPERATIONS_ERROR);
    }
}

int openLDAPRebindFunction(
    LDAP* session, LDAP_CONST char* url, ber_tag_t request, ber_int_t msgid, void* params) {
    const auto* conn = static_cast<OpenLDAPConnection*>(params);
    const auto& rebindCallbackParameters = conn->getRebindCallbackParameters();
    invariant(rebindCallbackParameters);

    LOGV2_DEBUG(7915600,
                1,
                "Binding to LDAP server when chasing referral",
                "ldapSessionId"_attr = conn->getId(),
                "ldapURL"_attr = url,
                "peerAddr"_attr = conn->getPeerSockAddr(),
                "bindOptions"_attr = conn->bindOptions()->toCleanString());

    auto [status, code] = openLDAPBindFunction(session, url, request, msgid, params);
    if (ldapReferralFail.shouldFail() || !status.isOK()) {
        UserAcquisitionStatsHandle userAcquisitionStatsHandle =
            UserAcquisitionStatsHandle(rebindCallbackParameters->referralUserAcquisitionStats.get(),
                                       rebindCallbackParameters->tickSource,
                                       kFailedReferral);
        LOGV2_ERROR(7915601,
                    "Could not bind to LDAP server when chasing referral",
                    "ldapSessionId"_attr = conn->getId(),
                    "ldapURL"_attr = url,
                    "status"_attr = status,
                    "peerAddr"_attr = static_cast<OpenLDAPConnection*>(params)->getPeerSockAddr(),
                    "bindOptions"_attr =
                        static_cast<OpenLDAPConnection*>(params)->bindOptions()->toCleanString());

        // If the failpoint is set, override the code to an error even if it succeeded.
        if (ldapReferralFail.shouldFail()) {
            code = -1;
        }
    } else {
        UserAcquisitionStatsHandle userAcquisitionStatsHandle =
            UserAcquisitionStatsHandle(rebindCallbackParameters->referralUserAcquisitionStats.get(),
                                       rebindCallbackParameters->tickSource,
                                       kSuccessfulReferral);
        LOGV2_DEBUG(7915602,
                    1,
                    "Successfully rebound to LDAP server when chasing referral",
                    "ldapSessionId"_attr = conn->getId(),
                    "ldapURL"_attr = url,
                    "peerAddr"_attr = static_cast<OpenLDAPConnection*>(params)->getPeerSockAddr(),
                    "bindOptions"_attr =
                        static_cast<OpenLDAPConnection*>(params)->bindOptions()->toCleanString());
    }
    return code;
}

// Spec for returning a general LDAP_OPT.
template <int optNum, typename T, T def>
class LDAPOptionGeneric {
public:
    using Type = T;
    static constexpr int Code = optNum;

    LDAPOptionGeneric(T val) : _val(val) {}

    T getValue() const {
        return _val;
    }

    static T makeDefaultInParam() {
        return def;
    }

private:
    T _val;
};

// Spec for returning a string LDAP_OPT.
template <int optNum>
class LDAPOptionString : public LDAPOptionGeneric<optNum, char*, nullptr> {
public:
    LDAPOptionString(char* val) : LDAPOptionGeneric<optNum, char*, nullptr>(val) {}
    ~LDAPOptionString() {
        char* strval = this->getValue();
        if (nullptr != strval) {
            OpenLDAPSessionParams::ldap_memfree(strval);
        }
    }
};

// Spec for how to process a request for API version information from ldap_get_option.
class LDAPOptionAPIInfo {
public:
    using Type = LDAPAPIInfo;
    constexpr static auto Code = LDAP_OPT_API_INFO;

    std::string getVendorName() const {
        return vendorName;
    }

    auto getVendorVersion() const {
        return vendorVersion;
    }

    const std::vector<std::string>& getExtensions() const& {
        return extensions;
    }
    const std::vector<std::string>& getExtensions() && = delete;

    auto getInfoVersion() const {
        return infoVersion;
    }

    auto getAPIVersion() const {
        return apiVersion;
    }

    auto getProtocolVersion() const {
        return protocolVersion;
    }

    static auto makeDefaultInParam() {
        Type info{};
        info.ldapai_info_version = 1;
        return info;
    };

    friend LDAPSessionHolder<OpenLDAPSessionParams>;

private:
    // LDAP_OPT_API populates a LDAPAPIInfo struct. The caller is responsible for
    // ldap_memfreeing ldapai_vendor_name, each element in ldapai_extensions, and
    // ldapai_extensions itself.
    struct FreeLDAPAPIInfoFunctor {
        explicit FreeLDAPAPIInfoFunctor() noexcept = default;
        void operator()(LDAPAPIInfo* info) noexcept {
            ldap_memfree(info->ldapai_vendor_name);

            std::for_each(LDAPArrayIterator<char*>(info->ldapai_extensions),
                          LDAPArrayIterator<char*>(),
                          &ldap_memfree);
            ldap_memfree(info->ldapai_extensions);
        }
    };

    // Takes ownership of all of info's fields.
    LDAPOptionAPIInfo(std::unique_ptr<LDAPAPIInfo, FreeLDAPAPIInfoFunctor> info) {
        infoVersion = info->ldapai_info_version;
        vendorName = OpenLDAPSessionParams::toNativeString(info->ldapai_vendor_name);
        std::transform(LDAPArrayIterator<char*>(info->ldapai_extensions),
                       LDAPArrayIterator<char*>(),
                       std::back_inserter(extensions),
                       OpenLDAPSessionParams::toNativeString);
        vendorVersion = info->ldapai_vendor_version;
        apiVersion = info->ldapai_api_version;
        protocolVersion = info->ldapai_protocol_version;
    }

    // Takes ownership of all of info's fields.
    LDAPOptionAPIInfo(LDAPAPIInfo& info)
        : LDAPOptionAPIInfo(std::unique_ptr<LDAPAPIInfo, FreeLDAPAPIInfoFunctor>(&info)) {}

    decltype(LDAPAPIInfo::ldapai_info_version) infoVersion;
    std::string vendorName;
    decltype(LDAPAPIInfo::ldapai_vendor_version) vendorVersion;
    std::vector<std::string> extensions;
    decltype(LDAPAPIInfo::ldapai_api_version) apiVersion;
    decltype(LDAPAPIInfo::ldapai_protocol_version) protocolVersion;
};

SockAddr inferSockAddr(const struct sockaddr* addr) {
    if (addr->sa_family == AF_INET) {
        return SockAddr(addr, sizeof(sockaddr_in));
    }
    if (addr->sa_family == AF_INET6) {
        return SockAddr(addr, sizeof(sockaddr_in6));
    }
    uasserted(31233,
              str::stream() << "Socket Address family is not IPv4 or IPv6: " << addr->sa_family);
}

void LDAPDebugCallbackFunction(const char* msg) {
    LOGV2_DEBUG(7997801, 1, "libldap log message", "msg"_attr = msg);
}

int LDAPTLSConnectCallbackFunction(LDAP* ld, void* ssl, void* sslCtx, void* args) {
    auto* conn = reinterpret_cast<OpenLDAPConnection*>(args);

    LOGV2_DEBUG(7997802,
                1,
                "Establishing TLS connection to LDAP server",
                "ldapSessionId"_attr = conn->getId(),
                "serverAddress"_attr = conn->getPeerSockAddr(),
                "tlsPackage"_attr = conn->getTraits().tlsPackage);
    return 0;
}

}  // namespace

class OpenLDAPConnection::OpenLDAPConnectionPIMPL
    : public LDAPSessionHolder<OpenLDAPSessionParams> {
public:
    static int LDAPConnectCallbackFunction(LDAP* ld,
                                           Sockbuf* sb,
                                           LDAPURLDesc* srv,
                                           struct sockaddr* addr,
                                           struct ldap_conncb* ctx) try {
        auto* this_ = reinterpret_cast<OpenLDAPConnection::OpenLDAPConnectionPIMPL*>(ctx->lc_arg);
        this_->_peer = inferSockAddr(addr);
        LOGV2_DEBUG(20163,
                    1,
                    "Established TCP connection to LDAP server",
                    "ldapSessionId"_attr = this_->getId(),
                    "serverAddress"_attr = this_->_peer,
                    "ldapURL"_attr = std::unique_ptr<char, decltype(&ldap_memfree)>(
                                         ldap_url_desc2str(srv), &ldap_memfree)
                                         .get());
        return 0;
    } catch (const std::exception& e) {
        LOGV2_ERROR(24057, "Failed LDAPConnectCallback with: {reason}", "reason"_attr = e.what());
        return LDAP_OPERATIONS_ERROR;
    }

    static void LDAPConnectCallbackDelete(LDAP* ld, Sockbuf* sb, struct ldap_conncb* ctx) {}

    struct ldap_conncb getTCPConnectionCallbacks() {
        return {&LDAPConnectCallbackFunction, &LDAPConnectCallbackDelete, this};
    }

    SockAddr _peer;
};

OpenLDAPConnection::ProviderTraits OpenLDAPConnection::_traits;

OpenLDAPConnection::OpenLDAPConnection(LDAPConnectionOptions options,
                                       std::shared_ptr<LDAPConnectionReaper> reaper)
    : LDAPConnection(std::move(options)),
      _pimpl(std::make_unique<OpenLDAPConnectionPIMPL>()),
      _reaper(std::move(reaper)),
      _tcpConnectionCallback(_pimpl->getTCPConnectionCallbacks()) {
    LOGV2_DEBUG(9339700, 3, "Creating OpenLDAPConnection", "ldapSessionId"_attr = _pimpl->getId());
    initTraits();

    // If the disableLDAPNativeTimeout failpoint is set, then reset _connectionOptions.timeout to
    // the value provided in the failpoint.
    disableNativeLDAPTimeout.execute([&](const BSONObj& data) {
        _connectionOptions.timeout = Milliseconds(data["delay"].numberInt() * 1000);
    });
    Seconds seconds = duration_cast<Seconds>(_connectionOptions.timeout);
    _timeout.tv_sec = seconds.count();
    _timeLimitInt = static_cast<int>(_timeout.tv_sec);
    _timeout.tv_usec = durationCount<Microseconds>(_connectionOptions.timeout - seconds);
}

OpenLDAPConnection::~OpenLDAPConnection() {
    Status status = disconnect();
    if (!status.isOK()) {
        LOGV2_ERROR(24058,
                    "LDAP unbind failed: {status}",
                    "ldapSessionId"_attr = getId(),
                    "status"_attr = status);
    }
}

SockAddr OpenLDAPConnection::getPeerSockAddr() const {
    return _pimpl->_peer;
}

void OpenLDAPConnection::initTraits() {
    static std::once_flag once;
    std::call_once(once, [] {
        auto& traits = OpenLDAPConnection::_traits;
        try {
            auto pimpl = std::make_unique<OpenLDAPConnectionPIMPL>();

            LDAPOptionAPIInfo info = pimpl->getOption<LDAPOptionAPIInfo>(
                "OpenLDAPConnection::connect", "Getting API info");
            const auto& ext = info.getExtensions();

            traits.slowLocking = OPENSSL_VERSION_NUMBER < 0x10101000L;
            traits.poolingSafe = true;
            traits.mozNSSCompat = false;
            traits.threadSafe = std::any_of(
                ext.begin(), ext.end(), [](std::string s) { return s == "THREAD_SAFE"; });

            BSONObjBuilder optionsBuilder;
            optionsBuilder.append("slowLocking", traits.slowLocking);

            using LDAPOptionConnectAsync = LDAPOptionGeneric<LDAP_OPT_CONNECT_ASYNC, int, 0>;
            const auto async = pimpl->getOption<LDAPOptionConnectAsync>(
                "OpenLDAPConnection::initTraits", "Getting value of async");
            optionsBuilder.append("async", async.getValue());

            using LDAPOptionTLSPackage = LDAPOptionString<LDAP_OPT_X_TLS_PACKAGE>;
            const auto tlsPackage = pimpl->getOption<LDAPOptionTLSPackage>(
                "OpenLDAPConnection::initTraits", "Getting TLS package name");
            if (nullptr != tlsPackage.getValue()) {
                optionsBuilder.append("tlsPackage", tlsPackage.getValue());
                traits.tlsPackage = tlsPackage.getValue();
            }

#ifdef LDAP_OPT_X_TLS_MOZNSS_COMPATIBILITY
            using LDAPOptionMozNSSCpt =
                LDAPOptionGeneric<LDAP_OPT_X_TLS_MOZNSS_COMPATIBILITY, int, 0>;
            auto mozNSSCompatValue = pimpl->getOption<LDAPOptionMozNSSCpt>(
                "OpenLDAPConnection::initTraits", "Getting value of MozNSS compat");
            traits.mozNSSCompat = mozNSSCompatValue.getValue() > 0;
#endif
            optionsBuilder.append("mozNSSCompat", traits.mozNSSCompat);

            const auto options = optionsBuilder.obj();
            LOGV2(24051,
                  "LDAPAPIInfo",
                  "infoVersion"_attr = info.getInfoVersion(),
                  "apiVersion"_attr = info.getAPIVersion(),
                  "protocolVersion"_attr = info.getProtocolVersion(),
                  "extensions"_attr = info.getExtensions(),
                  "vendorName"_attr = info.getVendorName(),
                  "vendorVersion"_attr = info.getVendorVersion(),
                  "options"_attr = options);

            if (ldapForceMultiThreadMode) {
                LOGV2_WARNING(7818800,
                              "Since ldapForceMultiThreadMode has been set, features requiring "
                              "thread safety will be enabled for LDAP connections and sessions");
            } else {
                if (traits.tlsPackage == "OpenSSL") {
                    // OpenSSL < 1.1.1 mixed with libldap_r results in slow performance, so the
                    // connection pool is disabled along with a warning.
                    // OpenSSL >= 1.1.1 is acceptable with either libldap_r or libldap since
                    // reliability concerns have not been seen in either case. Connection pooling is
                    // left on.
                    if (traits.slowLocking && traits.threadSafe) {
                        traits.poolingSafe = false;
                        LOGV2_WARNING(5661701,
                                      "OpenSSL below 1.1.1 has performance impact with libldap_r. "
                                      "Your OpenSSL version is: " OPENSSL_VERSION_TEXT);
                    }
                    if (traits.mozNSSCompat && !traits.threadSafe) {
                        traits.poolingSafe = false;
                        LOGV2_WARNING(
                            5661703,
                            "This system supports TLS_MOZNSS_COMPATIBILITY "
                            "and feature is turned on, which is known to cause crashes "
                            "unless libldap_r is used. "
                            "Disable TLS_MOZNSS_COMPATIBILITY in /etc/openldap/ldap.conf.");
                    }
                } else if (traits.tlsPackage == "GnuTLS" || traits.tlsPackage == "MozNSS") {
                    if (!traits.threadSafe) {
                        traits.poolingSafe = false;
                        LOGV2_WARNING(
                            24052,
                            "LDAP library does not advertise support for thread safety. All access "
                            "will be serialized and connection pooling will be disabled. "
                            "Link mongod against libldap_r to enable concurrent use of LDAP.");
                    }
                } else {
                    uasserted(5661704, "LDAP is using unknown TLS package: " + traits.tlsPackage);
                }
            }

            // If ldapEnableOpenLDAPLogging has been set, then set LDAP_OPT_DEBUG_LEVEL to -1, which
            // enables verbose OpenLDAP logging.
            if (ldapEnableOpenLDAPLogging) {
                int debugVerbosity = -1;
                int ret = ldap_set_option(nullptr, LDAP_OPT_DEBUG_LEVEL, &debugVerbosity);
                if (ret != LDAP_SUCCESS) {
                    uasserted(ErrorCodes::OperationFailed,
                              str::stream() << "Attempted to set the LDAP library's debug "
                                               "verbosity. Received error: "
                                            << ldap_err2string(ret));
                }

                // Set LBER_OPT_LOG_PRINT_FN so that all OpenLDAP debug-level logs are intercepted
                // and incorporated into server logs.
                ret = ber_set_option(nullptr,
                                     LBER_OPT_LOG_PRINT_FN,
                                     reinterpret_cast<void*>(&LDAPDebugCallbackFunction));
                if (ret != LBER_OPT_SUCCESS) {
                    uasserted(ErrorCodes::OperationFailed,
                              str::stream() << "Attempted to set the LBER library's logging "
                                               "callback function. Received error: "
                                            << ret);
                }
            }
        } catch (...) {
            Status status = exceptionToStatus();
            LOGV2_ERROR(24059, "Failed to get LDAP provider traits", "status"_attr = status);
            if (!ldapForceMultiThreadMode) {
                traits.poolingSafe = false;
                traits.threadSafe = false;
            }
        }
    });
}

Status OpenLDAPConnection::connect() {
    auto swHostURIs = _connectionOptions.constructHostURIs();
    if (!swHostURIs.isOK()) {
        return swHostURIs.getStatus();
    }
    std::string hostURIs = std::move(swHostURIs.getValue());

    if (!globalLDAPParams->serverCAFile.empty()) {
        int ret2 = ldap_set_option(
            nullptr, LDAP_OPT_X_TLS_CACERTFILE, globalLDAPParams->serverCAFile.c_str());
        if (ret2 != LDAP_SUCCESS) {
            return Status(ErrorCodes::OperationFailed,
                          str::stream() << "Attempted to set the ca cert. Received error: "
                                        << ldap_err2string(ret2));
        }
    }

    int err;
    {

        stdx::lock_guard<Latch> lock(libldapGlobalMutex);
        err = ldap_initialize(&_pimpl->getSession(), hostURIs.c_str());
    }

    // If ldapConnectionDelay is set, then sleep for "delay" seconds to simulate a hang in the
    // network call.
    ldapConnectionDelay.execute([&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Failed to initialize ldap session to \"" << hostURIs
                                    << "\". Received LDAP error: " << ldap_err2string(err));
    }

    // Upgrade connection to LDAPv3
    int version = LDAP_VERSION3;
    int ret = ldap_set_option(_pimpl->getSession(), LDAP_OPT_PROTOCOL_VERSION, &version);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to upgrade LDAP connection to use LDAPv3. Received error: "
                          << ldap_err2string(ret));
    }

    // Set LDAP operation timeout
    // OpenLDAP expects a 32-bit integer as the input here, hence the use of _timeLimitInt. The LDAP
    // timeout can only accept 32-bit integers, so this cast will not lose information here
    // (_timeout.tv_sec at this point is of type int64).
    ret = ldap_set_option(_pimpl->getSession(), LDAP_OPT_TIMELIMIT, &_timeLimitInt);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to set the LDAP operation timeout. Received error: "
                          << ldap_err2string(ret));
    }

    // Set async await timeout
    ret = ldap_set_option(_pimpl->getSession(), LDAP_OPT_TIMEOUT, &_timeout);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to set the LDAP async wait timeout. Received error: "
                          << ldap_err2string(ret));
    }

    // Set network connection timeout
    ret = ldap_set_option(_pimpl->getSession(), LDAP_OPT_NETWORK_TIMEOUT, &_timeout);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to set the LDAP server network timeout. Received error: "
                          << ldap_err2string(ret));
    }

    ret = ldap_set_option(_pimpl->getSession(), LDAP_OPT_CONNECT_CB, &_tcpConnectionCallback);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to set the LDAP TCP connect callback. Received error: "
                          << ldap_err2string(ret));
    }

    ret = ldap_set_option(_pimpl->getSession(),
                          LDAP_OPT_X_TLS_CONNECT_CB,
                          reinterpret_cast<void*>(&LDAPTLSConnectCallbackFunction));
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to set the LDAP TLS connect callback. Received error: "
                          << ldap_err2string(ret));
    }

    ret = ldap_set_option(_pimpl->getSession(), LDAP_OPT_X_TLS_CONNECT_ARG, this);
    if (ret != LDAP_SUCCESS) {
        return Status(
            ErrorCodes::OperationFailed,
            str::stream()
                << "Attempted to set the LDAP TLS connect callback's arguments. Received error: "
                << ldap_err2string(ret));
    }

    return Status::OK();
}

Status OpenLDAPConnection::bindAsUser(UniqueBindOptions bindOptions,
                                      TickSource* tickSource,
                                      SharedUserAcquisitionStats userAcquisitionStats) {
    if (MONGO_unlikely(ldapNetworkTimeoutOnBind.shouldFail())) {
        return Status(ErrorCodes::NetworkTimeout, "ldapNetworkTimeoutOnBind triggered");
    }

    UserAcquisitionStatsHandle userAcquisitionStatsHandle =
        UserAcquisitionStatsHandle(userAcquisitionStats.get(), tickSource, kBind);
    stdx::lock_guard<OpenLDAPGlobalMutex> lock(conditionalMutex);

    _bindOptions = std::move(bindOptions);
    _rebindCallbackParameters = LDAPRebindCallbackParameters(tickSource, userAcquisitionStats);

    // If ldapBindTimeoutHangIndefinitely is set, then hang until the failpoint is unset.
    ldapBindTimeoutHangIndefinitely.pauseWhileSet();

    // If ldapBindDelay is set, then sleep for "delay" seconds to simulate a hang in the
    // network call.
    ldapBindDelay.execute([&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

    // OpenLDAP needs to know how to bind to strange servers it gets referals to from the
    // target server.
    int err = ldap_set_rebind_proc(_pimpl->getSession(), &openLDAPRebindFunction, this);
    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Unable to set rebind proc, with error: " << ldap_err2string(err));
    }

    auto [status, code] = openLDAPBindFunction(_pimpl->getSession(), "default", 0, 0, this);
    if (!status.isOK()) {
        return status;
    }

    _boundUser = _bindOptions->bindDN;
    userAcquisitionStatsHandle.recordTimerEnd();
    return Status::OK();
}

boost::optional<std::string> OpenLDAPConnection::currentBoundUser() const {
    return _boundUser;
}

Status OpenLDAPConnection::checkLiveness(TickSource* tickSource,
                                         SharedUserAcquisitionStats userAcquisitionStats) {
    stdx::lock_guard<OpenLDAPGlobalMutex> lock(conditionalMutex);

    // If ldapLivenessCheckDelay is set, then sleep for "delay" seconds to simulate a hang in
    // the network call.
    ldapLivenessCheckDelay.execute(
        [&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

    // Update the rebindCallbackParameters in the event of a referral.
    _rebindCallbackParameters = LDAPRebindCallbackParameters(tickSource, userAcquisitionStats);

    return _pimpl->checkLiveness(&_timeout, tickSource, userAcquisitionStats);
}

StatusWith<LDAPEntityCollection> OpenLDAPConnection::query(
    LDAPQuery query, TickSource* tickSource, SharedUserAcquisitionStats userAcquisitionStats) {
    if (MONGO_unlikely(ldapNetworkTimeoutOnQuery.shouldFail())) {
        return Status(ErrorCodes::NetworkTimeout, "ldapNetworkTimeoutOnQuery triggered");
    }

    stdx::lock_guard<OpenLDAPGlobalMutex> lock(conditionalMutex);

    // If ldapSearchDelay is set, then sleep for "delay" seconds to simulate a hang in the
    // network call.
    ldapSearchDelay.execute([&](const BSONObj& data) { sleepsecs(data["delay"].numberInt()); });

    // Update the rebindCallbackParameters in the event of a referral.
    _rebindCallbackParameters = LDAPRebindCallbackParameters(tickSource, userAcquisitionStats);

    return _pimpl->query(std::move(query), &_timeout, tickSource, userAcquisitionStats);
}

LDAPSessionId OpenLDAPConnection::getId() const {
    return _pimpl->getId();
}

Status OpenLDAPConnection::disconnect() {
    LOGV2_DEBUG(
        9339701, 3, "Disconnecting OpenLDAPConnection", "ldapSessionId"_attr = _pimpl->getId());
    if (!_pimpl->getSession()) {
        return Status::OK();
    }

    int ret = ldap_get_option(_pimpl->getSession(), LDAP_OPT_CONNECT_CB, &_tcpConnectionCallback);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to unset the LDAP connect callback. Received error: "
                          << ldap_err2string(ret));
    }

    _reaper->reap(_pimpl->getSession(), getId());

    _pimpl->getSession() = nullptr;


    return Status::OK();
}

void disconnectLDAPConnection(LDAP* ldap, LDAPSessionId ldapSessionId) {
    stdx::lock_guard<OpenLDAPGlobalMutex> lock(conditionalMutex);
    // ldap_unbind_ext_s, contrary to its name, closes the connection to the server.
    int ret = ldap_unbind_ext_s(ldap, nullptr, nullptr);
    if (ret != LDAP_SUCCESS) {
        LOGV2_ERROR(5531602,
                    "Unable to unbind from LDAP",
                    "ldapSessionId"_attr = ldapSessionId,
                    "__error__"_attr = ldap_err2string(ret));
    }
}

void OpenLDAPGlobalMutex::lock() {
    initLockType();
    if (!_bypassLock) {
        libldapGlobalMutex.lock();
    }
}

void OpenLDAPGlobalMutex::unlock() {
    initLockType();
    if (!_bypassLock) {
        libldapGlobalMutex.unlock();
    }
}

void OpenLDAPGlobalMutex::initLockType() {
    std::call_once(_init, [this]() {
        OpenLDAPConnection::initTraits();
        _bypassLock = ldapForceMultiThreadMode || OpenLDAPConnection::getTraits().threadSafe;
        LOGV2_DEBUG(5661705, 1, "LDAP Global mutex", "bypassLock"_attr = _bypassLock);
    });
}

}  // namespace mongo
