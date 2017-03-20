/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "openldap_connection.h"

#include <boost/algorithm/string/join.hpp>
#include <ldap.h>
#include <sasl/sasl.h>

#include "mongo/stdx/memory.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"

#include "../ldap_connection_options.h"
#include "../ldap_query.h"
#include "ldap_connection_helpers.h"

namespace mongo {
namespace {

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

    static const auto LDAP_success = LDAP_SUCCESS;
    static const auto LDAP_OPT_error_code = LDAP_OPT_RESULT_CODE;
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
        const LDAPBindOptions* bindOptions = static_cast<LDAPBindOptions*>(defaults);

        while (request && request->id != SASL_CB_LIST_END) {
            switch (request->id) {
                case SASL_CB_AUTHNAME:
                    request->result = bindOptions->bindDN.c_str();
                    request->len = bindOptions->bindDN.size();
                    break;
                case SASL_CB_PASS:
                    request->result = bindOptions->password->c_str();
                    request->len = bindOptions->password->size();
                    break;
                default:
                    break;
            }
            ++request;
        }

        return LDAP_SUCCESS;
    } catch (...) {
        error() << "Unexpected exception caught in OpenLDAPConnection's saslInteract: "
                << exceptionToStatus();
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
int openLDAPBindFunction(
    LDAP* session, LDAP_CONST char* url, ber_tag_t request, ber_int_t msgid, void* params) {
    try {
        LDAPBindOptions* bindOptions = static_cast<LDAPBindOptions*>(params);

        LOG(3) << "Binding to LDAP server \"" << url
               << "\" with bind parameters: " << bindOptions->toCleanString();

        int ret;

        if (bindOptions->authenticationChoice == LDAPBindType::kSasl) {
            ret = ldap_sasl_interactive_bind_s(session,
                                               nullptr,
                                               bindOptions->saslMechanisms.c_str(),
                                               nullptr,
                                               nullptr,
                                               LDAP_SASL_QUIET,
                                               &saslInteract,
                                               static_cast<void*>(bindOptions));
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
        } else {
            error() << "Attempted to bind to LDAP server with unrecognized bind type: "
                    << authenticationChoiceToString(bindOptions->authenticationChoice);
            return LDAP_OPERATIONS_ERROR;
        }

        if (ret != LDAP_SUCCESS) {
            error() << "Failed to bind to LDAP server at " << url << ": " << ldap_err2string(ret)
                    << ". Bind parameters were: " << bindOptions->toCleanString();
        }
        return ret;
    } catch (...) {
        Status status = exceptionToStatus();
        error() << "Failed to bind to LDAP server at " << url << " : " << status;
        return LDAP_OPERATIONS_ERROR;
    }
}

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

    std::vector<std::string> getExtensions() const {
        return extensions;
    }

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
        Type info;
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

}  // namespace

class OpenLDAPConnection::OpenLDAPConnectionPIMPL
    : public LDAPSessionHolder<OpenLDAPSessionParams> {};

OpenLDAPConnection::OpenLDAPConnection(LDAPConnectionOptions options)
    : LDAPConnection(std::move(options)), _pimpl(stdx::make_unique<OpenLDAPConnectionPIMPL>()) {
    Seconds seconds = duration_cast<Seconds>(_options.timeout);
    _timeout.tv_sec = seconds.count();
    _timeout.tv_usec = durationCount<Microseconds>(_options.timeout - seconds);
}

OpenLDAPConnection::~OpenLDAPConnection() {
    Status status = disconnect();
    if (!status.isOK()) {
        error() << "LDAP unbind failed: " << status;
    }
}

Status OpenLDAPConnection::connect() {
    auto swHostURIs = _options.constructHostURIs();
    if (!swHostURIs.isOK()) {
        return swHostURIs.getStatus();
    }
    std::string hostURIs = std::move(swHostURIs.getValue());

    int err;
    {

        stdx::lock_guard<stdx::mutex> lock(libldapGlobalMutex);
        err = ldap_initialize(&_pimpl->getSession(), hostURIs.c_str());
    }

    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Failed to initialize ldap session to \"" << hostURIs
                                    << "\". Received LDAP error: "
                                    << ldap_err2string(err));
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
    ret = ldap_set_option(_pimpl->getSession(), LDAP_OPT_TIMELIMIT, &_timeout);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to set the LDAP operation timeout. Received error: "
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

    // Log LDAP API information
    if (shouldLog(LogstreamBuilder::severityCast(3))) {
        try {
            LDAPOptionAPIInfo info = _pimpl->getOption<LDAPOptionAPIInfo>(
                "OpenLDAPConnection::connect", "Getting API info");

            LOG(3) << "LDAPAPIInfo: { "
                   << "ldapai_info_version: " << info.getInfoVersion() << ", "
                   << "ldapai_api_version: " << info.getAPIVersion() << ", "
                   << "ldap_protocol_version: " << info.getProtocolVersion() << ", "
                   << "ldapai_extensions: [" << boost::algorithm::join(info.getExtensions(), ", ")
                   << "], "
                   << "ldapai_vendor_name: " << info.getVendorName() << ", "
                   << "ldapai_vendor_version: " << info.getVendorVersion() << "}";
        } catch (...) {
            Status status = exceptionToStatus();
            error() << "Attempted to get LDAPAPIInfo. Received error: " << status;
        }
    }

    return Status::OK();
}

Status OpenLDAPConnection::bindAsUser(const LDAPBindOptions& bindOptions) {
    int err = openLDAPBindFunction(_pimpl->getSession(), "default", 0, 0, (void*)&bindOptions);
    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "LDAP bind failed with error: " << ldap_err2string(err));
    }
    // OpenLDAP needs to know how to bind to strange servers it gets referals to from the
    // target server.
    err = ldap_set_rebind_proc(_pimpl->getSession(), &openLDAPBindFunction, (void*)&bindOptions);
    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Unable to set rebind proc, with error: "
                                    << ldap_err2string(err));
    }
    return Status::OK();
}

StatusWith<LDAPEntityCollection> OpenLDAPConnection::query(LDAPQuery query) {
    return _pimpl->query(std::move(query), &_timeout);
}

Status OpenLDAPConnection::disconnect() {
    if (!_pimpl->getSession()) {
        return Status::OK();
    }

    // ldap_unbind_ext_s, contrary to its name, closes the connection to the server.
    int ret = ldap_unbind_ext_s(_pimpl->getSession(), nullptr, nullptr);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Unable to unbind from LDAP: " << ldap_err2string(ret));
    }
    _pimpl->getSession() = nullptr;

    return Status::OK();
}
}  // namespace mongo
