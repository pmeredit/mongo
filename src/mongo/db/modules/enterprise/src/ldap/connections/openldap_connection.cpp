/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "openldap_connection.h"

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
}  // namespace

OpenLDAPConnection::OpenLDAPConnection(LDAPConnectionOptions options)
    : LDAPConnection(std::move(options)), _session(nullptr) {
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
    int err;
    {
        stdx::lock_guard<stdx::mutex> lock(libldapGlobalMutex);
        err = ldap_initialize(&_session, _options.hostURI.c_str());
    }

    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Failed to initialize ldap session to \"" << _options.hostURI
                                    << "\". Received LDAP error: " << ldap_err2string(err));
    }

    // Upgrade connection to LDAPv3
    int version = LDAP_VERSION3;
    int ret = ldap_set_option(_session, LDAP_OPT_PROTOCOL_VERSION, &version);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Attempted to upgrade LDAP connection to use LDAPv3. Received error: "
                          << ldap_err2string(ret));
    }

    // Log LDAP API information
    if (shouldLog(LogstreamBuilder::severityCast(3))) {
        auto info = stdx::make_unique<LDAPAPIInfo>();
        info->ldapai_info_version = 1;
        // LDAP_OPT_API populates a LDAPAPIInfo struct. The caller is responsible for
        // ldap_memfreeing ldapai_vendor_name, each element in ldapai_extensions, and
        // ldapai_extensions itself.
        ret = ldap_get_option(_session, LDAP_OPT_API_INFO, info.get());

        if (ret != LDAP_SUCCESS) {
            error() << "Attempted to get LDAPAPIInfo. Received error: " << ldap_err2string(ret);
        } else {
            ON_BLOCK_EXIT(ldap_memfree, info->ldapai_vendor_name);
            ON_BLOCK_EXIT([=](char** extensions) {
                for (char** it = extensions; *it != nullptr; ++it) {
                    ldap_memfree(*it);
                }
                ldap_memfree(extensions);
            }, info->ldapai_extensions);

            StringBuilder log;
            log << "LDAPAPIInfo: { "
                << "ldapai_info_version: " << info->ldapai_info_version << ", "
                << "ldapai_api_version: " << info->ldapai_api_version << ", "
                << "ldap_protocol_version: " << info->ldapai_protocol_version << ", "
                << "ldapai_extensions: [";

            char** it = info->ldapai_extensions;
            while (*it != nullptr) {
                log << *it;
                if (++it == nullptr) {
                    log << "], ";
                } else {
                    log << ", ";
                }
            }

            log << "ldapai_vendor_name: " << info->ldapai_vendor_name << ", "
                << "ldapai_vendor_version: " << info->ldapai_vendor_version << "}";
            LOG(3) << log.str();
        }
    }

    return Status::OK();
}

Status OpenLDAPConnection::bindAsUser(const LDAPBindOptions& bindOptions) {
    int err = openLDAPBindFunction(_session, "default", 0, 0, (void*)&bindOptions);
    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "LDAP bind failed with error: " << ldap_err2string(err));
    }
    // OpenLDAP needs to know how to bind to strange servers it gets referals to from the
    // target server.
    err = ldap_set_rebind_proc(_session, &openLDAPBindFunction, (void*)&bindOptions);
    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream()
                          << "Unable to set rebind proc, with error: " << ldap_err2string(err));
    }
    return Status::OK();
}

Status OpenLDAPConnection::_resultCodeToStatus(StringData functionName, StringData failureHint) {
    int error;
    int errorWhileGettingError = ldap_get_option(_session, LDAP_OPT_RESULT_CODE, &error);
    if (errorWhileGettingError != LDAP_SUCCESS) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "Unable to get error code after OpenLDAP operation ("
                                    << functionName << "), \"" << failureHint << "\". "
                                    << ldap_err2string(errorWhileGettingError));
    }
    if (error == LDAP_SUCCESS) {
        return Status(ErrorCodes::UnknownError,
                      str::stream() << "OpenLDAP operation (" << functionName << ") '"
                                    << failureHint
                                    << "' failed, but the result code indicates no fault.");
    }
    return Status(ErrorCodes::OperationFailed,
                  str::stream() << "OpenLDAP operation (" << functionName << ") '" << failureHint
                                << "' failed: " << ldap_err2string(error));
}

StatusWith<LDAPEntityCollection> OpenLDAPConnection::query(LDAPQuery query) {
    LOG(3) << "Performing OpenOpenLDAP query: " << query;
    LDAPEntityCollection queryResults;

    // Convert the attribute vector to a mutable null terminated array of char* strings
    // libldap wants a non-const copy, so prevent it from breaking our configuration data
    size_t requestedAttributesSize = query.getAttributes().size();
    std::vector<char*> requestedAttributes;
    ON_BLOCK_EXIT([](std::vector<char*>* reqAttr) {
        for (char* attribute : *reqAttr) {
            delete[] attribute;
        }
    }, &requestedAttributes);

    requestedAttributes.reserve(requestedAttributesSize + 1);
    for (size_t i = 0; i < requestedAttributesSize; ++i) {
        requestedAttributes.push_back(new char[query.getAttributes()[i].size() + 1]);

        strncpy(requestedAttributes.back(),
                query.getAttributes()[i].c_str(),
                query.getAttributes()[i].size() + 1);
    }
    requestedAttributes.push_back(nullptr);

    // ldap_msgfree is a no-op on nullptr. The result from ldap_search_ext_s must be ldap_msgfreed,
    // reguardless of the return code.
    LDAPMessage* queryResult = nullptr;
    ON_BLOCK_EXIT(ldap_msgfree, queryResult);

    // Perform the actual query
    int err = ldap_search_ext_s(_session,
                                query.getBaseDN().empty() ? nullptr : query.getBaseDN().c_str(),
                                mapScopeToLDAP(query.getScope()),
                                query.getFilter().empty() ? nullptr : query.getFilter().c_str(),
                                requestedAttributes.data(),
                                0,
                                nullptr,
                                nullptr,
                                &_timeout,
                                0,
                                &queryResult);
    if (err != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Failed to perform OpenLDAP query. Error: '"
                                    << ldap_err2string(err) << "' Query was: '" << query.toString()
                                    << "'");
    }

    // ldap_count_entries returns the number of entries contained in the result.
    // On error, it returns -1 and sets an error code.
    int entryCount = ldap_count_entries(_session, queryResult);
    if (entryCount < 0) {
        return _resultCodeToStatus("ldap_count_entries",
                                   "getting number of entries returned from OpenLDAP query");
    } else if (entryCount == 0) {
        // If we found no results, don't bother trying to process them.
        return queryResults;
    }

    // Process the data received by the query, and put into data structure for return.
    // We must iterate over each entry in the queryResult. ldap_first_entry returns a pointer to
    // the first entry, ldap_next_entry takes a pointer to an entry and returns a pointer to the
    // subsequent entry. These pointers are owned by the queryResult. If there are no remaining
    // results, ldap_next_entry returns NULL.
    LDAPMessage* entry = ldap_first_entry(_session, queryResult);
    if (!entry) {
        return _resultCodeToStatus("ldap_first_entry", "getting LDAP entry from results");
    }

    while (entry) {
        // ldap_get_dn returns an owned pointer to the DN of the provided entry. We must free it
        // with ldap_memfree. If ldap_get_dn experiences an error it will return NULL and set an
        // error code.
        char* entryDN = ldap_get_dn(_session, entry);
        ON_BLOCK_EXIT(ldap_memfree, entryDN);
        if (!entryDN) {
            return _resultCodeToStatus("ldap_get_dn",
                                       "getting DN for a result from the OpenLDAP query");
        }

        LOG(3) << "From OpenLDAP query result, got an entry with DN: " << entryDN;
        LDAPAttributeKeyValuesMap attributeValueMap;

        // For each entity in the response, parse each attribute it contained.
        // ldap_first_attribute and ldap_next_attribute work similarly to ldap_{first,next}_entry.
        // They return the name as an owned char*, which must be freed with ldap_memfree.
        // ldap_memfree behaves like free(), so will be a no-op when called with a NULL pointer.
        // ldap_first_attribute accepts a BerElement** which it will use to return an owned
        // pointer to a BerElement. This pointer must be provided to each subsequent invokation of
        // ldap_next_attribute. It must be freed with ber_free, with the second argument set to
        // zero. If either functions fail, they return NULL and set an error code.
        // Per RFC1823, if either function have no more attributes to return, they will return NULL.
        BerElement* element = nullptr;
        ON_BLOCK_EXIT([=](BerElement* element) { ber_free(element, 0); }, element);
        char* attribute = ldap_first_attribute(_session, entry, &element);
        while (attribute) {
            // This takes attribute by value, so we can safely set it to ldap_next_attribute
            // later, and the old ON_BLOCK_EXIT will free the old attribute.
            ON_BLOCK_EXIT(ldap_memfree, attribute);
            LOG(3) << "From LDAP entry with DN " << entryDN << ", got attribute " << attribute;

            // Attributes in LDAP are multi-valued.
            // For each attribute contained by the entity, we must parse each value it contained.
            // ldap_get_values_len returns an owned pointer to a NULL terminated array of berval
            // pointers to values. It must be freed with ldap_value_free_len. It is not defined
            // what ldap_value_free_len will do with a NULL pointer. On error, ldap_get_values_len
            // will return NULL and set an error code.
            berval** values = ldap_get_values_len(_session, entry, attribute);
            if (values == nullptr) {
                return _resultCodeToStatus(
                    "ldap_get_values_len",
                    "extracting values for attribute, after successful OpenLDAP query");
            }
            ON_BLOCK_EXIT(ldap_value_free_len, values);

            LDAPAttributeValues valueStore;

            while (*values) {
                LDAPAttributeValue strValue((*values)->bv_val, (*values)->bv_len);
                valueStore.emplace_back(std::move(strValue));
                ++values;
            }

            attributeValueMap.emplace(LDAPAttributeKey(attribute), std::move(valueStore));
            attribute = ldap_next_attribute(_session, entry, element);
        }

        queryResults.emplace(std::piecewise_construct,
                             std::forward_as_tuple(entryDN),
                             std::forward_as_tuple(std::move(attributeValueMap)));
        entry = ldap_next_entry(_session, entry);
    }

    return queryResults;
}

Status OpenLDAPConnection::disconnect() {
    if (!_session) {
        return Status::OK();
    }

    // ldap_unbind_ext_s, contrary to its name, closes the connection to the server.
    int ret = ldap_unbind_ext_s(_session, nullptr, nullptr);
    if (ret != LDAP_SUCCESS) {
        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "Unable to unbind from LDAP: " << ldap_err2string(ret));
    }
    _session = nullptr;

    return Status::OK();
}
}  // namespace mongo
