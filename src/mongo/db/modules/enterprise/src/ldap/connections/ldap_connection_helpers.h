/**
 *  Copyright (C) 2016 MongoDB Inc.
 */
#pragma once

// This file is a helper containing templates which must be instantiated with libary dependent
// types. As such, it must be included, and used in a .cpp file which has already set up logging.

#include <cstdint>

#include "mongo/logger/log_component.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/logstream_builder.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"

#include "../ldap_connection_options.h"
#include "../ldap_query.h"
#include "../ldap_type_aliases.h"

// We must redefine our log macro, because the original LOG isn't intended to be used from headers
#define MONGO_LDAPLOG(DLEVEL)                                                                  \
    if (!(::mongo::logger::globalLogDomain())                                                  \
             ->shouldLog(::mongo::logger::LogComponent::kAccessControl,                        \
                         ::mongo::logger::LogstreamBuilder::severityCast(DLEVEL))) {           \
    } else                                                                                     \
    ::mongo::logger::LogstreamBuilder(::mongo::logger::globalLogDomain(),                      \
                                      ::mongo::getThreadName(),                                \
                                      ::mongo::logger::LogstreamBuilder::severityCast(DLEVEL), \
                                      ::mongo::logger::LogComponent::kAccessControl)


namespace mongo {

enum class LDAPQueryScope : std::uint8_t;
class Status;
class StringData;

// Transform an LDAPQueryScope into the corresponding library specific constant
int mapScopeToLDAP(const LDAPQueryScope& type);

/**
 * Holds and interacts with a session to an LDAP server, using an RFC 1823 compliant
 * LDAP API. This class must be provided with a template parameter containing the
 * concrete types and functions which must be called.
 */
template <typename S>
class LDAPSessionHolder {
public:
    LDAPSessionHolder(typename S::SessionType* session) : _session(session) {}
    LDAPSessionHolder() : _session(nullptr) {}
    LDAPSessionHolder& operator=(typename S::SessionType* session) {
        _session = session;
        return *this;
    }

    typename S::SessionType*& getSession() {
        return _session;
    }

    Status resultCodeToStatus(StringData functionName, StringData failureHint) {
        typename S::ErrorCodeType error;
        typename S::ErrorCodeType errorWhileGettingError =
            S::ldap_get_option(_session, S::LDAP_OPT_error_code, &error);
        if (errorWhileGettingError != S::LDAP_success) {
            return Status(
                ErrorCodes::UnknownError,
                str::stream() << "Unable to get error code after LDAP operation <" << functionName
                              << ">. \"Failed to "
                              << failureHint
                              << "\". ("
                              << errorWhileGettingError
                              << "/"
                              << S::toNativeString(S::ldap_err2string(errorWhileGettingError))
                              << ")");
        }

        return resultCodeToStatus(error, functionName, failureHint);
    }

    Status resultCodeToStatus(typename S::ErrorCodeType statusCode,
                              StringData functionName,
                              StringData failureHint) {
        if (statusCode == S::LDAP_success) {
            return Status::OK();
        }

        LibraryCharPtr errorString;
        typename S::ErrorCodeType errorWhileGettingError =
            S::ldap_get_option(_session, S::LDAP_OPT_error_string, &errorString);
        if (errorWhileGettingError != S::LDAP_success || !errorString) {
            return Status(ErrorCodes::UnknownError,
                          str::stream() << "LDAP Operation <" << functionName << ">, "
                                        << failureHint
                                        << "\". ("
                                        << statusCode
                                        << "/"
                                        << S::toNativeString(S::ldap_err2string(statusCode))
                                        << ")");
        }

        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "LDAP operation <" << functionName << ">, " << failureHint
                                    << "\". ("
                                    << statusCode
                                    << "/"
                                    << S::toNativeString(S::ldap_err2string(statusCode))
                                    << "): "
                                    << S::toNativeString(errorString));
    }

    StatusWith<LDAPEntityCollection> query(LDAPQuery query, typename S::TimeoutType* timeout) {
        MONGO_LDAPLOG(3) << "Performing LDAP query: " << query;
        LDAPEntityCollection queryResults;

        // Convert the attribute vector to a mutable null terminated array of char* strings
        // libldap wants a non-const copy, so prevent it from breaking our configuration data
        size_t requestedAttributesSize = query.getAttributes().size();
        std::vector<LibraryCharPtr> requestedAttributes;
        const auto requestedAttributesGuard = MakeGuard([&] {
            for (LibraryCharPtr attribute : requestedAttributes) {
                delete[] attribute;
            }
        });

        requestedAttributes.reserve(requestedAttributesSize + 1);
        for (size_t i = 0; i < requestedAttributesSize; ++i) {
            requestedAttributes.push_back(
                new typename S::LibraryCharType[query.getAttributes()[i].size() + 1]);

            std::memcpy(requestedAttributes.back(),
                        S::toLibraryString(query.getAttributes()[i].c_str()).c_str(),
                        sizeof(typename S::LibraryCharType) *
                            (query.getAttributes()[i].size() + 1));
        }
        requestedAttributes.push_back(nullptr);

        // ldap_msgfree is a no-op on nullptr. The result from ldap_search_ext_s must be freed
        // with ldap_msgfree, reguardless of the return code.
        typename S::MessageType* queryResult = nullptr;
        const auto queryResultGuard = MakeGuard([&] { S::ldap_msgfree(queryResult); });

        // Perform the actual query
        typename S::ErrorCodeType err = S::ldap_search_ext_s(
            _session,
            query.getBaseDN().empty()
                ? nullptr
                : const_cast<LibraryCharPtr>(S::toLibraryString(query.getBaseDN().c_str()).c_str()),
            mapScopeToLDAP(query.getScope()),
            query.getFilter().empty()
                ? nullptr
                : const_cast<LibraryCharPtr>(S::toLibraryString(query.getFilter().c_str()).c_str()),
            requestedAttributes.data(),
            0,
            nullptr,
            nullptr,
            timeout,
            0,
            &queryResult);
        Status status = resultCodeToStatus(err,
                                           "ldap_search_ext_s",
                                           std::string("Failed to perform query: ") +
                                               S::toNativeString(S::ldap_err2string(err)) +
                                               "' Query was: '" + query.toString() + "'");
        if (!status.isOK()) {
            return status;
        }

        // ldap_count_entries returns the number of entries contained in the result.
        // On error, it returns -1 and sets an error code.
        int entryCount = S::ldap_count_entries(_session, queryResult);
        if (entryCount < 0) {
            return resultCodeToStatus("ldap_count_entries",
                                      "getting number of entries returned from LDAP query");
        } else if (entryCount == 0) {
            // If we found no results, don't bother trying to process them.
            return queryResults;
        }

        // Process the data received by the query, and put into data structure for return.
        // We must iterate over each entry in the queryResult. ldap_first_entry returns a pointer to
        // the first entry, ldap_next_entry takes a pointer to an entry and returns a pointer to the
        // subsequent entry. These pointers are owned by the queryResult. If there are no remaining
        // results, ldap_next_entry returns NULL.
        typename S::MessageType* entry = S::ldap_first_entry(_session, queryResult);
        if (!entry) {
            return resultCodeToStatus("ldap_first_entry", "getting LDAP entry from results");
        }

        while (entry) {
            // ldap_get_dn returns an owned pointer to the DN of the provided entry. We must free it
            // with ldap_memfree. If ldap_get_dn experiences an error it will return NULL and set an
            // error code.
            LibraryCharPtr entryDN = S::ldap_get_dn(_session, entry);
            const auto entryDNGuard = MakeGuard([&] { S::ldap_memfree(entryDN); });
            if (!entryDN) {
                return resultCodeToStatus("ldap_get_dn",
                                          "getting DN for a result from the LDAP query");
            }

            MONGO_LDAPLOG(3) << "From LDAP query result, got an entry with DN: "
                             << S::toNativeString(entryDN);
            LDAPAttributeKeyValuesMap attributeValueMap;

            // For each entity in the response, parse each attribute it contained.
            // ldap_first_attribute and ldap_next_attribute work similarly to
            // ldap_{first,next}_entry.  They return the name as an owned char*, which must be freed
            // with ldap_memfree.  ldap_memfree behaves like free(), so will be a no-op when called
            // with a NULL pointer.  ldap_first_attribute accepts a BerElement** which it will use
            // to return an owned pointer to a BerElement. This pointer must be provided to each
            // subsequent invokation of ldap_next_attribute. It must be freed with ber_free, with
            // the second argument set to zero. If either functions fail, they return NULL and set
            // an error code.  Per RFC1823, if either function have no more attributes to return,
            // they will return NULL.
            typename S::BerElementType* element = nullptr;
            const auto elementGuard = MakeGuard([&] { ber_free(element, 0); });
            LibraryCharPtr attribute = S::ldap_first_attribute(_session, entry, &element);
            while (attribute) {
                // This takes attribute by value, so we can safely set it to ldap_next_attribute
                // later, and the old ON_BLOCK_EXIT will free the old attribute.
                const auto attributeGuard = MakeGuard([attribute] { S::ldap_memfree(attribute); });
                MONGO_LDAPLOG(3) << "From LDAP entry with DN " << S::toNativeString(entryDN)
                                 << ", got attribute " << S::toNativeString(attribute);

                // Attributes in LDAP are multi-valued.  For each attribute contained by the entity,
                // we must parse each value it contained.  ldap_get_values_len returns an owned
                // pointer to a NULL terminated array of berval pointers to values. It must be freed
                // with ldap_value_free_len. It is not defined what ldap_value_free_len will do with
                // a NULL pointer. On error, ldap_get_values_len will return NULL and set an error
                // code.
                typename S::BerValueType** values =
                    S::ldap_get_values_len(_session, entry, attribute);
                if (values == nullptr) {
                    return resultCodeToStatus(
                        "ldap_get_values_len",
                        "extracting values for attribute, after successful LDAP query");
                }
                const auto valuesGuard = MakeGuard([&] { S::ldap_value_free_len(values); });

                LDAPAttributeValues valueStore;

                for (berval** value = values; *value != nullptr; value++) {
                    LDAPAttributeValue strValue((*values)->bv_val, (*values)->bv_len);
                    valueStore.emplace_back(std::move(strValue));
                }

                attributeValueMap.emplace(LDAPAttributeKey(S::toNativeString(attribute)),
                                          std::move(valueStore));
                attribute = ldap_next_attribute(_session, entry, element);
            }

            queryResults.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(S::toNativeString(entryDN)),
                                 std::forward_as_tuple(std::move(attributeValueMap)));
            entry = ldap_next_entry(_session, entry);
        }

        return queryResults;
    }

private:
    using LibraryCharPtr = typename S::LibraryCharType*;
    typename S::SessionType* _session;
};


}  // namespace mongo
