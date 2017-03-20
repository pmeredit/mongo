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

// Iterator across NULL terminated arrays.
// Arrays of this format will contain 0 or more pointers to objects of type T, followed by a pointer
// to NULL.
//
template <typename T>
class LDAPArrayIterator : public std::iterator<std::forward_iterator_tag, const T> {
public:
    // Constructs a iterator, beginning at the element in basePtr.
    // If basePtr is nullptr, produces a past-the-end iterator.
    // The zero argument constructor of this class returns a past-the-end iterator.
    explicit LDAPArrayIterator(const T* basePtr = nullptr) noexcept : _basePtr(basePtr) {
        static_assert(std::is_pointer<T>::value,
                      "LDAPArrayIterator must traverse an array of pointers");
    }
    LDAPArrayIterator& operator++() {
        this->advance();
        return *this;
    }
    LDAPArrayIterator operator++(int) {
        const auto ret = *this;
        this->advance();
        return ret;
    }
    friend bool operator==(const LDAPArrayIterator& lhs, const LDAPArrayIterator& rhs) {
        return lhs._basePtr == rhs._basePtr;
    }
    friend bool operator!=(const LDAPArrayIterator& lhs, const LDAPArrayIterator& rhs) {
        return !(lhs == rhs);
    }
    auto& operator*() const {
        return *this->_basePtr;
    }
    auto* operator-> () const {
        return this->_basePtr;
    }

private:
    void advance() {
        if (this->_basePtr) {
            ++this->_basePtr;
            if (!*this->_basePtr) {
                this->_basePtr = nullptr;
            }
        }
    }
    const T* _basePtr;
};

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


    // Template function for making requests from ldap_get_option.
    // OptionSpec must have the following constraints:
    //    type Type: The type returned by ldap_get_option
    //    static member Code: The value passed to ldap_get_option to acquire a particular option
    //
    //    static member makeDefaultInParam: A function which makes the unpopulated Type variable
    //                                      to be passed into ldap_get_option.
    //    The constructor must take a single variable of type Type.
    template <typename OptionSpec>
    OptionSpec getOption(StringData functionName, StringData failureHint) {
        typename OptionSpec::Type inParam = OptionSpec::makeDefaultInParam();
        const typename S::ErrorCodeType errorWhileGettingOption =
            S::ldap_get_option(_session, OptionSpec::Code, &inParam);
        if (errorWhileGettingOption != S::LDAP_success) {
            uasserted(
                ErrorCodes::UnknownError,
                str::stream() << "Unable to get error code after LDAP operation <" << functionName
                              << ">. \"Failed to "
                              << failureHint
                              << "\". ("
                              << errorWhileGettingOption
                              << "/"
                              << S::toNativeString(S::ldap_err2string(errorWhileGettingOption))
                              << ")");
        }

        return OptionSpec(inParam);
    }

    // A stateless deleter wrapping ldap_memfree, for use with unique_ptr.
    template <typename T>
    struct ldapMemFree {
        void operator()(T p) const {
            S::ldap_memfree(p);
        }
    };
    template <typename T>
    using UniqueMemFreed = std::unique_ptr<typename std::remove_pointer<T>::type, ldapMemFree<T>>;

    // Spec for how to process a request for an error code from ldap_get_option.
    class LDAPOptionErrorCode {
    public:
        using Type = typename S::ErrorCodeType;
        constexpr static auto Code = S::LDAP_OPT_error_code;

        Type getCode() const {
            return code;
        };

        static auto makeDefaultInParam() {
            return Type{};
        }

        friend LDAPSessionHolder<S>;

    private:
        explicit LDAPOptionErrorCode(const Type code) : code(code){};

        Type code;
    };

    // Spec for how to process a request for an error string from ldap_get_option.
    class LDAPOptionErrorString {
    public:
        using Type = typename S::LibraryCharType*;
        constexpr static auto Code = S::LDAP_OPT_error_string;

        std::string getErrorString() && {
            return std::move(_errString);
        }

        StringData getErrorString() const& {
            return _errString;
        }

        static auto makeDefaultInParam() {
            return nullptr;
        }

        friend LDAPSessionHolder<S>;
        friend StringBuilder& operator<<(
            StringBuilder& stream,
            const typename LDAPSessionHolder<S>::LDAPOptionErrorString& errString) {
            return stream << errString._errString;
        }

    private:
        // Takes ownership of errString which becomes invalid. Pass lifecycle manager
        // down to the "real" constructor, before member initialization begins.
        explicit LDAPOptionErrorString(Type errString)
            : LDAPOptionErrorString(UniqueMemFreed<Type>(errString)) {}

        explicit LDAPOptionErrorString(const UniqueMemFreed<Type>& errString) {
            if (errString) {
                _errString = S::toNativeString(errString.get());
            } else {
                _errString = "No error could be retrieved from the LDAP server.";
            }
        }

        std::string _errString;
    };

    // Helper function which converts the error code on the LDAP session handle into a Status.
    // If it acquires Status::OK(), it will return a different, non-OK, Status.
    // This function should be called when the caller has encountered a fatal event.
    // Will acquire any diagnostic message which may have been set on the LDAP session handle.
    Status obtainFatalResultCodeAsStatus(StringData functionName, StringData failureHint) {
        Status result(resultCodeToStatus(functionName, failureHint));

        if (result.isOK()) {
            return Status(
                ErrorCodes::UnknownError,
                str::stream() << functionName << "encountered a fatal condition, but the LDAP "
                              << "session handle reports the last operation was successful. "
                              << "Failed to "
                              << failureHint);
        }

        return result;
    }

    // Helper function which converts the error code on the LDAP session handle into a Status.
    // Will acquire any diagnostic message which may have been set on the LDAP session handle.
    Status resultCodeToStatus(StringData functionName, StringData failureHint) {
        LDAPOptionErrorCode swErrorCode = getOption<LDAPOptionErrorCode>(functionName, failureHint);

        return resultCodeToStatus(swErrorCode.getCode(), functionName, failureHint);
    }

    // Helper function which converts an LDAP error code into a Status.
    // Will acquire any diagnostic message which may have been set on the LDAP session handle.
    Status resultCodeToStatus(typename S::ErrorCodeType statusCode,
                              StringData functionName,
                              StringData failureHint) {
        if (statusCode == S::LDAP_success) {
            return Status::OK();
        }

        LDAPOptionErrorString errorStr =
            getOption<LDAPOptionErrorString>(functionName, failureHint);

        return Status(ErrorCodes::OperationFailed,
                      str::stream() << "LDAP operation <" << functionName << ">, " << failureHint
                                    << "\". ("
                                    << statusCode
                                    << "/"
                                    << S::toNativeString(S::ldap_err2string(statusCode))
                                    << "): "
                                    << errorStr);
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
            return obtainFatalResultCodeAsStatus(
                "ldap_count_entries", "getting number of entries returned from LDAP query");
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
            return obtainFatalResultCodeAsStatus("ldap_first_entry",
                                                 "getting LDAP entry from results");
        }

        while (entry) {
            // ldap_get_dn returns an owned pointer to the DN of the provided entry. We must free it
            // with ldap_memfree. If ldap_get_dn experiences an error it will return NULL and set an
            // error code.
            LibraryCharPtr entryDN = S::ldap_get_dn(_session, entry);
            const auto entryDNGuard = MakeGuard([&] { S::ldap_memfree(entryDN); });
            if (!entryDN) {
                return obtainFatalResultCodeAsStatus("ldap_get_dn",
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
            for (; attribute; attribute = ldap_next_attribute(_session, entry, element)) {
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
                    Status status = resultCodeToStatus(
                        "ldap_get_values_len",
                        "extracting values for attribute, after successful LDAP query");
                    if (status.isOK()) {
                        // libldap can return NULL when encountering an attribute with no values.
                        // This is not an error, and we should just skip parsing the attribute.
                        continue;
                    } else {
                        return status;
                    }
                }
                const auto valuesGuard = MakeGuard([&] { S::ldap_value_free_len(values); });

                LDAPAttributeValues valueStore;

                std::transform(LDAPArrayIterator<berval*>(values),
                               LDAPArrayIterator<berval*>(),
                               std::back_inserter(valueStore),
                               [](const auto* value) {
                                   return LDAPAttributeValue(value->bv_val, value->bv_len);
                               });

                attributeValueMap.emplace(LDAPAttributeKey(S::toNativeString(attribute)),
                                          std::move(valueStore));
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
