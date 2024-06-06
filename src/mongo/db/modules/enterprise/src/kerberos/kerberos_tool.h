/**
 *  Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <vector>

#include "kerberos_tool_options.h"

#if !defined(_WIN32)
#include <krb5.h>
#include <profile.h>
#endif

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/builder.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

class UniqueCStringPointer {
public:
    ~UniqueCStringPointer() {
        std::free(p);
    }
    UniqueCStringPointer() = default;
    UniqueCStringPointer& operator=(UniqueCStringPointer) = delete;
    UniqueCStringPointer& operator=(UniqueCStringPointer&&) = delete;
    UniqueCStringPointer(UniqueCStringPointer&) = delete;
    UniqueCStringPointer(UniqueCStringPointer&&) = delete;
    char* get() const {
        return p;
    }
    char** getPtr() {
        return &p;
    }

private:
    char* p = nullptr;
};

// constants for looking up environment variables
constexpr StringData kKRB5CCNAME = "KRB5CCNAME"_sd;
constexpr StringData kKRB5_KTNAME = "KRB5_KTNAME"_sd;
constexpr StringData kKRB5_CONFIG = "KRB5_CONFIG"_sd;
constexpr StringData kKRB5_TRACE = "KRB5_TRACE"_sd;
constexpr StringData kKRB5_CLIENT_KTNAME = "KRB5_CLIENT_KTNAME"_sd;

// KRB5 Macros that didn't make their way into the lib headers...
constexpr StringData KRB5_CONF_LIBDEFAULTS = "libdefaults"_sd;
constexpr StringData KRB5_CONF_RDNS = "rdns"_sd;

/**
 * Represents an easy-to-digest interface to a KRB5 keytab entry
 * Claims ownership of rawEntry, so must be kept alive if data in rawEntry is to be used
 * outside of this class (however, it is not recommended to do this, since this class is made
 * for looking at krb5_keytab_entry objects)
 */
#if !defined(_WIN32)
class KRB5KeytabEntry {
public:
    KRB5KeytabEntry(krb5_context krb5Context, krb5_keytab_entry rawEntry)
        : _rawEntry(rawEntry), _krb5Context(krb5Context) {}

    KRB5KeytabEntry(KRB5KeytabEntry&& other) noexcept
        : _rawEntry(other._rawEntry), _krb5Context(other._krb5Context) {
        other._krb5Context = nullptr;
    }

    ~KRB5KeytabEntry() {
        krb5_error_code error = 0;
        if (_krb5Context != nullptr) {
#ifdef MONGO_KRB5_HAVE_FEATURES
            error = krb5_free_keytab_entry_contents(_krb5Context, &_rawEntry);
#else
            error = 0;
            krb5_free_data_contents(_krb5Context, reinterpret_cast<krb5_data*>(&_rawEntry));
#endif
            if (error != 0) {
                std::cout << "Could not properly free krb5 keytab entry contents (error " << error
                          << ")" << std::endl;
            }
        }
    }

    /**
     * Returns the raw krb5_principal associated with this keytab entry
     */
    krb5_principal getPrincipal() const {
        return _rawEntry.principal;
    }

    /**
     * Returns the name of the principal associated with this keytab entry
     */
    StringData getPrincipalName() const {
        return krb5_princ_name(_krb5Context, _rawEntry.principal)->data;
    }

    /**
     * Returns a parsed string representation of the principal contained by this keytab entry
     */
    std::string getParsedPrincipal() const {
        char* parsed = nullptr;
        krb5_unparse_name(_krb5Context, getPrincipal(), &parsed);
        ScopeGuard guard = [&parsed, this] {
            if (parsed != nullptr) {
                krb5_free_unparsed_name(_krb5Context, parsed);
            }
        };
        return parsed;
    }

    krb5_kvno getKvno() const {
        return _rawEntry.vno;
    }

    /**
     * Returns the host name used by the principal contained in this keytab entry
     */
    StringData getPrincipalHost() const {
        return krb5_princ_component(_krb5Context, _rawEntry.principal, 1)->data;
    }

private:
    krb5_keytab_entry _rawEntry;
    // _krb5Context is NOT owned by this class
    krb5_context _krb5Context = nullptr;
};

/**
 * Represents an easy-to-digest interface to a KRB5 Keytab. It can be iterated over in sequence
 * like a list, and manages its own destruction of obnoxious heap objects
 */
class KRB5Keytab {
public:
    KRB5Keytab(krb5_context krb5Context, KerberosToolOptions::ConnectionType connectionType);

    KRB5Keytab() = default;

    KRB5Keytab(KRB5Keytab&& other) noexcept
        : _krb5Context(other._krb5Context),
          _krb5Keytab(other._krb5Keytab),
          _keytabEntries(std::move(other._keytabEntries)) {
        other._krb5Keytab = nullptr;
        other._krb5Context = nullptr;
    }

    ~KRB5Keytab() {
        if (_krb5Keytab != nullptr) {
            krb5_kt_close(_krb5Context, _krb5Keytab);
        }
    }

    auto begin() const {
        return _keytabEntries.begin();
    }

    auto end() const {
        return _keytabEntries.end();
    }

    boost::optional<std::string> getName() const {
        if (_krb5Keytab == nullptr) {
            return boost::none;
        }
        char ktNameBuf[MAX_KEYTAB_NAME_LEN];
        krb5_kt_get_name(_krb5Context, _krb5Keytab, ktNameBuf, MAX_KEYTAB_NAME_LEN);
        return std::string(ktNameBuf);
    }

#ifdef MONGO_KRB5_HAVE_FEATURES
    bool existsAndIsPopulated() const {
        return krb5_kt_have_content(_krb5Context, _krb5Keytab) == 0;
    }
#else
    bool existsAndIsPopulated() const {
        if (_krb5Keytab == nullptr) {
            return false;
        }
        krb5_error_code error;
        krb5_kt_cursor cursor = nullptr;
        ScopeGuard cursorGuard = [&cursor, this] {
            if (cursor != nullptr && _krb5Keytab != nullptr) {
                krb5_kt_end_seq_get(_krb5Context, _krb5Keytab, &cursor);
            }
        };
        krb5_keytab_entry entry;
        ScopeGuard entryGuard = [&entry, this] {
            krb5_free_data_contents(_krb5Context, reinterpret_cast<krb5_data*>(&entry));
        };

        // see if it is possible to get at least one entry via iteration
        error = krb5_kt_start_seq_get(_krb5Context, _krb5Keytab, &cursor);
        if (error == KRB5_KT_NOTFOUND) {
            return false;
        }
        error = krb5_kt_next_entry(_krb5Context, _krb5Keytab, &entry, &cursor);
        return error == 0;
    }
#endif

private:
    // _krb5Context is NOT owned by this class
    krb5_context _krb5Context = nullptr;
    krb5_keytab _krb5Keytab = nullptr;
    std::list<KRB5KeytabEntry> _keytabEntries;
};

/**
 * Represents an easy-to-digest interface to a KRB5 Credentials struct
 * Claims ownership of rawCreds, so must be kept alive if data in rawCreds is to be used
 * outside of this class (however, it is not recommended to do this, since this class is made
 * for looking at krb5_creds objects)
 */
class KRB5Credentials {
public:
    KRB5Credentials(krb5_context krb5Context, krb5_creds rawCreds)
        : _rawCreds(rawCreds), _krb5Context(krb5Context) {}

    KRB5Credentials(KRB5Credentials&& other) noexcept
        : _rawCreds(other._rawCreds), _krb5Context(other._krb5Context) {
        other._krb5Context = nullptr;
    }

    ~KRB5Credentials() {
        if (_krb5Context != nullptr) {
            krb5_free_cred_contents(_krb5Context, &_rawCreds);
        }
    }

    /**
     * Returns a pointer to the krb5_creds struct that represents a credentials cache entry
     */
    const krb5_creds* rawCreds() const {
        return &_rawCreds;
    }

    /**
     * Returns the client name in the credentials cache entry in terms of client@REALM
     */
    StringData getClientPrincipalName() const {
        return _rawCreds.client->data->data;
    }

private:
    krb5_creds _rawCreds;
    // _krb5Context is NOT owned by this class
    krb5_context _krb5Context = nullptr;
};

class KRB5CredentialsCache {
public:
    explicit KRB5CredentialsCache(krb5_context krb5Context) : _krb5Context(krb5Context) {
        // cursor for iterating over all available ccaches
        krb5_cccol_cursor krb5cccolCursor = nullptr;
        ScopeGuard ccolFree = [&] {
            if (krb5cccolCursor != nullptr) {
                uassert(31345,
                        "Could not properly free Credentials Cache Collection cursor.",
                        krb5_cccol_cursor_free(_krb5Context, &krb5cccolCursor) == 0);
            }
        };
        uassert(31333,
                "Could not resolve credentials cache collection",
                krb5_cccol_cursor_new(_krb5Context, &krb5cccolCursor) == 0);


        // retains the current ccache being accessed during iteration
        krb5_ccache krb5CCache = nullptr;
        ScopeGuard ccClose = [&] {
            if (krb5CCache != nullptr) {
                uassert(31356,
                        "Could not properly close credentials cache.",
                        krb5_cc_close(_krb5Context, krb5CCache) == 0);
            }
        };

        while ((krb5_cccol_cursor_next(_krb5Context, krb5cccolCursor, &krb5CCache) == 0) &&
               krb5CCache != nullptr) {

            ScopeGuard ccLoopClose = [&] {
                if (krb5CCache != nullptr) {
                    uassert(31342,
                            "Could not properly close credentials cache.",
                            krb5_cc_close(_krb5Context, krb5CCache) == 0);
                }
            };

            // cursor for iterating over all entries in one ccache
            krb5_cc_cursor krb5ccCursor = nullptr;
            ScopeGuard endSeq = [&] {
                if (krb5ccCursor != nullptr) {
                    uassert(31355,
                            "Could not properly end sequence across credentials cache.",
                            krb5_cc_end_seq_get(_krb5Context, krb5CCache, &krb5ccCursor) == 0);
                }
            };

            // points to one credentials entry in a ccache
            krb5_creds krb5Creds;
            uassert(31340,
                    "Could not resolve credentials cache",
                    krb5_cc_start_seq_get(_krb5Context, krb5CCache, &krb5ccCursor) == 0);

            krb5_error_code errorCode;
            while ((errorCode = krb5_cc_next_cred(
                        _krb5Context, krb5CCache, &krb5ccCursor, &krb5Creds)) != KRB5_CC_END) {
                uassert(31341, "Could not iterate between credentials in cache", errorCode == 0);
                _credentialSet.emplace(_krb5Context, krb5Creds);
            }
        }
        // if we successfully ran through the loop, the CCache handle should have already been freed
        ccClose.dismiss();
    }

    KRB5CredentialsCache(KRB5CredentialsCache&& other) noexcept
        : _credentialSet(std::move(other._credentialSet)), _krb5Context(other._krb5Context) {
        other._krb5Context = nullptr;
    }

    auto begin() const {
        return _credentialSet.begin();
    }

    auto end() const {
        return _credentialSet.end();
    }

private:
    // holds on to credentials from the credentials cache collection
    struct _cmpCreds {
        bool operator()(const KRB5Credentials& a, const KRB5Credentials& b) const {
            return a.rawCreds() < b.rawCreds();
        }
    };
    std::set<KRB5Credentials, _cmpCreds> _credentialSet;

    // _krb5Context is NOT owned by this class
    krb5_context _krb5Context = nullptr;
};

/**
 * This class is used to represent a KRB5 configuration profile (i.e. information from krb5.conf) as
 * a tree.
 */
class KRB5Profile {
public:
    static KRB5Profile create(krb5_context krb5Context) {
        profile_t profile;
        krb5_get_profile(krb5Context, &profile);
        return KRB5Profile(profile, ProfileTree::create(profile));
    }

    KRB5Profile(KRB5Profile&& other) noexcept
        : _profile(other._profile), _profileTree(std::move(other._profileTree)) {
        other._profile = nullptr;
    }

    ~KRB5Profile() {
        if (_profile != nullptr) {
            profile_release(_profile);
        }
    }

    std::string toString() const {
        StringBuilder sb;
        _profileTree.toString(&sb);
        return sb.str();
    }

    /**
     * Returns: true = enabled, false = disabled, boost::none = not set in krb5.conf
     * Note that MIT Kerberos defines the default state of rdns to be enabled, but we define this
     * ternary state to know when the user has not explicitly set this option in their config.
     */
    boost::optional<bool> rdnsState() const {
        constexpr int DEFAULT_RDNS_LOOKUP = -1;
        int state;
        profile_get_boolean(_profile,
                            KRB5_CONF_LIBDEFAULTS.rawData(),
                            KRB5_CONF_RDNS.rawData(),
                            nullptr,
                            DEFAULT_RDNS_LOOKUP,
                            &state);
        if (state == DEFAULT_RDNS_LOOKUP) {
            return boost::none;
        }
        return state;
    }

private:
    /**
     * Describes a subsection or relation in the KRB5 profile, including support for nested
     * subsections.
     *
     * profile: a KRB5 profile returned by krb5_get_profile, not owned by this class.
     * path: describes the path through the tree to reach this node, ending with its key.
     * value: the value associated with the key i.e. the right side of the equals sign.
     */
    class ProfileNode {
    public:
        explicit ProfileNode(profile_t profile, std::vector<std::string> path, std::string value)
            : _key(path.back()), _value(std::move(value)), _profile(profile) {
            KRB5Profile::resolveProfileNode(
                profile, path, [this](std::vector<std::string> childPath, const char* value) {
                    addChildNode(std::move(childPath), value == nullptr ? "" : value);
                });
        }

        ProfileNode() = default;

        void toString(StringBuilder* sb, std::string padding = "") const {
            // if in top-level section
            if (padding.empty()) {
                sb->append("[");
                sb->append(_key);
                sb->append("]\n");
                padding.push_back('\t');
                for (auto& subsectionPair : _subsections) {
                    subsectionPair.second.toString(sb, padding);
                }
                padding.pop_back();
                sb->append(padding);
            } else {
                sb->append(padding);
                sb->append(_key);
                sb->append(" = ");
                if (!hasSubsections()) {
                    if (_value.empty()) {
                        sb->append("\"\"");
                    } else {
                        sb->append(_value);
                    }
                    sb->append("\n");
                } else {
                    sb->append("{\n");
                    padding.push_back('\t');
                    for (auto& subsectionPair : _subsections) {
                        subsectionPair.second.toString(sb, padding);
                    }
                    padding.pop_back();
                    padding.append("}\n");
                    sb->append(padding);
                }
            }
        }

    private:
        /**
         * Adds a new subsection to this node
         */
        void addChildNode(std::vector<std::string> path, std::string value) {
            _subsections.try_emplace(path.back(), _profile, path, std::move(value));
        }

        /**
         * Tells if this node in the KRB5 profile has any nested subsections as children
         */
        bool hasSubsections() const {
            return !_subsections.empty();
        }

        std::string _key;
        std::string _value;
        std::map<std::string, ProfileNode> _subsections;
        profile_t _profile{};
    };

    /**
     * Provides an interface into a KRB5 profile by section
     */
    class ProfileTree {
    public:
        static ProfileTree create(profile_t profile) {
            return ProfileTree(profile);
        }

        void addSection(std::string sectionName) {
            std::vector<std::string> path{sectionName};
            _sections.try_emplace(sectionName, _profile, path, "");
        }

        void toString(StringBuilder* sb) const {
            for (const auto& section : _sections) {
                section.second.toString(sb);
                sb->append("\n");
            }
        }

    private:
        explicit ProfileTree(profile_t profile) : _profile(profile) {
            std::vector<std::string> emptyPath{};
            KRB5Profile::resolveProfileNode(
                profile, emptyPath, [&](std::vector<std::string> path, const char* unused) {
                    addSection(path.back());
                });
        }

        std::map<std::string, ProfileNode> _sections;
        profile_t _profile;
    };

    KRB5Profile(profile_t profile, ProfileTree profileTree)
        : _profile(profile), _profileTree(std::move(profileTree)) {}

    /**
     * Given a path to a section, or subsection, in a KRB5 profile, this function will crawl the
     * provided path on the KRB5 profile, fetch the value for the relation at the end of the path
     * (or "" if there is actually a subsection there) and call the provided callback with this
     * information so a node can be constructed in the ProfileTree
     *
     * childAdder is a functor that describes what function should be used to emplace the
     * ProfileNode. It takes in a path to a child, and the value of the relation at that path. If
     * the path leads to a subsection, the value should be an empty string.
     */
    template <typename Functor>
    static void resolveProfileNode(profile_t profile,
                                   std::vector<std::string>& path,
                                   Functor childAdder) {
        std::vector<char*> names;
        for (const auto& subsection : path) {
            names.emplace_back(const_cast<char*>(subsection.c_str()));
        }
        names.emplace_back(nullptr);

        void* iter = nullptr;
        ScopeGuard iterGuard = [&iter] {
            if (iter != nullptr) {
                profile_iterator_free(&iter);
            }
        };
        profile_iterator_create(profile, names.data(), PROFILE_ITER_LIST_SECTION, &iter);
        long ret = 0;
        while (ret == 0) {
            UniqueCStringPointer name;
            UniqueCStringPointer value;
            // iterate to the next item in the profile
            // nullptr has to be passed to receive the name of a top-level section
            ret = profile_iterator(&iter, name.getPtr(), (path.empty() ? nullptr : value.getPtr()));
            // if we are not at a leaf
            if (name.get() != nullptr) {
                // store subsection as child
                path.emplace_back(name.get());
                ScopeGuard pathPop = [&path] { path.pop_back(); };
                childAdder(path, value.get());
            }
        }
    }

    profile_t _profile{};
    ProfileTree _profileTree;
};
#endif

/**
 * This class is an interface to interacting with the Kerberos API in a more modern-C++y way.
 * It provides accessor methods and manages memory/lifetime of kerberos C structs.
 */
class KerberosEnvironment {

public:
#ifdef _WIN32
    static StatusWith<KerberosEnvironment> create(
        KerberosToolOptions::ConnectionType connectionType) {
        return KerberosEnvironment(connectionType);
    }

#else
    static StatusWith<KerberosEnvironment> create(
        KerberosToolOptions::ConnectionType connectionType) {
        krb5_context context;
        krb5_error_code error = krb5_init_context(&context);
        if (error != 0) {
            const char* rawError = krb5_get_error_message(context, error);
            ScopeGuard freeError = [&] { krb5_free_error_message(context, rawError); };
            std::string errorMessage = rawError;
            return Status(ErrorCodes::BadValue, errorMessage);
        }
        return KerberosEnvironment(context, connectionType);
    }

    KerberosEnvironment(KerberosEnvironment&& other) noexcept
        : _variables(std::move(other._variables)),
          _krb5Context(other._krb5Context),
          _krb5Profile(std::move(other._krb5Profile)),
          _keytab(std::move(other._keytab)),
          _credentialsCache(std::move(other._credentialsCache)),
          _connectionType(other._connectionType) {
        other._krb5Context = nullptr;
    }

    ~KerberosEnvironment() {
        if (_krb5Context != nullptr) {
            krb5_free_context(_krb5Context);
        }
    }

    const std::map<StringData, std::string>& variables() const {
        return _variables;
    }

    boost::optional<std::string> getKeytabName() const {
        return _keytab.getName();
    }

    const KRB5Profile& getProfile() const {
        return _krb5Profile;
    }

    StringData getConfigPath() const {
        return _variables.at(kKRB5_CONFIG);
    }

    StringData getTraceLocation() const {
        return _variables.at(kKRB5_TRACE);
    }

    bool keytabExistsAndIsPopulated() const {
        return _keytab.existsAndIsPopulated();
    }

    /**
     * Checks if keytab contains an entry with a principal whose name is equal to parameter name.
     * The principal is in the format service/name@REALM
     */
    bool keytabContainsPrincipalWithName(StringData name) const {
        return std::any_of(_keytab.begin(), _keytab.end(), [&name](const KRB5KeytabEntry& ktEntry) {
            return ktEntry.getPrincipalName() == name;
        });
    }

    /**
     * Checks if credentials cache contains an entry with a principal whose name is equal to
     * parameter name the principal is in the format service/hostname@REALM
     */
    bool credentialsCacheContainsClientPrincipalName(StringData name) const {
        return std::any_of(_credentialsCache.begin(),
                           _credentialsCache.end(),
                           [&name](const KRB5Credentials& creds) {
                               return creds.getClientPrincipalName() == name;
                           });
    }

    /**
     * Get all keytab entries that have the service name for MongoDB
     * The entries contained in the vector are owned by the KRB5Keytab that is owned by this
     * KerberosEnvironment object, so they both have to be alive as long as the entries are needed.
     */
    std::vector<const KRB5KeytabEntry*> keytabGetMongoDBEntries(
        StringData mongodbServiceName) const {
        std::vector<const KRB5KeytabEntry*> users;
        for (const auto& entry : _keytab) {
            if (entry.getPrincipalName() == mongodbServiceName) {
                users.emplace_back(&entry);
            }
        }
        return users;
    }

    bool isServerConnection() const {
        return _connectionType == KerberosToolOptions::ConnectionType::kServer;
    }

    bool isClientConnection() const {
        return _connectionType == KerberosToolOptions::ConnectionType::kClient;
    }
#endif

    // looks up the value of a given environment variable
    // returns empty string if the variable is not set
    static std::string getEnvironmentValue(StringData variable) {
        char* value;
        value = getenv(variable.rawData());
        if (value == nullptr) {
            return "";
        } else {
            return value;
        }
    }

private:
#ifdef _WIN32
    explicit KerberosEnvironment(KerberosToolOptions::ConnectionType connectionType)
        : _connectionType(connectionType) {}
#endif

#ifndef _WIN32
    explicit KerberosEnvironment(krb5_context krb5Context,
                                 KerberosToolOptions::ConnectionType connectionType);

    // stores environment variables and maps them to their value
    std::map<StringData, std::string> _variables;

    // required for interacting with the KRB5 API
    krb5_context _krb5Context = nullptr;

    // interface for configuration profile
    KRB5Profile _krb5Profile;

    // interface for keytab
    KRB5Keytab _keytab;

    // interface for credentials cache
    KRB5CredentialsCache _credentialsCache;
#endif

    // tells if this environment is for a client or server connection
    KerberosToolOptions::ConnectionType _connectionType;
};

}  // namespace mongo
