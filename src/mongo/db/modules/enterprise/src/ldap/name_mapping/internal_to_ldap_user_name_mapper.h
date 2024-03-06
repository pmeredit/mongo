/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <vector>


#include "rewrite_rule.h"

namespace mongo {
class BSONObj;
class LDAPRunner;
template <typename T>
class StatusWith;
class StringData;

/**
 * Manipulate an input string into an alternate representation based off a sequence of rules.
 * This is intended to transform a username used to authenticate to MongoDB into a name which
 * the LDAP server will recognize. This is necessary because, for example, a user may be
 * authenticating via an auth mechanism like Kerberos. The Kerberos principal will need to be
 * mapped to an LDAP DN before group acquisition can occur.
 */
class InternalToLDAPUserNameMapper {
    InternalToLDAPUserNameMapper(const InternalToLDAPUserNameMapper&) = delete;
    InternalToLDAPUserNameMapper& operator=(const InternalToLDAPUserNameMapper&) = delete;

public:
    InternalToLDAPUserNameMapper(InternalToLDAPUserNameMapper&& other);
    InternalToLDAPUserNameMapper& operator=(InternalToLDAPUserNameMapper&& other);

    /**
     * Given a string input, tranform input into result given provided rules.
     * The input will be tested against each rule in turn. If the rule matches the input, its result
     * will be returned by this method.
     * If the rule does not match, the next rule will be tried. If no rules remain, the
     * transformation will fail.
     */
    StatusWith<std::string> transform(LDAPRunner* runner, StringData input) const;

    /**
     * Factory function which generates a new InternalToLDAPUserNameMapper.
     *
     * This function accepts a BSON configuration object or array of such objects,
     * and creates and loads the resulting transformation rules into a new
     * InternalToLDAPUserNameMapper.
     *
     * The configuration objects can be two different types of documents. The first describes
     * a transformation using regular expressions and produces a RegexRewriteRule object in
     * the mapper. This document looks like:
     *
     * {
     *   match: <regex string>
     *   substitution: <string>
     * }
     *
     * The second type of document describes a LDAP query backed rule and produces an
     * LDAPRewriteRule. This document looks like:
     *
     * {
     *   match: <regex string>
     *   ldapQuery: <LDAP query>
     * }
     *
     * @param userToDNMapping The configuration containing document(s) describing rules
     * @return An error upon failure, or an InternalToLDAPUserNameMapper on success
     */
    static StatusWith<InternalToLDAPUserNameMapper> createNameMapper(std::string userToDNMapping);

    std::string toString() const {
        return _userToDNMapping;
    }

private:
    InternalToLDAPUserNameMapper(std::vector<std::unique_ptr<RewriteRule>> rules,
                                 std::string userToDNMapping);

    std::vector<std::unique_ptr<RewriteRule>> _transformations;
    std::string _userToDNMapping;
};

}  // namespace mongo
