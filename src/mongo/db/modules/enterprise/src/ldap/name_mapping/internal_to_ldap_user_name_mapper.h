/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <vector>

#include "mongo/base/disallow_copying.h"

#include "rewrite_rule.h"

namespace mongo {
struct BSONArray;
class LDAPRunnerInterface;
class OperationContext;
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
    MONGO_DISALLOW_COPYING(InternalToLDAPUserNameMapper);

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
    StatusWith<std::string> transform(OperationContext* txn, StringData input) const;

    /**
     * Factory function which generates a new InternalToLDAPUserNameMapper.
     *
     * This function accepts a BSON configuration array and creates and loads resulting the
     * transformation rules into a new InternalToLDAPUserNameMapper.
     *
     * The configuration array contains two different types of documents. The first describes
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
     * @param config The BSON configuration array containing documents describing rules
     * @return An error upon failure, or an InternalToLDAPUserNameMapper on success
     */
    static StatusWith<InternalToLDAPUserNameMapper> createNameMapper(BSONArray config);

private:
    explicit InternalToLDAPUserNameMapper(std::vector<std::unique_ptr<RewriteRule>> rules);

    std::vector<std::unique_ptr<RewriteRule>> _transformations;
};

}  // namespace mongo
