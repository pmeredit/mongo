/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <pcrecpp.h>
#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"

namespace mongo {
template <typename T>
class StatusWith;
class StringData;

/**
 * An interface for rules describing string transformations.
 *
 * Input is provided to a rule, and the rule checks in an implementation defined manner
 * if it applies to the input.
 * If it does not apply, it returns a bad status code.
 * If it does apply, it will perform an implementation specific operation, and return the
 * resulting string.
 */
class RewriteRule {
public:
    virtual ~RewriteRule() = default;

    /**
     * Transform input, or produce error status.
     * The nature of the transformation is implementation defined.
     *
     * The input will be a username presented to MongoDB. This has many different possible formats.
     * It could be a single word with no special characters, it could be a structured LDAP DN,
     * it could be an X.509 subject name, or a Kerberos principal.
     *
     * This function will try and transform it into a format which is useful in an LDAP query.
     *
     * @input The input string to transform
     */
    virtual StatusWith<std::string> resolve(StringData input) const = 0;

    /**
     * Produce a string representation of the rule
     */
    virtual const StringData toStringData() const = 0;

protected:
    StatusWith<std::vector<std::string>> _extractMatches(const pcrecpp::RE& match,
                                                         StringData input) const;
};

}  // namespace mongo
