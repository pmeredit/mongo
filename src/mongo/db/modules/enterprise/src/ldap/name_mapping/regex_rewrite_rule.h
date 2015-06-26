/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "rewrite_rule.h"

#include <pcrecpp.h>
#include <string>

namespace mongo {

template <typename T>
class StatusWith;

/**
 * RewriteRule implementation using regular expressions to transform inputs.
 *
 * This rewrite rule is matched if the input is matched by a regular expression.
 * It will transform the input by substituting the capture groups into a
 * substitution string, replacing the substrings ``{<capture group index>}'' with
 * the contents of the corresponding group.
 */
class RegexRewriteRule : public RewriteRule {
    MONGO_DISALLOW_COPYING(RegexRewriteRule);

public:
    RegexRewriteRule(RegexRewriteRule&& rr);
    RegexRewriteRule& operator=(RegexRewriteRule&& rr);

    /**
     * Factory function which returns an RegexRewriteRule, or an error
     *
     * @param match String representation of regular expression which will be matched against input
     * @param substitution String containing tokens to be replaced with capture groups
     */
    static StatusWith<RegexRewriteRule> create(const std::string& match, std::string substitution);

    /**
     * If the regular expression provided in 'match' matches 'input', all extracted capture groups
     * are substituted into 'substitution', and the result is returned. Otherwise, a non-OK Status
     * is returned.
     */
    StatusWith<std::string> resolve(StringData input) const final;

    const StringData toStringData() const final;

private:
    RegexRewriteRule(pcrecpp::RE match, std::string substitution, std::string stringRepresentation);

    pcrecpp::RE _match;
    std::string _substitution;          // The string which capture groups will be substituted into
    std::string _stringRepresentation;  // Stored for toStringData
};
}  // namespace mongo
