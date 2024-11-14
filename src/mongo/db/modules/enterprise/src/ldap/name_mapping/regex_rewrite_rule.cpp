/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "regex_rewrite_rule.h"

#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/errno_util.h"
#include "mongo/util/str.h"

namespace mongo {

StatusWith<RegexRewriteRule> RegexRewriteRule::create(const std::string& strMatch,
                                                      std::string substitution) {
    pcre::Regex match(strMatch, pcre::UTF);

    if (!match) {
        return {ErrorCodes::FailedToParse,
                str::stream() << "Error parsing in RegexRewriteRule. Expression was: \"" << strMatch
                              << "\". Error was: \"" << errorMessage(match.error()) << "\"."};
    }

    std::string stringRepresentation = str::stream()
        << "{ match: \"" << strMatch << "\" substitution: \"" << substitution << "\" }";

    return RegexRewriteRule{
        std::move(match), std::move(substitution), std::move(stringRepresentation)};
}

RegexRewriteRule::RegexRewriteRule(RegexRewriteRule&& rr) = default;
RegexRewriteRule& RegexRewriteRule::operator=(RegexRewriteRule&& rr) = default;

RegexRewriteRule::RegexRewriteRule(pcre::Regex match,
                                   std::string substitution,
                                   std::string stringRepresentation)
    : _match(std::move(match)),
      _substitution(std::move(substitution)),
      _stringRepresentation(std::move(stringRepresentation)) {}

/**
 * Strict string to number parsing.
 * Number may only have base-10 digits.
 * Number must not be negative or contain any whitespace.
 * Number must be smaller than "max" value.
 */
bool _parseGroup(StringData possibleNumber, std::size_t max, std::size_t* group) {
    std::size_t ret = 0;
    for (std::size_t digit = 0; digit < possibleNumber.size(); ++digit) {
        auto ch = possibleNumber[digit];
        if ((ch < '0') || (ch > '9')) {
            return false;
        }
        ret *= 10;
        ret += (ch - '0');
        if (ret >= max) {
            return false;
        }
    }

    *group = ret;
    return true;
}

StatusWith<std::string> RegexRewriteRule::resolve(
    LDAPRunner* runner,
    StringData input,
    TickSource* tickSource,
    const SharedUserAcquisitionStats& userAcquisitionStats) const {
    StatusWith<std::vector<std::string>> swExtractedMatches = _extractMatches(_match, input);
    if (!swExtractedMatches.isOK()) {
        return swExtractedMatches.getStatus();
    }
    auto components = std::move(swExtractedMatches.getValue());
    auto max = components.size();

    StringBuilder builder;
    StringData sub = _substitution;
    while (!sub.empty()) {
        auto open = sub.find('{');
        if (open == std::string::npos) {
            // No more subpattern groups. Consume all and finish.
            builder << sub;
            break;
        }

        auto close = sub.find('}', open);
        if (close == std::string::npos) {
            // No more subpattern groups. Consume all and finish.
            builder << sub;
            break;
        }

        std::size_t group = components.size();
        if (!_parseGroup(sub.substr(open + 1, close - (open + 1)), max, &group)) {
            // Not a valid subpattern group, consume up to the open brace and try again.
            builder << sub.substr(0, open + 1);
            sub = sub.substr(open + 1);
            continue;
        }

        // Valid subgroup. Copy in the preamble and substitue the find.
        invariant(group < max);
        builder << sub.substr(0, open) << components[group];

        // Continue after the last valid subgroup.
        sub = sub.substr(close + 1);
    }

    return builder.str();
}

StringData RegexRewriteRule::toStringData() const {
    return StringData(_stringRepresentation);
}
}  // namespace mongo
