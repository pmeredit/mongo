/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "regex_rewrite_rule.h"

#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

StatusWith<RegexRewriteRule> RegexRewriteRule::create(const std::string& strMatch,
                                                      std::string substitution) {
    pcrecpp::RE match(strMatch, pcrecpp::UTF8());

    if (!match.error().empty()) {
        return {ErrorCodes::FailedToParse,
                str::stream() << "Error parsing in RegexRewriteRule. Expression was: \"" << strMatch
                              << "\". Error was: \"" << match.error() << "\"."};
    }

    std::string stringRepresentation = str::stream()
        << "{ match: \"" << strMatch << "\" substitution: \"" << substitution << "\" }";

    return RegexRewriteRule{
        std::move(match), std::move(substitution), std::move(stringRepresentation)};
}

#if defined(_WIN32) && _MSC_VER < 1900
RegexRewriteRule::RegexRewriteRule(RegexRewriteRule&& rr)
    : _match(std::move(rr._match)),
      _substitution(std::move(rr._substitution)),
      _stringRepresentation(std::move(rr._stringRepresentation)) {}

RegexRewriteRule& RegexRewriteRule::operator=(RegexRewriteRule&& rr) {
    _match = std::move(rr._match);
    _substitution = std::move(rr._substitution);
    _stringRepresentation = std::move(rr._stringRepresentation);
    return *this;
}
#else
RegexRewriteRule::RegexRewriteRule(RegexRewriteRule&& rr) = default;
RegexRewriteRule& RegexRewriteRule::operator=(RegexRewriteRule&& rr) = default;
#endif


RegexRewriteRule::RegexRewriteRule(pcrecpp::RE match,
                                   std::string substitution,
                                   std::string stringRepresentation)
    : _match(std::move(match)),
      _substitution(std::move(substitution)),
      _stringRepresentation(std::move(stringRepresentation)) {}

StatusWith<std::string> RegexRewriteRule::resolve(StringData input) const {
    StatusWith<std::vector<std::string>> swExtractedMatches = _extractMatches(_match, input);
    if (!swExtractedMatches.isOK()) {
        return swExtractedMatches.getStatus();
    }
    auto components = std::move(swExtractedMatches.getValue());

    std::string output = _substitution;
    for (size_t i = 0; i < components.size(); ++i) {
        std::string matchTarget = mongoutils::str::stream() << "\\{" << i << "\\}";
        pcrecpp::RE matchExp(std::move(matchTarget));
        if (matchExp.GlobalReplace(pcrecpp::StringPiece(components[i]), &output) < 0) {
            return Status(ErrorCodes::FailedToParse, "Unable to perform regex substitution");
        }
    }
    return output;
}

const StringData RegexRewriteRule::toStringData() const {
    return StringData(_stringRepresentation);
}
}  // namespace mongo
