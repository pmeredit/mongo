/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "regex_rewrite_rule.h"

#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

StatusWith<RegexRewriteRule> RegexRewriteRule::create(const std::string& strMatch,
                                                      std::string substitution) {
    pcrecpp::RE match(strMatch, pcrecpp::UTF8());

    if (!match.error().empty()) {
        return {ErrorCodes::FailedToParse,
                str::stream() << "Error parsing in RegexRewriteRule. Expression was: \"" << strMatch
                              << "\". Error was: \""
                              << match.error()
                              << "\"."};
    }

    std::string stringRepresentation = str::stream()
        << "{ match: \"" << strMatch << "\" substitution: \"" << substitution << "\" }";

    return RegexRewriteRule{
        std::move(match), std::move(substitution), std::move(stringRepresentation)};
}

RegexRewriteRule::RegexRewriteRule(RegexRewriteRule&& rr) = default;
RegexRewriteRule& RegexRewriteRule::operator=(RegexRewriteRule&& rr) = default;

RegexRewriteRule::RegexRewriteRule(pcrecpp::RE match,
                                   std::string substitution,
                                   std::string stringRepresentation)
    : _match(std::move(match)),
      _substitution(std::move(substitution)),
      _stringRepresentation(std::move(stringRepresentation)) {}

StatusWith<std::string> RegexRewriteRule::resolve(OperationContext* txn, StringData input) const {
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
