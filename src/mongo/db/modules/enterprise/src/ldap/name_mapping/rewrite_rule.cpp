/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "rewrite_rule.h"

#include "mongo/base/string_data.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"

namespace mongo {
StatusWith<std::vector<std::string>> RewriteRule::_extractMatches(const pcrecpp::RE& match,
                                                                  StringData input) const {
    // pcrecpp's DoMatch method takes in an array of pointers to Arg objects. We need to create
    // that array, and the underlying Arg objects. Args wrap an underlying datatype,
    // std::strings in our case. So we need to create the std::strings too. DoMatch will then
    // populate these std::strings with the contents of the capture groups it matched.
    int numCaptureGroups = match.NumberOfCapturingGroups();
    if (numCaptureGroups < 0) {
        return Status(ErrorCodes::FailedToParse,
                      "Failed to extract captures with regular expression, because the regular "
                      "expression was not valid.");
    }
    std::vector<std::string> matches(numCaptureGroups);
    std::vector<pcrecpp::Arg> args(numCaptureGroups);
    std::vector<const pcrecpp::Arg*> argPtrs(numCaptureGroups);
    for (int i = 0; i < numCaptureGroups; ++i) {
        args[i] = &matches[i];
        argPtrs[i] = &args[i];
    }

    int consumed = 0;
    pcrecpp::StringPiece inputPiece(input.rawData(), input.size());

    if (!match.DoMatch(
            inputPiece, pcrecpp::RE::ANCHOR_BOTH, &consumed, argPtrs.data(), numCaptureGroups)) {
        return Status(ErrorCodes::FailedToParse,
                      "Regular expression did not match input string " + input.toString());
    }


    return matches;
}

}  // namespace mongo
