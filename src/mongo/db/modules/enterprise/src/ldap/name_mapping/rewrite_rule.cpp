/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "rewrite_rule.h"

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"

namespace mongo {
StatusWith<std::vector<std::string>> RewriteRule::_extractMatches(const pcre::Regex& re,
                                                                  StringData input) const {
    if (!re) {
        return Status(ErrorCodes::FailedToParse,
                      "Failed to extract captures with regular expression, because the regular "
                      "expression was not valid.");
    }
    auto m = re.matchView(input, pcre::ANCHORED | pcre::ENDANCHORED);
    if (!m) {
        return Status(ErrorCodes::FailedToParse,
                      "Regular expression did not match input string " + input);
    }
    return m.getCapturesStrings();
}
}  // namespace mongo
