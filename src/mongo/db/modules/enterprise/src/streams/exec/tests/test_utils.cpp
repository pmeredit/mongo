/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/tests/test_utils.h"
#include "mongo/db/matcher/parsed_match_expression_for_test.h"
#include "streams/exec/parser.h"
#include "streams/exec/test_constants.h"

using namespace mongo;

namespace streams {

BSONObj TestUtils::getTestLogSinkSpec() {
    return BSON(Parser::kEmitStageName << BSON(kTestTypeLogToken << 1));
}

BSONObj TestUtils::getTestMemorySinkSpec() {
    return BSON(Parser::kEmitStageName << BSON(kTestTypeMemoryToken << 1));
}

BSONObj TestUtils::getTestSourceSpec() {
    return BSON(Parser::kSourceStageName << BSON("name"
                                                 << "_test" << kTestTypeMemoryToken << 1));
}

};  // namespace streams
