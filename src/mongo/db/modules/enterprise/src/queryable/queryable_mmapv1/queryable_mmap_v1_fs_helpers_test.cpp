/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"

#include "queryable_mmap_v1_fs_helpers.h"

namespace mongo {
namespace queryable {
namespace {

class QueryableMmapV1Test : public unittest::Test {};

TEST_F(QueryableMmapV1Test, RemoveDirectoryTest) {
    ASSERT_EQ("db.ns", removeDirectory("db/db.ns"));
    ASSERT_EQ("db.ns", removeDirectory("db\\db.ns"));
    ASSERT_EQ("db.ns", removeDirectory("db.ns"));
}

TEST_F(QueryableMmapV1Test, MMAPV1DatafileRegexTest) {
    const std::string dbname = "db";
    std::smatch matcher;

    std::string filename = "db.0";
    ASSERT_TRUE(std::regex_match(filename, matcher, getMMAPV1DatafileRegex(dbname)));
    ASSERT_EQ("0", matcher[1].str());

    filename = "db/db.010";
    ASSERT_TRUE(std::regex_match(filename, matcher, getMMAPV1DatafileRegex(dbname)));
    ASSERT_EQ("010", matcher[1].str());

    filename = "test/db.0";
    ASSERT_FALSE(std::regex_match(filename, matcher, getMMAPV1DatafileRegex(dbname)));

    filename = "db0.0";
    ASSERT_FALSE(std::regex_match(filename, matcher, getMMAPV1DatafileRegex(dbname)));

    filename = "db..0";
    ASSERT_FALSE(std::regex_match(filename, matcher, getMMAPV1DatafileRegex(dbname)));

    filename = "db.ns";
    ASSERT_FALSE(std::regex_match(filename, matcher, getMMAPV1DatafileRegex(dbname)));
}

}  // namespace
}  // namespace queryable
}  // namespace mongo
