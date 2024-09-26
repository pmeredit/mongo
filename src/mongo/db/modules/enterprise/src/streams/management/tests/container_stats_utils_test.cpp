/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <cstdio>  // For tmpnam, tmpfile
#include <fstream>

#include "mongo/unittest/assert.h"
#include "streams/management/container_group_stats_provider.h"

namespace streams {

using namespace mongo;

TEST(ContainerStatsUtilsTest, readInt64ValueFromFileTest) {
    char* tempFileName = tmpnam(nullptr);  // Generate a unique filename
    ASSERT_TRUE(tempFileName != nullptr);
    FILE* fl = fopen(tempFileName, "w");
    const int64_t value = 12345678901234;
    fprintf(fl, "%ld", value);
    fclose(fl);
    ASSERT_EQ(*readInt64ValueFromFile(std::string{tempFileName}), value);
    remove(tempFileName);

    // read from non existent file.
    auto val = readInt64ValueFromFile(std::string{tempFileName});
    ASSERT_FALSE(val);
    fl = fopen(tempFileName, "w");
    fclose(fl);
    // read from empty file.
    val = readInt64ValueFromFile(std::string{tempFileName});
    ASSERT_FALSE(val);
    remove(tempFileName);
    // read from an invalid file
    fl = fopen(tempFileName, "w");
    fprintf(fl, "hello");
    fclose(fl);
    // read from empty file.
    val = readInt64ValueFromFile(std::string{tempFileName});
    ASSERT_FALSE(val);
    remove(tempFileName);
}


TEST(ContainerStatsUtilsTest, readCgroupV2StatFileTest) {
    char* tempFileName = tmpnam(nullptr);  // Generate a unique filename
    ASSERT_TRUE(tempFileName != nullptr);
    FILE* fl = fopen(tempFileName, "w");
    fclose(fl);
    // read from empty file.
    auto val = readCgroupV2StatFile(std::string{tempFileName});
    ASSERT_FALSE(val);
    remove(tempFileName);

    // test basic.
    fl = fopen(tempFileName, "w");
    const int64_t value = 12345678901234;
    fprintf(fl, "usage_usec %ld", value);
    fclose(fl);
    val = readCgroupV2StatFile(std::string{tempFileName});
    ASSERT_TRUE(val);
    ASSERT_EQ(*val, value);
    // test a file with multiple lines.
    fl = fopen(tempFileName, "w");
    fprintf(fl, "system_usec %ld\n", 1234L);
    fprintf(fl, "usage_usec %ld\n", value);
    fprintf(fl, "period %ld\n", 5678L);
    fclose(fl);
    val = readCgroupV2StatFile(std::string{tempFileName});
    ASSERT_TRUE(val);
    ASSERT_EQ(*val, value);
    remove(tempFileName);
    // test invalid value.
    fl = fopen(tempFileName, "w");
    fprintf(fl, "usage_usec test1245");
    fclose(fl);
    val = readCgroupV2StatFile(std::string{tempFileName});
    ASSERT_FALSE(val);
    remove(tempFileName);
}

TEST(ContainerStatsTest, readCgroupV2MaxFileTest) {
    char* tempFileName = tmpnam(nullptr);  // Generate a unique filename
    ASSERT_TRUE(tempFileName != nullptr);
    FILE* fl = fopen(tempFileName, "w");
    fclose(fl);
    // read from empty file.
    auto val = readCgroupV2MaxFile(std::string{tempFileName});
    ASSERT_FALSE(val);
    remove(tempFileName);

    int64_t val1 = 12345L;
    int64_t val2 = 67890L;
    fl = fopen(tempFileName, "w");
    fprintf(fl, "%ld %ld", val1, val2);
    fclose(fl);
    val = readCgroupV2MaxFile(std::string{tempFileName});
    ASSERT_TRUE(val);
    ASSERT_EQ(std::get<0>(*val), val1);
    ASSERT_EQ(std::get<1>(*val), val2);
    remove(tempFileName);

    fl = fopen(tempFileName, "w");
    fprintf(fl, "max %ld", val1);
    fclose(fl);
    val = readCgroupV2MaxFile(std::string{tempFileName});
    ASSERT_FALSE(val);
    remove(tempFileName);
}

}  // namespace streams
