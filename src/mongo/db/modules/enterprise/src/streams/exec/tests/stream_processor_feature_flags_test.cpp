/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "streams/exec/config_gen.h"
#include "streams/exec/feature_flag.h"
#include "streams/exec/stream_processor_feature_flags.h"


namespace streams {

using namespace mongo;

TEST(StreamProcessorFeatureFlags, FeatureFlagTests) {
    mongo::stdx::unordered_map<std::string, mongo::Value> featureFlags;

    featureFlags["longValue"] = mongo::Value(60 * 1000);
    featureFlags["boolValue"] = mongo::Value(true);
    featureFlags["doubleValue"] = mongo::Value(2.2);
    featureFlags["stringValue"] = mongo::Value(std::string("test"));
    featureFlags["stringValue2"] = mongo::Value(std::string("true"));

    StreamProcessorFeatureFlags spff{featureFlags,
                                     std::chrono::time_point<std::chrono::system_clock>::min()};

    FeatureFlagDefinition longValueDefinition{"longValue", "Long Value", mongo::Value(10000), {}};
    ASSERT_EQ(60000, spff.getFeatureFlagValue(longValueDefinition).getInt().get());

    FeatureFlagDefinition longValue3Definition{
        "longValueMissingString", "Long Value", mongo::Value(10000), {}};
    ASSERT_EQ(10000, spff.getFeatureFlagValue(longValue3Definition).getInt().get());

    FeatureFlagDefinition boolValueDefinition{"boolValue", "Boolean Value", mongo::Value(true), {}};
    ASSERT_TRUE(spff.getFeatureFlagValue(boolValueDefinition).getBool().get());

    FeatureFlagDefinition doubleValueDefinition{
        "doubleValue", "Double Value", mongo::Value(0.0), {}};
    ASSERT_EQ(spff.getFeatureFlagValue(doubleValueDefinition).getDouble().get(), 2.2);

    FeatureFlagDefinition stringValueDefinition{
        "stringValue", "String Value", mongo::Value(std::string("hello")), {}};
    ASSERT_EQ(spff.getFeatureFlagValue(stringValueDefinition).getString().get(), "test");


    FeatureFlagDefinition stringValue2Definition{
        "stringValue2", "String Value", mongo::Value(std::string("hello")), {}};
    ASSERT_EQ(spff.getFeatureFlagValue(stringValue2Definition).getString().get(), "true");

    FeatureFlagDefinition stringValueDefaultDefinition{
        "stringValueDefault", "String Value", mongo::Value(std::string("hello")), {}};
    ASSERT_EQ(spff.getFeatureFlagValue(stringValueDefaultDefinition).getString().get(), "hello");
    ASSERT_TRUE(spff.isOverridden(stringValue2Definition));
    ASSERT_FALSE(spff.isOverridden(stringValueDefaultDefinition));

    FeatureFlagDefinition tierSpecificDefinition{
        "tierDefault",
        "tier Default",
        mongo::Value(1),
        {{"SP10", mongo::Value(2)}, {"SP30", mongo::Value(3)}}};
    mongo::streams::gStreamsSppTier = "SP10";
    ASSERT_EQ(spff.getFeatureFlagValue(tierSpecificDefinition).getInt().get(), 2);
    mongo::streams::gStreamsSppTier = "SP30";
    ASSERT_EQ(spff.getFeatureFlagValue(tierSpecificDefinition).getInt().get(), 3);
    mongo::streams::gStreamsSppTier = "SPCanary";
    ASSERT_EQ(spff.getFeatureFlagValue(tierSpecificDefinition).getInt().get(), 1);
}

TEST(StreamProcessorFeatureFlags, ValidateFeatureFlagTests) {
    ASSERT_TRUE(FeatureFlags::validateFeatureFlag(FeatureFlags::kCheckpointDurationInMs.name,
                                                  mongo::Value(123456)));
    ASSERT_TRUE(FeatureFlags::validateFeatureFlag(FeatureFlags::kCheckpointDurationInMs.name,
                                                  mongo::Value((long long)12345678901234)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(FeatureFlags::kCheckpointDurationInMs.name,
                                                   mongo::Value(true)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(FeatureFlags::kCheckpointDurationInMs.name,
                                                   mongo::Value(1.234)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(FeatureFlags::kCheckpointDurationInMs.name,
                                                   mongo::Value(std::string{"somestring"})));

    ASSERT_TRUE(FeatureFlags::validateFeatureFlag(
        FeatureFlags::kUseExecutionPlanFromCheckpoint.name, mongo::Value(true)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(
        FeatureFlags::kUseExecutionPlanFromCheckpoint.name, mongo::Value(1)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(
        FeatureFlags::kUseExecutionPlanFromCheckpoint.name, mongo::Value(1.234)));
    ASSERT_FALSE(
        FeatureFlags::validateFeatureFlag(FeatureFlags::kUseExecutionPlanFromCheckpoint.name,
                                          mongo::Value(std::string{"somestring"})));

    ASSERT_TRUE(FeatureFlags::validateFeatureFlag(
        FeatureFlags::kSourceBufferPreallocationFraction.name, mongo::Value(0.5)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(
        FeatureFlags::kSourceBufferPreallocationFraction.name, mongo::Value(1)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(
        FeatureFlags::kSourceBufferPreallocationFraction.name, mongo::Value(true)));
    ASSERT_FALSE(
        FeatureFlags::validateFeatureFlag(FeatureFlags::kSourceBufferPreallocationFraction.name,
                                          mongo::Value(std::string{"somestring"})));

    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(FeatureFlags::kTestOnlyStringType.name,
                                                   mongo::Value(0.5)));
    ASSERT_FALSE(
        FeatureFlags::validateFeatureFlag(FeatureFlags::kTestOnlyStringType.name, mongo::Value(1)));
    ASSERT_FALSE(FeatureFlags::validateFeatureFlag(FeatureFlags::kTestOnlyStringType.name,
                                                   mongo::Value(true)));
    ASSERT_TRUE(FeatureFlags::validateFeatureFlag(FeatureFlags::kTestOnlyStringType.name,
                                                  mongo::Value(std::string{"somestring"})));

    ASSERT_TRUE(FeatureFlags::validateFeatureFlag("testFeatureFlag", mongo::Value(true)));
    ASSERT_TRUE(FeatureFlags::validateFeatureFlag("testFeatureFlag", mongo::Value(1)));
    ASSERT_TRUE(FeatureFlags::validateFeatureFlag("testFeatureFlag", mongo::Value(1.0)));
    ASSERT_TRUE(FeatureFlags::validateFeatureFlag("testFeatureFlag",
                                                  mongo::Value(std::string{"somestring"})));
}

}  // namespace streams
