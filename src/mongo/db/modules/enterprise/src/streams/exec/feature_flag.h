#pragma once
#include <string>

#include "mongo/db/exec/document_value/value.h"


namespace streams {

// FeatureFlagDefinition class used to fetch a feature flag value.
struct FeatureFlagDefinition {
    std::string name;
    std::string description;
    mongo::Value defaultValue;
};

// FeatureFlagValue is wrapper to mongo to return only expected type values or default value.
class FeatureFlagValue {
public:
    FeatureFlagValue(const mongo::Value& val) : _value(val) {}
    boost::optional<int64_t> getInt() const;
    boost::optional<bool> getBool() const;
    boost::optional<double> getDouble() const;
    boost::optional<std::string> getString() const;

private:
    mongo::Value _value;
};

// Empty class to limit scope of global FeatureFlagDefinitions.
class FeatureFlags {
public:
    static const FeatureFlagDefinition kCheckpointDurationInMs;
};

}  // namespace streams
