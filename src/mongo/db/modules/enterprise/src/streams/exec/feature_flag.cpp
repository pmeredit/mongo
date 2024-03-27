/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/feature_flag.h"
#include "mongo/bson/bsontypes.h"

namespace streams {

boost::optional<int64_t> FeatureFlagValue::getInt() const {
    boost::optional<int64_t> returnValue;
    if (_value.getType() != mongo::BSONType::NumberInt &&
        _value.getType() != mongo::BSONType::NumberLong) {
        return returnValue;
    }
    returnValue.emplace(_value.coerceToLong());
    return returnValue;
}

boost::optional<bool> FeatureFlagValue::getBool() const {
    boost::optional<bool> returnValue;
    if (_value.getType() != mongo::BSONType::Bool) {
        return returnValue;
    }
    returnValue.emplace(_value.getBool());
    return returnValue;
}

boost::optional<std::string> FeatureFlagValue::getString() const {
    boost::optional<std::string> returnValue;
    if (_value.getType() != mongo::BSONType::String) {
        return returnValue;
    }
    returnValue.emplace(_value.getString());
    return returnValue;
}

boost::optional<double> FeatureFlagValue::getDouble() const {
    boost::optional<double> returnValue;
    if (_value.getType() != mongo::BSONType::NumberDouble) {
        return returnValue;
    }
    returnValue.emplace(_value.getDouble());
    return returnValue;
}

const FeatureFlagDefinition FeatureFlags::kCheckpointDurationInMs{
    "checkpointDuration", "checkpoint Duration in ms", mongo::Value(60 * 60 * 1000)};

}  // namespace streams
