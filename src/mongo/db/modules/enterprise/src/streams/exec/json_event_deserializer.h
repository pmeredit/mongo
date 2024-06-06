/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/event_deserializer.h"

namespace streams {

/**
 * An event deserializer that can deserialize an input event that is in JSON format.
 */
class JsonEventDeserializer : public EventDeserializer {
public:
    struct Options {
        // Currently only set to false in operator_dag_bm.
        bool allowBsonCxxParsing{true};
        // Currently only set to true in operator_dag_bm.
        bool forceBsonCxxParsing{false};
    };

    JsonEventDeserializer() {}
    // This constructor is currently only used in operator_dag_bm.
    JsonEventDeserializer(Options options) : _options(std::move(options)) {}

private:
    mongo::BSONObj doDeserialize(const char* buf, int len) override;
    Options _options;
};

}  // namespace streams
