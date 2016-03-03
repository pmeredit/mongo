/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <bt_types.h>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/builder.h"

namespace mongo {
namespace fts {

class RlpEnvironment;

/**
 * RlpStringBuffer
 *
 * Provides reusable buffer for converting UTF-16 strings from RLP to UTF-8
 */
class RlpStringBuffer {
    MONGO_DISALLOW_COPYING(RlpStringBuffer);

private:
    const size_t kInitialSize = 32;

public:
    RlpStringBuffer(RlpEnvironment* rlpEnvironment)
        : _rlpEnvironment(rlpEnvironment), _dynamicBuf(kInitialSize) {}

    /**
     * Store a UTF-8 version of the UTF-16 string parameter
     */
    void assign(const BT_Char16* str, size_t len, bool removeDiacritics);

    /**
     * Accesses the result of the conversion in assign.
     * Lifetime is bound to both RlpStringBuffer, and the next call to assign.
     */
    StringData getStringData() const {
        return _stringData;
    }

private:
    RlpEnvironment* const _rlpEnvironment;
    std::vector<char> _dynamicBuf;
    StackBufBuilder _buffer;
    StringData _stringData;
};

}  // namespace fts
}  // namespace mongo
