/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "rlp_stringbuffer.h"

#include <algorithm>

#include "mongo/db/fts/unicode/string.h"
#include "rlp_environment.h"

namespace mongo {
namespace fts {

void RlpStringBuffer::assign(const BT_Char16* str, size_t len, bool removeDiacritics) {
    if (str == nullptr) {
        _stringData = StringData();
        return;
    }

    while (true) {
        bool truncated;

        // Replace this with a better function that handles
        // UTF-16 -> UTF-8 validation errors in ICU
        size_t written = _rlpEnvironment->bt_xutf16toutf8_lengths(
            _dynamicBuf.data(), _dynamicBuf.size(), str, len, &truncated);

        if (!truncated) {
            // TODO: Add a constructor for unicode::String that takes UTF-16 as input to avoid
            // conversions and buffer copying.
            if (removeDiacritics) {
                // Add a null terminator so the buffer can be converted to a unicode string for
                // removing diacritics.
                if (_dynamicBuf.size() == written) {
                    _dynamicBuf.push_back(0);
                } else {
                    _dynamicBuf[written] = 0;
                }

                _stringData =
                    StringData(unicode::String(_dynamicBuf.data()).removeDiacritics().toString());
            } else {
                _stringData = StringData(_dynamicBuf.data(), written);
            }
            return;
        }

        // It is hard to calculate the size of a UTF-16 => UTF-8 ahead of time
        // RLP does not provide a mechanism to do it, and we do not have the libraries
        // aka, ICU to do it.
        _dynamicBuf.resize(_dynamicBuf.size() * 2);
    }
}

}  // namespace fts
}  // namespace mongo
