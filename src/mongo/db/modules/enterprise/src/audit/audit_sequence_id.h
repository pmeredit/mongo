/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/data_range.h"
#include "mongo/base/data_range_cursor.h"

namespace mongo {
namespace audit {

/**
 * Represents an incrementing 96-bit sequence counter that
 * is used as an initialization vector in encrypting audit logs.
 * Implements methods to serialize and deserialize to/from a raw
 * byte buffer.
 */
class AuditSequenceID {
public:
    AuditSequenceID(std::uint32_t hi = 0, std::uint64_t lo = 0) : _seqHigh(hi), _seqLow(lo) {}

    AuditSequenceID& operator++() {
        // This invariant prevents wraparound to zero of the low 64-bits
        // (the invocation count) when the max value is reached.
        // This is so that the audit encryptor will not start reusing
        // IVs in the unlikely event that all 2^64 values have been used.
        invariant(_seqLow < std::numeric_limits<std::uint64_t>::max());
        _seqLow++;
        return *this;
    }

    bool operator==(const AuditSequenceID& other) const {
        return std::tie(other._seqHigh, other._seqLow) == std::tie(_seqHigh, _seqLow);
    }
    bool operator!=(const AuditSequenceID& other) const {
        return !(*this == other);
    }

    void serialize(DataRange outBuf) {
        uassert(ErrorCodes::InvalidLength,
                "Audit sequence ID output buffer size is too small",
                outBuf.length() >= kSerializedAuditSequenceIDSize);
        auto writer = DataRangeCursor(outBuf);
        writer.writeAndAdvance<LittleEndian<std::uint32_t>>(_seqHigh);
        writer.writeAndAdvance<LittleEndian<std::uint64_t>>(_seqLow);
    }

    static AuditSequenceID deserialize(ConstDataRange inBuf) {
        AuditSequenceID parsed;
        uassert(ErrorCodes::InvalidLength,
                "Audit sequence ID input buffer has invalid length",
                inBuf.length() >= kSerializedAuditSequenceIDSize);
        auto reader = ConstDataRangeCursor(inBuf);
        parsed._seqHigh = reader.readAndAdvance<LittleEndian<std::uint32_t>>();
        parsed._seqLow = reader.readAndAdvance<LittleEndian<std::uint64_t>>();
        return parsed;
    }

    static constexpr auto kSerializedAuditSequenceIDSize =
        sizeof(std::uint32_t) + sizeof(std::uint64_t);

private:
    std::uint32_t _seqHigh;
    std::uint64_t _seqLow;
};

class AuditSequenceIDChecker {
public:
    explicit AuditSequenceIDChecker(AuditSequenceID startSeq) : _seq(startSeq) {}

    bool matchAndIncrement(ConstDataRange seqBuf) {
        // deserialize sequence buffer; throw on error
        AuditSequenceID otherSeq = AuditSequenceID::deserialize(seqBuf);

        // compare the deserialized sequence ID against expected sequence ID;
        // increment expected sequence ID on a match
        if (otherSeq == _seq) {
            ++_seq;
            return true;
        }
        return false;
    }

private:
    AuditSequenceID _seq;
};

}  // namespace audit
}  // namespace mongo
