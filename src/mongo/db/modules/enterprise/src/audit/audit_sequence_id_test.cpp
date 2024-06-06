/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_sequence_id.h"

#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace audit {

class AuditSequenceIDTest : public mongo::unittest::Test {
public:
    AuditSequenceIDTest() {}
};

class AuditSequenceIDCheckerTest : public mongo::unittest::Test {
public:
    AuditSequenceIDCheckerTest() {}
};

TEST_F(AuditSequenceIDTest, IncrementOperatorTest) {
    AuditSequenceID seq;
    AuditSequenceID nextSeq(0, 1);

    ++seq;
    ASSERT_TRUE(seq == nextSeq);

    seq = AuditSequenceID(999, 1999);
    nextSeq = AuditSequenceID(999, 2002);
    ++(++(++seq));
    ASSERT_TRUE(seq == nextSeq);
}

DEATH_TEST_F(AuditSequenceIDTest, IncrementWraparoundTest, "invariant") {
    AuditSequenceID seq(999, ~0);
    ++seq;
}

TEST_F(AuditSequenceIDTest, EqualityOperatorsTest) {
    AuditSequenceID seq;
    AuditSequenceID otherSeq(10, 23);
    ASSERT_FALSE(seq == otherSeq);
    ASSERT_TRUE(seq != otherSeq);

    seq = AuditSequenceID(10, 23);
    ASSERT_TRUE(seq == otherSeq);
    ASSERT_FALSE(seq != otherSeq);
}

TEST_F(AuditSequenceIDTest, SerializeTest) {
    AuditSequenceID seq(0xdeadbeef, 0x01aa02bb03cc04dd);
    std::vector<std::uint8_t> expected = {
        0xef, 0xbe, 0xad, 0xde, 0xdd, 0x04, 0xcc, 0x03, 0xbb, 0x02, 0xaa, 0x01};
    std::vector<std::uint8_t> output(AuditSequenceID::kSerializedAuditSequenceIDSize);

    // test normal serialization
    seq.serialize(output);
    ASSERT_TRUE(output == expected);

    // test exception thrown if output buffer is too small
    output.pop_back();
    ASSERT_THROWS(seq.serialize(output), mongo::DBException);
}

TEST_F(AuditSequenceIDTest, DeserializeTest) {
    std::vector<std::uint8_t> input = {
        0xef, 0xbe, 0xad, 0xde, 0xdd, 0x04, 0xcc, 0x03, 0xbb, 0x02, 0xaa, 0x01};
    AuditSequenceID expected(0xdeadbeef, 0x01aa02bb03cc04dd);

    // test normal deserialization
    auto parsed = AuditSequenceID::deserialize(input);
    ASSERT_TRUE(parsed == expected);

    // test exception thrown if input is too small
    input.pop_back();
    ASSERT_THROWS(AuditSequenceID::deserialize(input), mongo::DBException);
}

TEST_F(AuditSequenceIDCheckerTest, MatchAndIncrementTest) {
    AuditSequenceID seq(0xdeadbeef, 0x1000);
    std::vector<std::uint8_t> serialized(AuditSequenceID::kSerializedAuditSequenceIDSize);
    AuditSequenceIDChecker checker(seq);

    seq.serialize(serialized);
    ASSERT_TRUE(checker.matchAndIncrement(serialized));

    ++seq;
    seq.serialize(serialized);
    ASSERT_TRUE(checker.matchAndIncrement(serialized));

    ++seq;
    ++seq;
    seq.serialize(serialized);
    ASSERT_FALSE(checker.matchAndIncrement(serialized));

    seq = AuditSequenceID(0xdeadbeef, 0x1002);
    seq.serialize(serialized);
    ASSERT_TRUE(checker.matchAndIncrement(serialized));
}

}  // namespace audit
}  // namespace mongo
