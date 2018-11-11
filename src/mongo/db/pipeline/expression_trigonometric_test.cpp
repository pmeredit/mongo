#include "mongo/unittest/unittest.h"
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/expression_trigonometric.h"
#include "mongo/db/pipeline/expression_context_for_test.h"

namespace ExpressionTests {

using boost::intrusive_ptr;
using namespace mongo;

// assert_approximately_eq is a helper function for asserting approximate results.
static void assert_approximately_eq(const Value& evaluated, const Value& expected) {
    switch (evaluated.getType()) {
        case NumberDouble:
            ASSERT_VALUE_LT(Value(std::abs(evaluated.getDouble() - expected.getDouble())),
                            Value(.000001));
            break;
        case NumberDecimal:
            ASSERT_VALUE_LT(Value(evaluated.getDecimal().subtract(expected.getDecimal()).toAbs()),
                            Value(Decimal128(".000001")));
            break;
        case NumberInt:
        case NumberLong:
            ASSERT_VALUE_EQ(evaluated, expected);
            break;
        default:
            if (evaluated.nullish()) {
                ASSERT_VALUE_EQ(evaluated, expected);
            } else {
                ASSERT(false &&
                       "assert_approximately_eq should only be used with expressions that return "
                       "numeric or nullish Values");
            }
    }
}

class ExpressionBaseTest : public mongo::unittest::Test {
public:
    void addOperand(intrusive_ptr<ExpressionNary> expr, Value arg) {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        expr->addOperand(ExpressionConstant::create(expCtx, arg));
    }
};

// A testing class for testing approximately equal results for one argument numeric expressions.
class ExpressionNaryTestOneArgApproximate : public ExpressionBaseTest {
public:
    virtual void assertEvaluates(Value input, Value output) {
        addOperand(_expr, input);
        auto evaluated = _expr->evaluate(Document());
        ASSERT_EQUALS(output.getType(), evaluated.getType());
        assert_approximately_eq(evaluated, output);
    }

    intrusive_ptr<ExpressionNary> _expr;
};

// A testing class for testing approximately equal results for two argument numeric expressions.
class ExpressionNaryTestTwoArgApproximate : public ExpressionBaseTest {
public:
    virtual void assertEvaluates(Value input1, Value input2, Value output) {
        addOperand(_expr, input1);
        addOperand(_expr, input2);
        auto evaluated = _expr->evaluate(Document());
        ASSERT_EQUALS(output.getType(), evaluated.getType());
        assert_approximately_eq(evaluated, output);
    }

    intrusive_ptr<ExpressionNary> _expr;
};

/* ------------------------- ExpressionArcSine -------------------------- */
class ExpressionArcSineTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionArcSine(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionArcSineTest, IntArg) {
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(1), Value(1.57079632679));
}

TEST_F(ExpressionArcSineTest, LongArg) {
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(1LL), Value(1.57079632679));
}

TEST_F(ExpressionArcSineTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(0.0));
    assertEvaluates(Value(0.1), Value(0.100167421162));
    assertEvaluates(Value(0.2), Value(0.20135792079));
    assertEvaluates(Value(0.3), Value(0.304692654015));
    assertEvaluates(Value(0.4), Value(0.411516846067));
    assertEvaluates(Value(0.5), Value(0.523598775598));
    assertEvaluates(Value(0.6), Value(0.643501108793));
    assertEvaluates(Value(0.7), Value(0.775397496611));
    assertEvaluates(Value(0.8), Value(0.927295218002));
    assertEvaluates(Value(0.9), Value(1.119769515));
    assertEvaluates(Value(1.0), Value(1.57079632679));
}

TEST_F(ExpressionArcSineTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("0.1")), Value(Decimal128("0.100167421162")));
    assertEvaluates(Value(Decimal128("0.2")), Value(Decimal128("0.20135792079")));
    assertEvaluates(Value(Decimal128("0.3")), Value(Decimal128("0.304692654015")));
    assertEvaluates(Value(Decimal128("0.4")), Value(Decimal128("0.411516846067")));
    assertEvaluates(Value(Decimal128("0.5")), Value(Decimal128("0.523598775598")));
    assertEvaluates(Value(Decimal128("0.6")), Value(Decimal128("0.643501108793")));
    assertEvaluates(Value(Decimal128("0.7")), Value(Decimal128("0.775397496611")));
    assertEvaluates(Value(Decimal128("0.8")), Value(Decimal128("0.927295218002")));
    assertEvaluates(Value(Decimal128("0.9")), Value(Decimal128("1.119769515")));
    assertEvaluates(Value(Decimal128("1.0")), Value(Decimal128("1.57079632679")));
}

TEST_F(ExpressionArcSineTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionArcCosine -------------------------- */

class ExpressionArcCosineTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionArcCosine(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionArcCosineTest, IntArg) {
    assertEvaluates(Value(0), Value(1.57079632679));
    assertEvaluates(Value(1), Value(0.0));
}

TEST_F(ExpressionArcCosineTest, LongArg) {
    assertEvaluates(Value(0LL), Value(1.57079632679));
    assertEvaluates(Value(1LL), Value(0.0));
}

TEST_F(ExpressionArcCosineTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(1.57079632679));
    assertEvaluates(Value(0.1), Value(1.47062890563));
    assertEvaluates(Value(0.2), Value(1.369438406));
    assertEvaluates(Value(0.3), Value(1.26610367278));
    assertEvaluates(Value(0.4), Value(1.15927948073));
    assertEvaluates(Value(0.5), Value(1.0471975512));
    assertEvaluates(Value(0.6), Value(0.927295218002));
    assertEvaluates(Value(0.7), Value(0.795398830184));
    assertEvaluates(Value(0.8), Value(0.643501108793));
    assertEvaluates(Value(0.9), Value(0.451026811796));
    assertEvaluates(Value(1.0), Value(0.0));
}

TEST_F(ExpressionArcCosineTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("1.57079632679")));
    assertEvaluates(Value(Decimal128("0.1")), Value(Decimal128("1.47062890563")));
    assertEvaluates(Value(Decimal128("0.2")), Value(Decimal128("1.369438406")));
    assertEvaluates(Value(Decimal128("0.3")), Value(Decimal128("1.26610367278")));
    assertEvaluates(Value(Decimal128("0.4")), Value(Decimal128("1.15927948073")));
    assertEvaluates(Value(Decimal128("0.5")), Value(Decimal128("1.0471975512")));
    assertEvaluates(Value(Decimal128("0.6")), Value(Decimal128("0.927295218002")));
    assertEvaluates(Value(Decimal128("0.7")), Value(Decimal128("0.795398830184")));
    assertEvaluates(Value(Decimal128("0.8")), Value(Decimal128("0.643501108793")));
    assertEvaluates(Value(Decimal128("0.9")), Value(Decimal128("0.451026811796")));
    assertEvaluates(Value(Decimal128("1.0")), Value(Decimal128("0.0")));
}

TEST_F(ExpressionArcCosineTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionArcTangent -------------------------- */

class ExpressionArcTangentTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionArcTangent(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionArcTangentTest, IntArg) {
    assertEvaluates(Value(-1), Value(-0.785398163397));
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(1), Value(0.785398163397));
}

TEST_F(ExpressionArcTangentTest, LongArg) {
    assertEvaluates(Value(-1LL), Value(-0.785398163397));
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(1LL), Value(0.785398163397));
}

TEST_F(ExpressionArcTangentTest, DoubleArg) {
    assertEvaluates(Value(-1.5), Value(-0.982793723247));
    assertEvaluates(Value(-1.0471975512), Value(-0.80844879263));
    assertEvaluates(Value(-0.785398163397), Value(-0.665773750028));
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(0.785398163397), Value(0.665773750028));
    assertEvaluates(Value(1.0471975512), Value(0.80844879263));
    assertEvaluates(Value(1.5), Value(0.982793723247));
}

TEST_F(ExpressionArcTangentTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("-1.5")), Value(Decimal128("-0.982793723247")));
    assertEvaluates(Value(Decimal128("-1.0471975512")), Value(Decimal128("-0.80844879263")));
    assertEvaluates(Value(Decimal128("-0.785398163397")), Value(Decimal128("-0.665773750028")));
    assertEvaluates(Value(Decimal128("0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("0.665773750028")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("0.80844879263")));
    assertEvaluates(Value(Decimal128("1.5")), Value(Decimal128("0.982793723247")));
}

TEST_F(ExpressionArcTangentTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionArcTangent2 -------------------------- */

class ExpressionArcTangent2Test : public ExpressionNaryTestTwoArgApproximate {
public:
    virtual void assertEvaluates(Value input1, Value input2, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionArcTangent2(expCtx);
        ExpressionNaryTestTwoArgApproximate::assertEvaluates(input1, input2, output);
    }
};

TEST_F(ExpressionArcTangent2Test, TwoIntArgs) {
    assertEvaluates(Value(1), Value(0), Value(1.57079632679));
    assertEvaluates(Value(0), Value(1), Value(0.0));
    assertEvaluates(Value(-1), Value(0), Value(-1.57079632679));
    assertEvaluates(Value(0), Value(-1), Value(3.14159265359));
}

TEST_F(ExpressionArcTangent2Test, TwoLongArg) {
    assertEvaluates(Value(1LL), Value(0LL), Value(1.57079632679));
    assertEvaluates(Value(0LL), Value(1LL), Value(0.0));
    assertEvaluates(Value(-1LL), Value(0LL), Value(-1.57079632679));
    assertEvaluates(Value(0LL), Value(-1LL), Value(3.14159265359));
}

TEST_F(ExpressionArcTangent2Test, LongIntArg) {
    assertEvaluates(Value(1LL), Value(0),  Value(1.57079632679));
    assertEvaluates(Value(0LL), Value(1),  Value(0.0));
    assertEvaluates(Value(-1LL), Value(0), Value(-1.57079632679));
    assertEvaluates(Value(0LL), Value(-1), Value(3.14159265359));
}

TEST_F(ExpressionArcTangent2Test, IntLongArg) {
    assertEvaluates(Value(1), Value(0LL),  Value(1.57079632679));
    assertEvaluates(Value(0), Value(1LL),  Value(0.0));
    assertEvaluates(Value(-1), Value(0LL), Value(-1.57079632679));
    assertEvaluates(Value(0), Value(-1LL), Value(3.14159265359));
}

TEST_F(ExpressionArcTangent2Test, TwoDoubleArg) {
    assertEvaluates(Value(1.0), Value(0.0), Value(1.57079632679));
    assertEvaluates(Value(0.866025403784), Value(0.5), Value(1.0471975512));
    assertEvaluates(Value(0.707106781187), Value(0.707106781187), Value(0.785398163397));
    assertEvaluates(Value(0.5), Value(0.866025403784), Value(0.523598775598));
    assertEvaluates(Value(6.12323399574e-17), Value(1.0), Value(6.12323399574e-17));
    assertEvaluates(Value(-0.5), Value(0.866025403784), Value(-0.523598775598));
    assertEvaluates(Value(-0.707106781187), Value(0.707106781187), Value(-0.785398163397));
    assertEvaluates(Value(-0.866025403784), Value(0.5), Value(-1.0471975512));
    assertEvaluates(Value(-1.0), Value(1.22464679915e-16), Value(-1.57079632679));
    assertEvaluates(Value(-0.866025403784), Value(-0.5), Value(-2.09439510239));
    assertEvaluates(Value(-0.707106781187), Value(-0.707106781187), Value(-2.35619449019));
    assertEvaluates(Value(-0.5), Value(-0.866025403784), Value(-2.61799387799));
    assertEvaluates(Value(-1.83697019872e-16), Value(-1.0), Value(-3.14159265359));
    assertEvaluates(Value(0.5), Value(-0.866025403784), Value(2.61799387799));
    assertEvaluates(Value(0.707106781187), Value(-0.707106781187), Value(2.35619449019));
    assertEvaluates(Value(0.866025403784), Value(-0.5), Value(2.09439510239));
    assertEvaluates(Value(1.0), Value(-2.44929359829e-16), Value(1.57079632679));
}

TEST_F(ExpressionArcTangent2Test, TwoDecimalArg) {
    assertEvaluates(
        Value(Decimal128("1.0")), Value(Decimal128("0.0")), Value(Decimal128("1.57079632679")));
    assertEvaluates(Value(Decimal128("0.866025403784")),
                    Value(Decimal128("0.5")),
                    Value(Decimal128("1.0471975512")));
    assertEvaluates(Value(Decimal128("0.707106781187")),
                    Value(Decimal128("0.707106781187")),
                    Value(Decimal128("0.785398163397")));
    assertEvaluates(Value(Decimal128("0.5")),
                    Value(Decimal128("0.866025403784")),
                    Value(Decimal128("0.523598775598")));
    assertEvaluates(Value(Decimal128("6.12323399574e-17")),
                    Value(Decimal128("1.0")),
                    Value(Decimal128("6.12323399574e-17")));
    assertEvaluates(Value(Decimal128("-0.5")),
                    Value(Decimal128("0.866025403784")),
                    Value(Decimal128("-0.523598775598")));
    assertEvaluates(Value(Decimal128("-0.707106781187")),
                    Value(Decimal128("0.707106781187")),
                    Value(Decimal128("-0.785398163397")));
    assertEvaluates(Value(Decimal128("-0.866025403784")),
                    Value(Decimal128("0.5")),
                    Value(Decimal128("-1.0471975512")));
    assertEvaluates(Value(Decimal128("-1.0")),
                    Value(Decimal128("1.22464679915e-16")),
                    Value(Decimal128("-1.57079632679")));
    assertEvaluates(Value(Decimal128("-0.866025403784")),
                    Value(Decimal128("-0.5")),
                    Value(Decimal128("-2.09439510239")));
    assertEvaluates(Value(Decimal128("-0.707106781187")),
                    Value(Decimal128("-0.707106781187")),
                    Value(Decimal128("-2.35619449019")));
    assertEvaluates(Value(Decimal128("-0.5")),
                    Value(Decimal128("-0.866025403784")),
                    Value(Decimal128("-2.61799387799")));
    assertEvaluates(Value(Decimal128("-1.83697019872e-16")),
                    Value(Decimal128("-1.0")),
                    Value(Decimal128("-3.14159265359")));
    assertEvaluates(Value(Decimal128("0.5")),
                    Value(Decimal128("-0.866025403784")),
                    Value(Decimal128("2.61799387799")));
    assertEvaluates(Value(Decimal128("0.707106781187")),
                    Value(Decimal128("-0.707106781187")),
                    Value(Decimal128("2.35619449019")));
    assertEvaluates(Value(Decimal128("0.866025403784")),
                    Value(Decimal128("-0.5")),
                    Value(Decimal128("2.09439510239")));
    assertEvaluates(Value(Decimal128("1.0")),
                    Value(Decimal128("-2.44929359829e-16")),
                    Value(Decimal128("1.57079632679")));
}

TEST_F(ExpressionArcTangent2Test, DoubleDecimalArg) {
    assertEvaluates(Value(1.0), Value(Decimal128("0.0")), Value(Decimal128("1.57079632679")));
    assertEvaluates(
        Value(0.866025403784), Value(Decimal128("0.5")), Value(Decimal128("1.0471975512")));
    assertEvaluates(Value(0.707106781187),
                    Value(Decimal128("0.707106781187")),
                    Value(Decimal128("0.785398163397")));
    assertEvaluates(
        Value(0.5), Value(Decimal128("0.866025403784")), Value(Decimal128("0.523598775598")));
    assertEvaluates(
        Value(6.12323399574e-17), Value(Decimal128("1.0")), Value(Decimal128("6.12323399574e-17")));
    assertEvaluates(
        Value(-0.5), Value(Decimal128("0.866025403784")), Value(Decimal128("-0.523598775598")));
    assertEvaluates(Value(-0.707106781187),
                    Value(Decimal128("0.707106781187")),
                    Value(Decimal128("-0.785398163397")));
    assertEvaluates(
        Value(-0.866025403784), Value(Decimal128("0.5")), Value(Decimal128("-1.0471975512")));
    assertEvaluates(
        Value(-1.0), Value(Decimal128("1.22464679915e-16")), Value(Decimal128("-1.57079632679")));
    assertEvaluates(
        Value(-0.866025403784), Value(Decimal128("-0.5")), Value(Decimal128("-2.09439510239")));
    assertEvaluates(Value(-0.707106781187),
                    Value(Decimal128("-0.707106781187")),
                    Value(Decimal128("-2.35619449019")));
    assertEvaluates(
        Value(-0.5), Value(Decimal128("-0.866025403784")), Value(Decimal128("-2.61799387799")));
    assertEvaluates(
        Value(-1.83697019872e-16), Value(Decimal128("-1.0")), Value(Decimal128("-3.14159265359")));
    assertEvaluates(
        Value(0.5), Value(Decimal128("-0.866025403784")), Value(Decimal128("2.61799387799")));
    assertEvaluates(Value(0.707106781187),
                    Value(Decimal128("-0.707106781187")),
                    Value(Decimal128("2.35619449019")));
    assertEvaluates(
        Value(0.866025403784), Value(Decimal128("-0.5")), Value(Decimal128("2.09439510239")));
    assertEvaluates(
        Value(1.0), Value(Decimal128("-2.44929359829e-16")), Value(Decimal128("1.57079632679")));
}

TEST_F(ExpressionArcTangent2Test, DecimalDoubleArg) {
    assertEvaluates(Value(Decimal128("1.0")), Value(0.0), Value(Decimal128("1.57079632679")));
    assertEvaluates(
        Value(Decimal128("0.866025403784")), Value(0.5), Value(Decimal128("1.0471975512")));
    assertEvaluates(Value(Decimal128("0.707106781187")),
                    Value(0.707106781187),
                    Value(Decimal128("0.785398163397")));
    assertEvaluates(
        Value(Decimal128("0.5")), Value(0.866025403784), Value(Decimal128("0.523598775598")));
    assertEvaluates(
        Value(Decimal128("6.12323399574e-17")), Value(1.0), Value(Decimal128("6.12323399574e-17")));
    assertEvaluates(
        Value(Decimal128("-0.5")), Value(0.866025403784), Value(Decimal128("-0.523598775598")));
    assertEvaluates(Value(Decimal128("-0.707106781187")),
                    Value(0.707106781187),
                    Value(Decimal128("-0.785398163397")));
    assertEvaluates(
        Value(Decimal128("-0.866025403784")), Value(0.5), Value(Decimal128("-1.0471975512")));
    assertEvaluates(
        Value(Decimal128("-1.0")), Value(1.22464679915e-16), Value(Decimal128("-1.57079632679")));
    assertEvaluates(
        Value(Decimal128("-0.866025403784")), Value(-0.5), Value(Decimal128("-2.09439510239")));
    assertEvaluates(Value(Decimal128("-0.707106781187")),
                    Value(-0.707106781187),
                    Value(Decimal128("-2.35619449019")));
    assertEvaluates(
        Value(Decimal128("-0.5")), Value(-0.866025403784), Value(Decimal128("-2.61799387799")));
    assertEvaluates(
        Value(Decimal128("-1.83697019872e-16")), Value(-1.0), Value(Decimal128("-3.14159265359")));
    assertEvaluates(
        Value(Decimal128("0.5")), Value(-0.866025403784), Value(Decimal128("2.61799387799")));
    assertEvaluates(Value(Decimal128("0.707106781187")),
                    Value(-0.707106781187),
                    Value(Decimal128("2.35619449019")));
    assertEvaluates(
        Value(Decimal128("0.866025403784")), Value(-0.5), Value(Decimal128("2.09439510239")));
    assertEvaluates(
        Value(Decimal128("1.0")), Value(-2.44929359829e-16), Value(Decimal128("1.57079632679")));
}

TEST_F(ExpressionArcTangent2Test, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL), Value(BSONNULL));
    assertEvaluates(Value(1), Value(BSONNULL), Value(BSONNULL));
    assertEvaluates(Value(BSONNULL), Value(1), Value(BSONNULL));
}

/* ------------------------- ExpressionArcCosine -------------------------- */

class ExpressionCosineTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionCosine(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionCosineTest, IntArg) {
    assertEvaluates(Value(0), Value(1.0));
    assertEvaluates(Value(1), Value(0.540302305868));
    assertEvaluates(Value(2), Value(-0.416146836547));
    assertEvaluates(Value(3), Value(-0.9899924966));
    assertEvaluates(Value(4), Value(-0.653643620864));
    assertEvaluates(Value(5), Value(0.283662185463));
    assertEvaluates(Value(6), Value(0.96017028665));
}

TEST_F(ExpressionCosineTest, LongArg) {
    assertEvaluates(Value(0LL), Value(1.0));
    assertEvaluates(Value(1LL), Value(0.540302305868));
    assertEvaluates(Value(2LL), Value(-0.416146836547));
    assertEvaluates(Value(3LL), Value(-0.9899924966));
    assertEvaluates(Value(4LL), Value(-0.653643620864));
    assertEvaluates(Value(5LL), Value(0.283662185463));
    assertEvaluates(Value(6LL), Value(0.96017028665));
}

TEST_F(ExpressionCosineTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(1.0));
    assertEvaluates(Value(0.523598775598), Value(0.866025403784));
    assertEvaluates(Value(0.785398163397), Value(0.707106781187));
    assertEvaluates(Value(1.0471975512), Value(0.5));
    assertEvaluates(Value(1.57079632679), Value(6.12323399574e-17));
    assertEvaluates(Value(2.09439510239), Value(-0.5));
    assertEvaluates(Value(2.35619449019), Value(-0.707106781187));
    assertEvaluates(Value(2.61799387799), Value(-0.866025403784));
    assertEvaluates(Value(3.14159265359), Value(-1.0));
    assertEvaluates(Value(3.66519142919), Value(-0.866025403784));
    assertEvaluates(Value(3.92699081699), Value(-0.707106781187));
    assertEvaluates(Value(4.18879020479), Value(-0.5));
    assertEvaluates(Value(4.71238898038), Value(-1.83697019872e-16));
    assertEvaluates(Value(5.23598775598), Value(0.5));
    assertEvaluates(Value(5.49778714378), Value(0.707106781187));
    assertEvaluates(Value(5.75958653158), Value(0.866025403784));
    assertEvaluates(Value(6.28318530718), Value(1.0));
}

TEST_F(ExpressionCosineTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("1.0")));
    assertEvaluates(Value(Decimal128("0.523598775598")), Value(Decimal128("0.866025403784")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("0.707106781187")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("0.5")));
    assertEvaluates(Value(Decimal128("1.57079632679")), Value(Decimal128("6.12323399574e-17")));
    assertEvaluates(Value(Decimal128("2.09439510239")), Value(Decimal128("-0.5")));
    assertEvaluates(Value(Decimal128("2.35619449019")), Value(Decimal128("-0.707106781187")));
    assertEvaluates(Value(Decimal128("2.61799387799")), Value(Decimal128("-0.866025403784")));
    assertEvaluates(Value(Decimal128("3.14159265359")), Value(Decimal128("-1.0")));
    assertEvaluates(Value(Decimal128("3.66519142919")), Value(Decimal128("-0.866025403784")));
    assertEvaluates(Value(Decimal128("3.92699081699")), Value(Decimal128("-0.707106781187")));
    assertEvaluates(Value(Decimal128("4.18879020479")), Value(Decimal128("-0.5")));
    assertEvaluates(Value(Decimal128("4.71238898038")), Value(Decimal128("-1.83697019872e-16")));
    assertEvaluates(Value(Decimal128("5.23598775598")), Value(Decimal128("0.5")));
    assertEvaluates(Value(Decimal128("5.49778714378")), Value(Decimal128("0.707106781187")));
    assertEvaluates(Value(Decimal128("5.75958653158")), Value(Decimal128("0.866025403784")));
    assertEvaluates(Value(Decimal128("6.28318530718")), Value(Decimal128("1.0")));
}

TEST_F(ExpressionCosineTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionHyperbolicCosine -------------------------- */

class ExpressionHyperbolicCosineTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionHyperbolicCosine(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionHyperbolicCosineTest, IntArg) {
    assertEvaluates(Value(0), Value(1.0));
    assertEvaluates(Value(1), Value(1.54308063482));
    assertEvaluates(Value(2), Value(3.76219569108));
    assertEvaluates(Value(3), Value(10.0676619958));
    assertEvaluates(Value(4), Value(27.308232836));
    assertEvaluates(Value(5), Value(74.2099485248));
    assertEvaluates(Value(6), Value(201.715636122));
}

TEST_F(ExpressionHyperbolicCosineTest, LongArg) {
    assertEvaluates(Value(0LL), Value(1.0));
    assertEvaluates(Value(1LL), Value(1.54308063482));
    assertEvaluates(Value(2LL), Value(3.76219569108));
    assertEvaluates(Value(3LL), Value(10.0676619958));
    assertEvaluates(Value(4LL), Value(27.308232836));
    assertEvaluates(Value(5LL), Value(74.2099485248));
    assertEvaluates(Value(6LL), Value(201.715636122));
}

TEST_F(ExpressionHyperbolicCosineTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(1.0));
    assertEvaluates(Value(0.523598775598), Value(1.14023832108));
    assertEvaluates(Value(0.785398163397), Value(1.32460908925));
    assertEvaluates(Value(1.0471975512), Value(1.6002868577));
    assertEvaluates(Value(1.57079632679), Value(2.50917847866));
    assertEvaluates(Value(2.09439510239), Value(4.12183605387));
    assertEvaluates(Value(2.35619449019), Value(5.32275214952));
    assertEvaluates(Value(2.61799387799), Value(6.89057236498));
    assertEvaluates(Value(3.14159265359), Value(11.5919532755));
    assertEvaluates(Value(3.66519142919), Value(19.5446063168));
    assertEvaluates(Value(3.92699081699), Value(25.3868611924));
    assertEvaluates(Value(4.18879020479), Value(32.97906491));
    assertEvaluates(Value(4.71238898038), Value(55.6633808904));
    assertEvaluates(Value(5.23598775598), Value(93.9599750339));
    assertEvaluates(Value(5.49778714378), Value(122.07757934));
    assertEvaluates(Value(5.75958653158), Value(158.610147472));
    assertEvaluates(Value(6.28318530718), Value(267.746761484));
}

TEST_F(ExpressionHyperbolicCosineTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("1.0")));
    assertEvaluates(Value(Decimal128("0.523598775598")), Value(Decimal128("1.14023832108")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("1.32460908925")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("1.6002868577")));
    assertEvaluates(Value(Decimal128("1.57079632679")), Value(Decimal128("2.50917847866")));
    assertEvaluates(Value(Decimal128("2.09439510239")), Value(Decimal128("4.12183605387")));
    assertEvaluates(Value(Decimal128("2.35619449019")), Value(Decimal128("5.32275214952")));
    assertEvaluates(Value(Decimal128("2.61799387799")), Value(Decimal128("6.89057236498")));
    assertEvaluates(Value(Decimal128("3.14159265359")), Value(Decimal128("11.5919532755")));
    assertEvaluates(Value(Decimal128("3.66519142919")), Value(Decimal128("19.5446063168")));
    assertEvaluates(Value(Decimal128("3.92699081699")), Value(Decimal128("25.3868611924")));
    assertEvaluates(Value(Decimal128("4.18879020479")), Value(Decimal128("32.97906491")));
    assertEvaluates(Value(Decimal128("4.71238898038")), Value(Decimal128("55.6633808904")));
    assertEvaluates(Value(Decimal128("5.23598775598")), Value(Decimal128("93.9599750339")));
    assertEvaluates(Value(Decimal128("5.49778714378")), Value(Decimal128("122.07757934")));
    assertEvaluates(Value(Decimal128("5.75958653158")), Value(Decimal128("158.610147472")));
    assertEvaluates(Value(Decimal128("6.28318530718")), Value(Decimal128("267.746761484")));
}

TEST_F(ExpressionHyperbolicCosineTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionRadiansToDegrees -------------------------- */

class ExpressionRadiansToDegreesTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionRadiansToDegrees(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionRadiansToDegreesTest, IntArg) {
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(1), Value(57.2957795131));
    assertEvaluates(Value(2), Value(114.591559026));
    assertEvaluates(Value(3), Value(171.887338539));
    assertEvaluates(Value(4), Value(229.183118052));
    assertEvaluates(Value(5), Value(286.478897565));
    assertEvaluates(Value(6), Value(343.774677078));
}

TEST_F(ExpressionRadiansToDegreesTest, LongArg) {
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(1LL), Value(57.2957795131));
    assertEvaluates(Value(2LL), Value(114.591559026));
    assertEvaluates(Value(3LL), Value(171.887338539));
    assertEvaluates(Value(4LL), Value(229.183118052));
    assertEvaluates(Value(5LL), Value(286.478897565));
    assertEvaluates(Value(6LL), Value(343.774677078));
}

TEST_F(ExpressionRadiansToDegreesTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(0.0));
    assertEvaluates(Value(0.523598775598), Value(30.0));
    assertEvaluates(Value(0.785398163397), Value(45.0));
    assertEvaluates(Value(1.0471975512), Value(60.0));
    assertEvaluates(Value(1.57079632679), Value(90.0));
    assertEvaluates(Value(2.09439510239), Value(120.0));
    assertEvaluates(Value(2.35619449019), Value(135.0));
    assertEvaluates(Value(2.61799387799), Value(150.0));
    assertEvaluates(Value(3.14159265359), Value(180.0));
    assertEvaluates(Value(3.66519142919), Value(210.0));
    assertEvaluates(Value(3.92699081699), Value(225.0));
    assertEvaluates(Value(4.18879020479), Value(240.0));
    assertEvaluates(Value(4.71238898038), Value(270.0));
    assertEvaluates(Value(5.23598775598), Value(300.0));
    assertEvaluates(Value(5.49778714378), Value(315.0));
    assertEvaluates(Value(5.75958653158), Value(330.0));
    assertEvaluates(Value(6.28318530718), Value(360.0));
}

TEST_F(ExpressionRadiansToDegreesTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("0.523598775598")), Value(Decimal128("30.0")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("45.0")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("60.0")));
    assertEvaluates(Value(Decimal128("1.57079632679")), Value(Decimal128("90.0")));
    assertEvaluates(Value(Decimal128("2.09439510239")), Value(Decimal128("120.0")));
    assertEvaluates(Value(Decimal128("2.35619449019")), Value(Decimal128("135.0")));
    assertEvaluates(Value(Decimal128("2.61799387799")), Value(Decimal128("150.0")));
    assertEvaluates(Value(Decimal128("3.14159265359")), Value(Decimal128("180.0")));
    assertEvaluates(Value(Decimal128("3.66519142919")), Value(Decimal128("210.0")));
    assertEvaluates(Value(Decimal128("3.92699081699")), Value(Decimal128("225.0")));
    assertEvaluates(Value(Decimal128("4.18879020479")), Value(Decimal128("240.0")));
    assertEvaluates(Value(Decimal128("4.71238898038")), Value(Decimal128("270.0")));
    assertEvaluates(Value(Decimal128("5.23598775598")), Value(Decimal128("300.0")));
    assertEvaluates(Value(Decimal128("5.49778714378")), Value(Decimal128("315.0")));
    assertEvaluates(Value(Decimal128("5.75958653158")), Value(Decimal128("330.0")));
    assertEvaluates(Value(Decimal128("6.28318530718")), Value(Decimal128("360.0")));
}

TEST_F(ExpressionRadiansToDegreesTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionDegreesToRadians -------------------------- */

class ExpressionDegreesToRadiansTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionDegreesToRadians(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionDegreesToRadiansTest, IntArg) {
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(45), Value(0.785398163397));
    assertEvaluates(Value(90), Value(1.57079632679));
    assertEvaluates(Value(135), Value(2.35619449019));
    assertEvaluates(Value(180), Value(3.14159265359));
    assertEvaluates(Value(180), Value(3.14159265359));
    assertEvaluates(Value(225), Value(3.92699081699));
    assertEvaluates(Value(270), Value(4.71238898038));
    assertEvaluates(Value(315), Value(5.49778714378));
    assertEvaluates(Value(360), Value(6.28318530718));
}

TEST_F(ExpressionDegreesToRadiansTest, LongArg) {
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(45LL), Value(0.785398163397));
    assertEvaluates(Value(90LL), Value(1.57079632679));
    assertEvaluates(Value(135LL), Value(2.35619449019));
    assertEvaluates(Value(180LL), Value(3.14159265359));
    assertEvaluates(Value(180LL), Value(3.14159265359));
    assertEvaluates(Value(225LL), Value(3.92699081699));
    assertEvaluates(Value(270LL), Value(4.71238898038));
    assertEvaluates(Value(315LL), Value(5.49778714378));
    assertEvaluates(Value(360LL), Value(6.28318530718));
}

TEST_F(ExpressionDegreesToRadiansTest, DoubleArg) {
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(45), Value(0.785398163397));
    assertEvaluates(Value(90), Value(1.57079632679));
    assertEvaluates(Value(135), Value(2.35619449019));
    assertEvaluates(Value(180), Value(3.14159265359));
    assertEvaluates(Value(180), Value(3.14159265359));
    assertEvaluates(Value(225), Value(3.92699081699));
    assertEvaluates(Value(270), Value(4.71238898038));
    assertEvaluates(Value(315), Value(5.49778714378));
    assertEvaluates(Value(360), Value(6.28318530718));
}

TEST_F(ExpressionDegreesToRadiansTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("45")), Value(Decimal128("0.785398163397")));
    assertEvaluates(Value(Decimal128("90")), Value(Decimal128("1.57079632679")));
    assertEvaluates(Value(Decimal128("135")), Value(Decimal128("2.35619449019")));
    assertEvaluates(Value(Decimal128("180")), Value(Decimal128("3.14159265359")));
    assertEvaluates(Value(Decimal128("180")), Value(Decimal128("3.14159265359")));
    assertEvaluates(Value(Decimal128("225")), Value(Decimal128("3.92699081699")));
    assertEvaluates(Value(Decimal128("270")), Value(Decimal128("4.71238898038")));
    assertEvaluates(Value(Decimal128("315")), Value(Decimal128("5.49778714378")));
    assertEvaluates(Value(Decimal128("360")), Value(Decimal128("6.28318530718")));
}

TEST_F(ExpressionDegreesToRadiansTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionSine -------------------------- */

class ExpressionSineTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionSine(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionSineTest, IntArg) {
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(1), Value(0.841470984808));
    assertEvaluates(Value(2), Value(0.909297426826));
    assertEvaluates(Value(3), Value(0.14112000806));
    assertEvaluates(Value(4), Value(-0.756802495308));
    assertEvaluates(Value(5), Value(-0.958924274663));
    assertEvaluates(Value(6), Value(-0.279415498199));
}

TEST_F(ExpressionSineTest, LongArg) {
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(1LL), Value(0.841470984808));
    assertEvaluates(Value(2LL), Value(0.909297426826));
    assertEvaluates(Value(3LL), Value(0.14112000806));
    assertEvaluates(Value(4LL), Value(-0.756802495308));
    assertEvaluates(Value(5LL), Value(-0.958924274663));
    assertEvaluates(Value(6LL), Value(-0.279415498199));
}

TEST_F(ExpressionSineTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(0.0));
    assertEvaluates(Value(0.523598775598), Value(0.5));
    assertEvaluates(Value(0.785398163397), Value(0.707106781187));
    assertEvaluates(Value(1.0471975512), Value(0.866025403784));
    assertEvaluates(Value(1.57079632679), Value(1.0));
    assertEvaluates(Value(2.09439510239), Value(0.866025403784));
    assertEvaluates(Value(2.35619449019), Value(0.707106781187));
    assertEvaluates(Value(2.61799387799), Value(0.5));
    assertEvaluates(Value(3.14159265359), Value(1.22464679915e-16));
    assertEvaluates(Value(3.66519142919), Value(-0.5));
    assertEvaluates(Value(3.92699081699), Value(-0.707106781187));
    assertEvaluates(Value(4.18879020479), Value(-0.866025403784));
    assertEvaluates(Value(4.71238898038), Value(-1.0));
    assertEvaluates(Value(5.23598775598), Value(-0.866025403784));
    assertEvaluates(Value(5.49778714378), Value(-0.707106781187));
    assertEvaluates(Value(5.75958653158), Value(-0.5));
    assertEvaluates(Value(6.28318530718), Value(-2.44929359829e-16));
}

TEST_F(ExpressionSineTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("0.523598775598")), Value(Decimal128("0.5")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("0.707106781187")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("0.866025403784")));
    assertEvaluates(Value(Decimal128("1.57079632679")), Value(Decimal128("1.0")));
    assertEvaluates(Value(Decimal128("2.09439510239")), Value(Decimal128("0.866025403784")));
    assertEvaluates(Value(Decimal128("2.35619449019")), Value(Decimal128("0.707106781187")));
    assertEvaluates(Value(Decimal128("2.61799387799")), Value(Decimal128("0.5")));
    assertEvaluates(Value(Decimal128("3.14159265359")), Value(Decimal128("1.22464679915e-16")));
    assertEvaluates(Value(Decimal128("3.66519142919")), Value(Decimal128("-0.5")));
    assertEvaluates(Value(Decimal128("3.92699081699")), Value(Decimal128("-0.707106781187")));
    assertEvaluates(Value(Decimal128("4.18879020479")), Value(Decimal128("-0.866025403784")));
    assertEvaluates(Value(Decimal128("4.71238898038")), Value(Decimal128("-1.0")));
    assertEvaluates(Value(Decimal128("5.23598775598")), Value(Decimal128("-0.866025403784")));
    assertEvaluates(Value(Decimal128("5.49778714378")), Value(Decimal128("-0.707106781187")));
    assertEvaluates(Value(Decimal128("5.75958653158")), Value(Decimal128("-0.5")));
    assertEvaluates(Value(Decimal128("6.28318530718")), Value(Decimal128("-2.44929359829e-16")));
}

TEST_F(ExpressionSineTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionHyperbolicSine -------------------------- */

class ExpressionHyperbolicSineTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionHyperbolicSine(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionHyperbolicSineTest, IntArg) {
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(1), Value(1.17520119364));
    assertEvaluates(Value(2), Value(3.62686040785));
    assertEvaluates(Value(3), Value(10.0178749274));
    assertEvaluates(Value(4), Value(27.2899171971));
    assertEvaluates(Value(5), Value(74.2032105778));
    assertEvaluates(Value(6), Value(201.71315737));
}

TEST_F(ExpressionHyperbolicSineTest, LongArg) {
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(1LL), Value(1.17520119364));
    assertEvaluates(Value(2LL), Value(3.62686040785));
    assertEvaluates(Value(3LL), Value(10.0178749274));
    assertEvaluates(Value(4LL), Value(27.2899171971));
    assertEvaluates(Value(5LL), Value(74.2032105778));
    assertEvaluates(Value(6LL), Value(201.71315737));
}

TEST_F(ExpressionHyperbolicSineTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(0.0));
    assertEvaluates(Value(0.523598775598), Value(0.547853473888));
    assertEvaluates(Value(0.785398163397), Value(0.868670961486));
    assertEvaluates(Value(1.0471975512), Value(1.24936705052));
    assertEvaluates(Value(1.57079632679), Value(2.30129890231));
    assertEvaluates(Value(2.09439510239), Value(3.9986913428));
    assertEvaluates(Value(2.35619449019), Value(5.22797192468));
    assertEvaluates(Value(2.61799387799), Value(6.81762330413));
    assertEvaluates(Value(3.14159265359), Value(11.5487393573));
    assertEvaluates(Value(3.66519142919), Value(19.5190070464));
    assertEvaluates(Value(3.92699081699), Value(25.3671583194));
    assertEvaluates(Value(4.18879020479), Value(32.9639002901));
    assertEvaluates(Value(4.71238898038), Value(55.6543975994));
    assertEvaluates(Value(5.23598775598), Value(93.9546534685));
    assertEvaluates(Value(5.49778714378), Value(122.073483515));
    assertEvaluates(Value(5.75958653158), Value(158.606995057));
    assertEvaluates(Value(6.28318530718), Value(267.744894041));
}

TEST_F(ExpressionHyperbolicSineTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("0.523598775598")), Value(Decimal128("0.547853473888")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("0.868670961486")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("1.24936705052")));
    assertEvaluates(Value(Decimal128("1.57079632679")), Value(Decimal128("2.30129890231")));
    assertEvaluates(Value(Decimal128("2.09439510239")), Value(Decimal128("3.9986913428")));
    assertEvaluates(Value(Decimal128("2.35619449019")), Value(Decimal128("5.22797192468")));
    assertEvaluates(Value(Decimal128("2.61799387799")), Value(Decimal128("6.81762330413")));
    assertEvaluates(Value(Decimal128("3.14159265359")), Value(Decimal128("11.5487393573")));
    assertEvaluates(Value(Decimal128("3.66519142919")), Value(Decimal128("19.5190070464")));
    assertEvaluates(Value(Decimal128("3.92699081699")), Value(Decimal128("25.3671583194")));
    assertEvaluates(Value(Decimal128("4.18879020479")), Value(Decimal128("32.9639002901")));
    assertEvaluates(Value(Decimal128("4.71238898038")), Value(Decimal128("55.6543975994")));
    assertEvaluates(Value(Decimal128("5.23598775598")), Value(Decimal128("93.9546534685")));
    assertEvaluates(Value(Decimal128("5.49778714378")), Value(Decimal128("122.073483515")));
    assertEvaluates(Value(Decimal128("5.75958653158")), Value(Decimal128("158.606995057")));
    assertEvaluates(Value(Decimal128("6.28318530718")), Value(Decimal128("267.744894041")));
}

TEST_F(ExpressionHyperbolicSineTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionTangent -------------------------- */

class ExpressionTangentTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionTangent(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionTangentTest, IntArg) {
    assertEvaluates(Value(-1), Value(-1.55740772465));
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(1), Value(1.55740772465));
}

TEST_F(ExpressionTangentTest, LongArg) {
    assertEvaluates(Value(-1LL), Value(-1.55740772465));
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(1LL), Value(1.55740772465));
}

TEST_F(ExpressionTangentTest, DoubleArg) {
    assertEvaluates(Value(-1.5), Value(-14.1014199472));
    assertEvaluates(Value(-1.0471975512), Value(-1.73205080757));
    assertEvaluates(Value(-0.785398163397), Value(-1.0));
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(0.785398163397), Value(1.0));
    assertEvaluates(Value(1.0471975512), Value(1.73205080757));
    assertEvaluates(Value(1.5), Value(14.1014199472));
}

TEST_F(ExpressionTangentTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("-1.5")), Value(Decimal128("-14.1014199472")));
    assertEvaluates(Value(Decimal128("-1.0471975512")), Value(Decimal128("-1.73205080757")));
    assertEvaluates(Value(Decimal128("-0.785398163397")), Value(Decimal128("-1.0")));
    assertEvaluates(Value(Decimal128("0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("1.0")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("1.73205080757")));
    assertEvaluates(Value(Decimal128("1.5")), Value(Decimal128("14.1014199472")));
}

TEST_F(ExpressionTangentTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}

/* ------------------------- ExpressionHyperbolicTangent -------------------------- */

class ExpressionHyperbolicTangentTest : public ExpressionNaryTestOneArgApproximate {
public:
    virtual void assertEvaluates(Value input, Value output) override {
        intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest());
        _expr = new ExpressionHyperbolicTangent(expCtx);
        ExpressionNaryTestOneArgApproximate::assertEvaluates(input, output);
    }
};

TEST_F(ExpressionHyperbolicTangentTest, IntArg) {
    assertEvaluates(Value(0), Value(0.0));
    assertEvaluates(Value(1), Value(0.761594155956));
    assertEvaluates(Value(2), Value(0.964027580076));
    assertEvaluates(Value(3), Value(0.995054753687));
    assertEvaluates(Value(4), Value(0.999329299739));
    assertEvaluates(Value(5), Value(0.999909204263));
    assertEvaluates(Value(6), Value(0.999987711651));
}

TEST_F(ExpressionHyperbolicTangentTest, LongArg) {
    assertEvaluates(Value(0LL), Value(0.0));
    assertEvaluates(Value(1LL), Value(0.761594155956));
    assertEvaluates(Value(2LL), Value(0.964027580076));
    assertEvaluates(Value(3LL), Value(0.995054753687));
    assertEvaluates(Value(4LL), Value(0.999329299739));
    assertEvaluates(Value(5LL), Value(0.999909204263));
    assertEvaluates(Value(6LL), Value(0.999987711651));
}

TEST_F(ExpressionHyperbolicTangentTest, DoubleArg) {
    assertEvaluates(Value(0.0), Value(0.0));
    assertEvaluates(Value(0.523598775598), Value(0.480472778156));
    assertEvaluates(Value(0.785398163397), Value(0.655794202633));
    assertEvaluates(Value(1.0471975512), Value(0.780714435359));
    assertEvaluates(Value(1.57079632679), Value(0.917152335667));
    assertEvaluates(Value(2.09439510239), Value(0.970123821166));
    assertEvaluates(Value(2.35619449019), Value(0.982193380007));
    assertEvaluates(Value(2.61799387799), Value(0.989413207353));
    assertEvaluates(Value(3.14159265359), Value(0.996272076221));
    assertEvaluates(Value(3.66519142919), Value(0.998690213046));
    assertEvaluates(Value(3.92699081699), Value(0.999223894879));
    assertEvaluates(Value(4.18879020479), Value(0.999540174353));
    assertEvaluates(Value(4.71238898038), Value(0.999838613989));
    assertEvaluates(Value(5.23598775598), Value(0.999943363486));
    assertEvaluates(Value(5.49778714378), Value(0.999966449));
    assertEvaluates(Value(5.75958653158), Value(0.99998012476));
    assertEvaluates(Value(6.28318530718), Value(0.99999302534));
}

TEST_F(ExpressionHyperbolicTangentTest, DecimalArg) {
    assertEvaluates(Value(Decimal128("0.0")), Value(Decimal128("0.0")));
    assertEvaluates(Value(Decimal128("0.523598775598")), Value(Decimal128("0.480472778156")));
    assertEvaluates(Value(Decimal128("0.785398163397")), Value(Decimal128("0.655794202633")));
    assertEvaluates(Value(Decimal128("1.0471975512")), Value(Decimal128("0.780714435359")));
    assertEvaluates(Value(Decimal128("1.57079632679")), Value(Decimal128("0.917152335667")));
    assertEvaluates(Value(Decimal128("2.09439510239")), Value(Decimal128("0.970123821166")));
    assertEvaluates(Value(Decimal128("2.35619449019")), Value(Decimal128("0.982193380007")));
    assertEvaluates(Value(Decimal128("2.61799387799")), Value(Decimal128("0.989413207353")));
    assertEvaluates(Value(Decimal128("3.14159265359")), Value(Decimal128("0.996272076221")));
    assertEvaluates(Value(Decimal128("3.66519142919")), Value(Decimal128("0.998690213046")));
    assertEvaluates(Value(Decimal128("3.92699081699")), Value(Decimal128("0.999223894879")));
    assertEvaluates(Value(Decimal128("4.18879020479")), Value(Decimal128("0.999540174353")));
    assertEvaluates(Value(Decimal128("4.71238898038")), Value(Decimal128("0.999838613989")));
    assertEvaluates(Value(Decimal128("5.23598775598")), Value(Decimal128("0.999943363486")));
    assertEvaluates(Value(Decimal128("5.49778714378")), Value(Decimal128("0.999966449")));
    assertEvaluates(Value(Decimal128("5.75958653158")), Value(Decimal128("0.99998012476")));
    assertEvaluates(Value(Decimal128("6.28318530718")), Value(Decimal128("0.99999302534")));
}

TEST_F(ExpressionHyperbolicTangentTest, NullArg) {
    assertEvaluates(Value(BSONNULL), Value(BSONNULL));
}
}
