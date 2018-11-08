#include "expression_trigonometric.h"

namespace mongo {
/* ----------------------- ExpressionArcCosine ---------------------------- */

double ExpressionArcCosine::doubleFunc(double arg) const {
    return std::acos(arg);
}

Decimal128 ExpressionArcCosine::decimalFunc(Decimal128 arg) const {
    return arg.acos();
}

REGISTER_EXPRESSION(acos, ExpressionArcCosine::parse);
const char* ExpressionArcCosine::getOpName() const {
    return "$acos";
}

/* ----------------------- ExpressionArcSine ---------------------------- */

double ExpressionArcSine::doubleFunc(double arg) const {
    return std::asin(arg);
}

Decimal128 ExpressionArcSine::decimalFunc(Decimal128 arg) const {
    return arg.asin();
}

REGISTER_EXPRESSION(asin, ExpressionArcSine::parse);
const char* ExpressionArcSine::getOpName() const {
    return "$asin";
}

/* ----------------------- ExpressionArcTangent ---------------------------- */

double ExpressionArcTangent::doubleFunc(double arg) const {
    return std::atan(arg);
}

Decimal128 ExpressionArcTangent::decimalFunc(Decimal128 arg) const {
    return arg.atan();
}

REGISTER_EXPRESSION(atan, ExpressionArcTangent::parse);
const char* ExpressionArcTangent::getOpName() const {
    return "$atan";
}

/* ----------------------- ExpressionHyperbolicArcCosine ---------------------------- */

double ExpressionHyperbolicArcCosine::doubleFunc(double arg) const {
    return std::acosh(arg);
}

Decimal128 ExpressionHyperbolicArcCosine::decimalFunc(Decimal128 arg) const {
    return arg.acosh();
}

REGISTER_EXPRESSION(acosh, ExpressionHyperbolicArcCosine::parse);
const char* ExpressionHyperbolicArcCosine::getOpName() const {
    return "$acosh";
}

/* ----------------------- ExpressionHyperbolicArcSine ---------------------------- */

double ExpressionHyperbolicArcSine::doubleFunc(double arg) const {
    return std::asinh(arg);
}

Decimal128 ExpressionHyperbolicArcSine::decimalFunc(Decimal128 arg) const {
    return arg.asinh();
}

REGISTER_EXPRESSION(asinh, ExpressionHyperbolicArcSine::parse);
const char* ExpressionHyperbolicArcSine::getOpName() const {
    return "$asinh";
}

/* ----------------------- ExpressionHyperbolicArcTangent ---------------------------- */

double ExpressionHyperbolicArcTangent::doubleFunc(double arg) const {
    return std::atanh(arg);
}

Decimal128 ExpressionHyperbolicArcTangent::decimalFunc(Decimal128 arg) const {
    return arg.atanh();
}

REGISTER_EXPRESSION(atanh, ExpressionHyperbolicArcTangent::parse);
const char* ExpressionHyperbolicArcTangent::getOpName() const {
    return "$atanh";
}

/* ----------------------- ExpressionCosine ---------------------------- */

double ExpressionCosine::doubleFunc(double arg) const {
    return std::cos(arg);
}

Decimal128 ExpressionCosine::decimalFunc(Decimal128 arg) const {
    return arg.cos();
}

REGISTER_EXPRESSION(cos, ExpressionCosine::parse);
const char* ExpressionCosine::getOpName() const {
    return "$cos";
}

/* ----------------------- ExpressionHyperbolicCosine ---------------------------- */

double ExpressionHyperbolicCosine::doubleFunc(double arg) const {
    return std::cosh(arg);
}

Decimal128 ExpressionHyperbolicCosine::decimalFunc(Decimal128 arg) const {
    return arg.cosh();
}

REGISTER_EXPRESSION(cosh, ExpressionHyperbolicCosine::parse);
const char* ExpressionHyperbolicCosine::getOpName() const {
    return "$cosh";
}

/* ----------------------- ExpressionSine ---------------------------- */

double ExpressionSine::doubleFunc(double arg) const {
    return std::sin(arg);
}

Decimal128 ExpressionSine::decimalFunc(Decimal128 arg) const {
    return arg.sin();
}

REGISTER_EXPRESSION(sin, ExpressionSine::parse);
const char* ExpressionSine::getOpName() const {
    return "$sin";
}

/* ----------------------- ExpressionHyperbolicSine ---------------------------- */

double ExpressionHyperbolicSine::doubleFunc(double arg) const {
    return std::sinh(arg);
}

Decimal128 ExpressionHyperbolicSine::decimalFunc(Decimal128 arg) const {
    return arg.sinh();
}

REGISTER_EXPRESSION(sinh, ExpressionHyperbolicSine::parse);
const char* ExpressionHyperbolicSine::getOpName() const {
    return "$sinh";
}

/* ----------------------- ExpressionTangent ---------------------------- */

double ExpressionTangent::doubleFunc(double arg) const {
    return std::tan(arg);
}

Decimal128 ExpressionTangent::decimalFunc(Decimal128 arg) const {
    return arg.tan();
}

REGISTER_EXPRESSION(tan, ExpressionTangent::parse);
const char* ExpressionTangent::getOpName() const {
    return "$tan";
}

/* ----------------------- ExpressionHyperbolicTangent ---------------------------- */

double ExpressionHyperbolicTangent::doubleFunc(double arg) const {
    return std::tanh(arg);
}

Decimal128 ExpressionHyperbolicTangent::decimalFunc(Decimal128 arg) const {
    return arg.tanh();
}

REGISTER_EXPRESSION(tanh, ExpressionHyperbolicTangent::parse);
const char* ExpressionHyperbolicTangent::getOpName() const {
    return "$tanh";
}

/* ----------------------- ExpressionArcTangent2 ---------------------------- */

Value ExpressionArcTangent2::evaluateNumericArgs(const Value& numericArg1,
                                           const Value& numericArg2) const {
    BSONType type1 = numericArg1.getType();
    BSONType type2 = numericArg2.getType();
    auto totalType = NumberDouble;
    // If the type of either argument is NumberDecimal, we promote to Decimal128. If the type of
    // either
    // arg is NumberLong, we also promote to NumberDecimal rather than failing in the case where the
    // long cannot fit in a double.
    if (type1 == NumberDecimal || type2 == NumberDecimal || type1 == NumberLong ||
        type2 == NumberLong) {
        totalType = NumberDecimal;
    }
    switch (totalType) {
        case NumberDecimal: {
            auto getDecimal = [](BSONType type, const Value& arg) -> Decimal128 {
                switch (type) {
                    case NumberDecimal:
                        return arg.getDecimal();
                    case NumberDouble:
                        return Decimal128(arg.getDouble());
                    case NumberLong:
                        return Decimal128(static_cast<std::int64_t>(arg.getLong()));
                    case NumberInt:
                        return Decimal128(arg.getInt());
                    default:
                        uassert(50984, "unreachable", false);
                }
            };
            auto dec1 = getDecimal(type1, numericArg1);
            auto dec2 = getDecimal(type2, numericArg2);
            return Value(dec1.atan2(dec2));
        }
        case NumberDouble: {
            auto getDouble = [](BSONType type, const Value& arg) -> double {
                switch (type) {
                    case NumberDouble:
                        return arg.getDouble();
                    case NumberInt:
                        return static_cast<double>(arg.getInt());
                    default:
                        uassert(50985, "unreachable", false);
                }
            };
            auto double1 = getDouble(type1, numericArg1);
            auto double2 = getDouble(type2, numericArg2);
            return Value(std::atan2(double1, double2));
        }
        default:
            uassert(50986, "unreachable", false);
    }
}

REGISTER_EXPRESSION(atan2, ExpressionArcTangent2::parse);
const char* ExpressionArcTangent2::getOpName() const {
    return "$atan2";
}

static const double PI = 3.141592653589793;
static const Decimal128 DECIMAL_PI = Decimal128("3.14159265358979323846264338327950288419716939937510"); 
static const double DOUBLE_180 = 180.0;
static const Decimal128 DECIMAL_180 = Decimal128("180.0"); 

/* ----------------------- ExpressionDegreesToRadians ---------------------------- */

Value ExpressionDegreesToRadians::evaluateNumericArg(const Value& numericArg) const {
    BSONType type = numericArg.getType();
    if (type == NumberDouble) {
        return Value(numericArg.getDouble() * 180.0 / PI);
    } else if (type == NumberDecimal) {
        return Value(numericArg.getDecimal().multiply(DECIMAL_180).divide(DECIMAL_PI));
    } else {
        long long num = numericArg.getLong();
        uassert(50987,
                "can't take $degrees of long long min",
                num != std::numeric_limits<long long>::min());
        auto degreesVal = num * 180.0 / PI;
        return Value(degreesVal);
    }
}

REGISTER_EXPRESSION(degrees, ExpressionDegreesToRadians::parse);
const char* ExpressionDegreesToRadians::getOpName() const {
    return "$degreesToRadians";
}

/* ----------------------- ExpressionDegreesToRadians ---------------------------- */

Value ExpressionRadiansToDegrees::evaluateNumericArg(const Value& numericArg) const {
    BSONType type = numericArg.getType();
    if (type == NumberDouble) {
        return Value(numericArg.getDouble() * PI / 180.0);
    } else if (type == NumberDecimal) {
        return Value(numericArg.getDecimal().multiply(DECIMAL_PI).divide(DECIMAL_180));
    } else {
        long long num = numericArg.getLong();
        uassert(50988,
                "can't take $radians of long long min",
                num != std::numeric_limits<long long>::min());
        auto radiansVal = num * PI / 180.0;
        return Value(radiansVal);
    }
}

REGISTER_EXPRESSION(radians, ExpressionRadiansToDegrees::parse);
const char* ExpressionRadiansToDegrees::getOpName() const {
    return "$radiansToDegrees";
}
}
