#include "expression_trigonometric.h"

namespace mongo {
/* ----------------------- ExpressionArcCosine ---------------------------- */

double ExpressionArcCosine::doubleFunc(double arg) const {
    return std::acos(arg);
}

Decimal128 ExpressionArcCosine::decimalFunc(Decimal128 arg) const {
    return arg.acos();
}

bool ExpressionArcCosine::isInclusive() const {
    return true;
}

boost::optional<double> ExpressionArcCosine::getUpperBound() const {
    return boost::optional<double>(1.0);
}

boost::optional<double> ExpressionArcCosine::getLowerBound() const {
    return boost::optional<double>(-1.0);
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

bool ExpressionArcSine::isInclusive() const {
    return true;
}

boost::optional<double> ExpressionArcSine::getUpperBound() const {
    return boost::optional<double>(1.0);
}

boost::optional<double> ExpressionArcSine::getLowerBound() const {
    return boost::optional<double>(-1.0);
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

bool ExpressionHyperbolicArcCosine::isInclusive() const {
    return true;
}

boost::optional<double> ExpressionHyperbolicArcCosine::getUpperBound() const {
    return boost::none;
}

boost::optional<double> ExpressionHyperbolicArcCosine::getLowerBound() const {
    return boost::optional<double>(1.0);
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

bool ExpressionHyperbolicArcTangent::isInclusive() const {
    return false;
}

boost::optional<double> ExpressionHyperbolicArcTangent::getUpperBound() const {
    return boost::optional<double>(1.0);
}

boost::optional<double> ExpressionHyperbolicArcTangent::getLowerBound() const {
    return boost::optional<double>(-1.0);
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
    if (type1 == NumberDecimal || type2 == NumberDecimal) {
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
                    case NumberLong:
                        return static_cast<double>(arg.getLong());
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


/* ----------------------- ExpressionDegreesToRadians and ExpressionRadiansToDegrees
 * ---------------------------- */
static const Decimal128 DECIMAL_PI =
    Decimal128("3.14159265358979323846264338327950288419716939937510");
static const Decimal128 DECIMAL_180 = Decimal128("180");
static const double DOUBLE_PI = 3.141592653589793;

struct DegreesToRadians {
    Decimal128 decimalNumerator() {
        return DECIMAL_PI;
    }
    Decimal128 decimalDenominator() {
        return DECIMAL_180;
    }
    double doubleNumerator() {
        return DOUBLE_PI;
    }
    double doubleDenominator() {
        return 180.0;
    }
};

struct RadiansToDegrees {
    Decimal128 decimalNumerator() {
        return DECIMAL_180;
    }
    Decimal128 decimalDenominator() {
        return DECIMAL_PI;
    }
    double doubleNumerator() {
        return 180.0;
    }
    double doubleDenominator() {
        return DOUBLE_PI;
    }
};

template <typename ConversionValues>
static Value doDegreeRadiansConversion(const Value& numericArg) {
    ConversionValues c;
    BSONType type = numericArg.getType();
    if (type == NumberDouble) {
        return Value(numericArg.getDouble() * c.doubleNumerator() / c.doubleDenominator());
    } else if (type == NumberDecimal) {
        return Value(
            numericArg.getDecimal().multiply(c.decimalNumerator()).divide(c.decimalDenominator()));
    } else {
        long long num = numericArg.getLong();
        uassert(50987,
                "cannot degree/radians convert long long min",
                num != std::numeric_limits<long long>::min());
        auto degreesVal = num * c.doubleNumerator() / c.doubleDenominator();
        return Value(degreesVal);
    }
}

Value ExpressionDegreesToRadians::evaluateNumericArg(const Value& numericArg) const {
    return doDegreeRadiansConversion<DegreesToRadians>(numericArg);
}

REGISTER_EXPRESSION(degreesToRadians, ExpressionDegreesToRadians::parse);
const char* ExpressionDegreesToRadians::getOpName() const {
    return "$degreesToRadians";
}

Value ExpressionRadiansToDegrees::evaluateNumericArg(const Value& numericArg) const {
    return doDegreeRadiansConversion<RadiansToDegrees>(numericArg);
}

REGISTER_EXPRESSION(radiansToDegrees, ExpressionRadiansToDegrees::parse);
const char* ExpressionRadiansToDegrees::getOpName() const {
    return "$radiansToDegrees";
}
}
