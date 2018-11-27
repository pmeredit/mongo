/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "expression_trigonometric.h"

namespace mongo {
/* ----------------------- ExpressionArcCosine ---------------------------- */

double ExpressionArcCosine::doubleFunc(double arg) const {
    return std::acos(arg);
}

Decimal128 ExpressionArcCosine::decimalFunc(Decimal128 arg) const {
    return arg.acos();
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
    // If the type of either argument is NumberDecimal, we promote to Decimal128.
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
						MONGO_UNREACHABLE;
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
						MONGO_UNREACHABLE;
                }
            };
            auto double1 = getDouble(type1, numericArg1);
            auto double2 = getDouble(type2, numericArg2);
            return Value(std::atan2(double1, double2));
        }
        default:
			MONGO_UNREACHABLE;
    }
}

REGISTER_EXPRESSION(atan2, ExpressionArcTangent2::parse);
const char* ExpressionArcTangent2::getOpName() const {
    return "$atan2";
}


/* ----------------------- ExpressionDegreesToRadians and ExpressionRadiansToDegrees ---- */
static const Decimal128 DECIMAL_180 = Decimal128("180");
static const Decimal128 DECIMAL_PI =
    Decimal128("3.14159265358979323846264338327950288419716939937510");
static const Decimal128 DECIMAL_PI_OVER_180 = DECIMAL_PI.divide(DECIMAL_180);
static const Decimal128 DECIMAL_180_OVER_PI = DECIMAL_180.divide(DECIMAL_PI);
static constexpr double DOUBLE_PI = 3.141592653589793;
static constexpr double DOUBLE_PI_OVER_180 = DOUBLE_PI/180.0;
static constexpr double DOUBLE_180_OVER_PI = 180.0/DOUBLE_PI;

struct DegreesToRadians {
    Decimal128 decimalFactor() {
        return DECIMAL_PI_OVER_180;
    }
    double doubleFactor() {
        return DOUBLE_PI_OVER_180;
    }
};

struct RadiansToDegrees {
    Decimal128 decimalFactor() {
        return DECIMAL_180_OVER_PI;
    }
    double doubleFactor() {
        return DOUBLE_180_OVER_PI;
    }
};

template <typename ConversionValues>
static Value doDegreeRadiansConversion(const Value& numericArg) {
    ConversionValues c;
    BSONType type = numericArg.getType();
    if (type == NumberDouble) {
        return Value(numericArg.getDouble() * c.doubleFactor());
    } else if (type == NumberDecimal) {
        return Value(
            numericArg.getDecimal().multiply(c.decimalFactor()));
    } else {
        auto num = static_cast<double>(numericArg.getLong());
        auto degreesVal = num * c.doubleFactor();
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
