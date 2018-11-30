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

/* ----------------------- Register Bounded Single Argument Trigonometric Functions
 * ----------------------------- */
REGISTER_EXPRESSION(acos, ExpressionArcCosine::parse);
REGISTER_EXPRESSION(asin, ExpressionArcSine::parse);
REGISTER_EXPRESSION(acosh, ExpressionHyperbolicArcCosine::parse);
REGISTER_EXPRESSION(atanh, ExpressionHyperbolicArcTangent::parse);

/* ----------------------- Register Unbounded Single Argument Trigonometric Functions
 * ---------------------------- */
REGISTER_EXPRESSION(atan, ExpressionArcTangent::parse);
REGISTER_EXPRESSION(asinh, ExpressionHyperbolicArcSine::parse);
REGISTER_EXPRESSION(cos, ExpressionCosine::parse);
REGISTER_EXPRESSION(cosh, ExpressionHyperbolicCosine::parse);
REGISTER_EXPRESSION(sin, ExpressionSine::parse);
REGISTER_EXPRESSION(sinh, ExpressionHyperbolicSine::parse);
REGISTER_EXPRESSION(tan, ExpressionTangent::parse);
REGISTER_EXPRESSION(tanh, ExpressionHyperbolicTangent::parse);

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
            auto dec1 = numericArg1.coerceToDecimal();
            auto dec2 = numericArg2.coerceToDecimal();
            return Value(dec1.atan2(dec2));
        }
        case NumberDouble: {
            auto double1 = numericArg1.coerceToDouble();
            auto double2 = numericArg2.coerceToDouble();
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
static constexpr double DOUBLE_PI_OVER_180 = DOUBLE_PI / 180.0;
static constexpr double DOUBLE_180_OVER_PI = 180.0 / DOUBLE_PI;

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
    switch (numericArg.getType()) {
        case BSONType::NumberDecimal:
            return Value(numericArg.getDecimal().multiply(c.decimalFactor()));
        default:
            return Value(numericArg.coerceToDouble() * c.doubleFactor());
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
