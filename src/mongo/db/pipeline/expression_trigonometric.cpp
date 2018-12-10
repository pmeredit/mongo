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

#include "mongo/platform/basic.h"

#include "expression_trigonometric.h"

namespace mongo {

/* ----------------------- Inclusive Bounded Trigonometric Functions ---------------------------- */

#define CREATE_BOUNDED_TRIGONOMETRIC_CLASS(className, funcName, boundType, lowerBound, upperBound)\
    class Expression##className final                                                             \
        : public ExpressionBoundedTrigonometric<Expression##className, boundType> {               \
    public:                                                                                       \
        explicit Expression##className(const boost::intrusive_ptr<ExpressionContext>& expCtx)     \
            : ExpressionBoundedTrigonometric(expCtx) {}                                           \
        double getLowerBound() const final {                                                      \
            return lowerBound;                                                                    \
        }                                                                                         \
                                                                                                  \
        double getUpperBound() const final {                                                      \
            return upperBound;                                                                    \
        }                                                                                         \
                                                                                                  \
        double doubleFunc(double arg) const final {                                               \
            return std::funcName(arg);                                                            \
        }                                                                                         \
                                                                                                  \
        Decimal128 decimalFunc(Decimal128 arg) const final {                                      \
            return arg.funcName();                                                                \
        }                                                                                         \
                                                                                                  \
        const char* getOpName() const final {                                                     \
            return "$"#funcName;                                                                  \
        }                                                                                         \
    };                                                                                            \
    REGISTER_EXPRESSION(funcName, Expression##className::parse);                                  \


CREATE_BOUNDED_TRIGONOMETRIC_CLASS(ArcCosine,
                         acos,
						 InclusiveBoundType,
                         -1.0,
                         1.0);

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(ArcSine,
                         asin,
						 InclusiveBoundType,
                         -1.0,
                         1.0);

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(HyperbolicArcTangent,
                         atanh,
						 InclusiveBoundType,
                         -1.0,
                         1.0);

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(HyperbolicArcCosine,
                                   acosh,
						 InclusiveBoundType,
                                   1.0,
                                   std::numeric_limits<double>::infinity());

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(Cosine,
		                           cos,
						 ExclusiveBoundType,
								   -std::numeric_limits<double>::infinity(),
								   std::numeric_limits<double>::infinity());

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(Sine,
		                           sin,
						 ExclusiveBoundType,
								   -std::numeric_limits<double>::infinity(),
								   std::numeric_limits<double>::infinity());

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(Tangent,
		                           tan,
						 ExclusiveBoundType,
								   -std::numeric_limits<double>::infinity(),
								   std::numeric_limits<double>::infinity());

#undef CREATE_BOUNDED_TRIGONOMETRIC_CLASS

/* ----------------------- Unbounded Trigonometric Functions ---------------------------- */

template <typename TrigType>
class ExpressionTrigonometric : public ExpressionSingleNumericArg<TrigType> {
public:
    explicit ExpressionTrigonometric(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionSingleNumericArg<TrigType>(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const override {
        switch (numericArg.getType()) {
            case BSONType::NumberDouble:
                return Value(doubleFunc(numericArg.getDouble()));
            case BSONType::NumberDecimal:
                return Value(decimalFunc(numericArg.getDecimal()));
            default: {
                auto num = static_cast<double>(numericArg.getLong());
                return Value(doubleFunc(num));
            }
        }
    }

    virtual double doubleFunc(double x) const = 0;
    virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
    virtual const char* getOpName() const = 0;
};

#define CREATE_TRIGONOMETRIC_CLASS(className, funcName)                                         \
    class Expression##className final : public ExpressionTrigonometric<Expression##className> { \
    public:                                                                                     \
        explicit Expression##className(const boost::intrusive_ptr<ExpressionContext>& expCtx)   \
            : ExpressionTrigonometric(expCtx) {}                                                \
                                                                                                \
        double doubleFunc(double arg) const final {                                             \
            return std::funcName(arg);                                                          \
        }                                                                                       \
                                                                                                \
        Decimal128 decimalFunc(Decimal128 arg) const final {                                    \
            return arg.funcName();                                                              \
        }                                                                                       \
                                                                                                \
        const char* getOpName() const final {                                                   \
            return "$"#funcName;                                                                \
        }                                                                                       \
    };                                                                                          \
    REGISTER_EXPRESSION(funcName, Expression##className::parse);                                \

CREATE_TRIGONOMETRIC_CLASS(ArcTangent, atan);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicArcSine, asinh);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicCosine, cosh);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicSine, sinh);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicTangent, tanh);

#undef CREATE_TRIGONOMETRIC_CLASS


/* ----------------------- ExpressionArcTangent2 ---------------------------- */

class ExpressionArcTangent2 final : public ExpressionTwoNumericArgs<ExpressionArcTangent2> {
public:
    explicit ExpressionArcTangent2(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTwoNumericArgs(expCtx) {}

    Value evaluateNumericArgs(const Value& numericArg1, const Value& numericArg2) const final;
    const char* getOpName() const final;
};

Value ExpressionArcTangent2::evaluateNumericArgs(const Value& numericArg1,
                                                 const Value& numericArg2) const {
    BSONType type1 = numericArg1.getType();
    BSONType type2 = numericArg2.getType();
    auto totalType = BSONType::NumberDouble;
    // If the type of either argument is NumberDecimal, we promote to Decimal128.
    if (type1 == BSONType::NumberDecimal || type2 == BSONType::NumberDecimal) {
        totalType = BSONType::NumberDecimal;
    }
    switch (totalType) {
        case BSONType::NumberDecimal: {
            auto dec1 = numericArg1.coerceToDecimal();
            auto dec2 = numericArg2.coerceToDecimal();
            return Value(dec1.atan2(dec2));
        }
        case BSONType::NumberDouble: {
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

class ExpressionDegreesToRadians final
    : public ExpressionSingleNumericArg<ExpressionDegreesToRadians> {
public:
    explicit ExpressionDegreesToRadians(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionSingleNumericArg(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const final;
    const char* getOpName() const final;
};


class ExpressionRadiansToDegrees final
    : public ExpressionSingleNumericArg<ExpressionRadiansToDegrees> {
public:
    explicit ExpressionRadiansToDegrees(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionSingleNumericArg(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const final;
    const char* getOpName() const final;
};

static constexpr double DOUBLE_PI = 3.141592653589793;
static constexpr double DOUBLE_PI_OVER_180 = DOUBLE_PI / 180.0;
static constexpr double DOUBLE_180_OVER_PI = 180.0 / DOUBLE_PI;

struct DegreesToRadians {
    Decimal128 decimalFactor() {
        return Decimal128::kPIOver180;
    }
    double doubleFactor() {
        return DOUBLE_PI_OVER_180;
    }
};

struct RadiansToDegrees {
    Decimal128 decimalFactor() {
        return Decimal128::k180OverPI;
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
} // namespace mongo
