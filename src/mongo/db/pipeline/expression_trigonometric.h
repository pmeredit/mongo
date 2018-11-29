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

#pragma once

#include "expression.h"

namespace mongo {

template <typename BoundedTrigType>
class ExpressionBoundedTrigonometric : public ExpressionSingleNumericArg<BoundedTrigType> {
public:
    explicit ExpressionBoundedTrigonometric(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionSingleNumericArg<BoundedTrigType>(expCtx) {}

    bool checkLowerBound(double input) const {
        return !getLowerBound() || input >= getLowerBound().get();
    }

    bool checkLowerBound(Decimal128 input) const {
        return !getLowerBound() || input.isGreaterEqual(Decimal128(getLowerBound().get()));
    }

    bool checkUpperBound(double input) const {
        return !getUpperBound() || input <= getUpperBound().get();
    }

    bool checkUpperBound(Decimal128 input) const {
        return !getUpperBound() || input.isLessEqual(Decimal128(getUpperBound().get()));
    }

    std::string toString(double d) const {
        return str::stream() << d;
    }

    std::string toString(Decimal128 d) const {
        return d.toString();
    }

	bool isnan(double d) const {
		return std::isnan(d);
	}

	bool isnan(Decimal128 d) const {
		return d.isNaN();
	}

    template <typename T>
    bool checkBounds(T input) const {
        return isnan(input) || (checkLowerBound(input) && checkUpperBound(input));
    }

    template <typename T>
    void assertBounds(T input) const {
        if (!checkBounds(input)) {
            std::string lowerBound;
            std::string upperBound;
            if (getLowerBound()) {
                lowerBound = str::stream() << "[" << getLowerBound().get();
            } else {
                lowerBound = str::stream() << "(-Infinity";
            }
            if (getUpperBound()) {
                upperBound = str::stream() << getUpperBound().get() << "]";
            } else {
                upperBound = str::stream() << "Infinity)";
            }
            uassert(50989,
                    str::stream() << "cannot apply " << getOpName() << " to " << toString(input)
                                  << ", value must in "
                                  << lowerBound
                                  << ","
                                  << upperBound,
                    false);
        }
    }

    Value evaluateNumericArg(const Value& numericArg) const {
		switch (numericArg.getType()) {
			case BSONType::NumberDouble: {
    	        auto input = numericArg.getDouble();
        	    assertBounds(input);
           	 	return Value(doubleFunc(input));
			}
            case BSONType::NumberDecimal: {
            	auto input = numericArg.getDecimal();
            	assertBounds(input);
            	return Value(decimalFunc(input));
			}
			default: {
            	auto input = static_cast<double>(numericArg.getLong());
            	assertBounds(input);
            	return Value(doubleFunc(input));
			}
		}
    }

    virtual boost::optional<double> getLowerBound() const = 0;
    virtual boost::optional<double> getUpperBound() const = 0;
    virtual double doubleFunc(double x) const = 0;
    virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
    virtual const char* getOpName() const = 0;
};

#define CREATE_BOUNDED_TRIGONOMETRIC_CLASS(className, funcName, lowerBound, upperBound)               \
   class Expression##className final : public ExpressionBoundedTrigonometric<Expression##className> { \
	   public:                                                                                        \
		   explicit Expression##className(const boost::intrusive_ptr<ExpressionContext>& expCtx)      \
	            : ExpressionBoundedTrigonometric(expCtx) {}                                           \
	   boost::optional<double> getLowerBound() const final {                                          \
           return lowerBound;                                                                         \
	   }                                                                                              \
 	                                                                                                  \
	   boost::optional<double> getUpperBound() const final {                                          \
           return upperBound;                                                                         \
	   }                                                                                              \
																									  \
	   double doubleFunc(double arg) const final { 													  \
		   return std::funcName(arg);  																  \
	   } 																							  \
																									  \
	   Decimal128 decimalFunc(Decimal128 arg) const final { 										  \
		   return arg.funcName();  																	  \
	   } 																							  \
	   																								  \
	   const char* getOpName() const final {  														  \
		   return "$funcName";                                                                        \
       }                                                                                              \
   }; 																								  \


CREATE_BOUNDED_TRIGONOMETRIC_CLASS(ArcCosine,
		acos,
		boost::optional<double>(-1.0),
		boost::optional<double>(1.0));

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(ArcSine,
		asin,
		boost::optional<double>(-1.0),
		boost::optional<double>(1.0));

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(HyperbolicArcTangent,
		atanh,
		boost::optional<double>(-1.0),
		boost::optional<double>(1.0));

CREATE_BOUNDED_TRIGONOMETRIC_CLASS(HyperbolicArcCosine,
		acosh,
		boost::optional<double>(1.0),
		boost::none);

#undef CREATE_BOUNDED_TRIGONOMETRIC_CLASS

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

#define CREATE_TRIGONOMETRIC_CLASS(className, funcName)                                               \
   class Expression##className final : public ExpressionTrigonometric<Expression##className> {        \
	   public:                                                                                        \
		   explicit Expression##className(const boost::intrusive_ptr<ExpressionContext>& expCtx)      \
	            : ExpressionTrigonometric(expCtx) {}                                                  \
																									  \
	   double doubleFunc(double arg) const final { 													  \
		   return std::funcName(arg);  																  \
	   } 																							  \
																									  \
	   Decimal128 decimalFunc(Decimal128 arg) const final { 										  \
		   return arg.funcName();  																	  \
	   } 																							  \
	   																								  \
	   const char* getOpName() const final {  														  \
		   return "$funcName";                                                                        \
       }                                                                                              \
   }; 																								  \

CREATE_TRIGONOMETRIC_CLASS(ArcTangent, atan);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicArcSine, asinh);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicCosine, cosh);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicSine, sinh);
CREATE_TRIGONOMETRIC_CLASS(HyperbolicTangent, tanh);
CREATE_TRIGONOMETRIC_CLASS(Cosine, cos);
CREATE_TRIGONOMETRIC_CLASS(Sine, sin);
CREATE_TRIGONOMETRIC_CLASS(Tangent, tan);

#undef CREATE_TRIGONOMETRIC_CLASS

class ExpressionArcTangent2 final : public ExpressionTwoNumericArgs<ExpressionArcTangent2> {
public:
    explicit ExpressionArcTangent2(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTwoNumericArgs(expCtx) {}

    Value evaluateNumericArgs(const Value& numericArg1, const Value& numericArg2) const;
    const char* getOpName() const final;
};


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
}
