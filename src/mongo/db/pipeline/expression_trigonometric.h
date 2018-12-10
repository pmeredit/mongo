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

/*
 * InclusiveBoundType defines the necessary configuration
 * for inclusively bounded trig functions.
 */
struct InclusiveBoundType {
	static std::string leftBracket() {
		return "[";
	}

	static std::string rightBracket() {
		return  "]";
	}

	static bool lt(double input, double bound) {
		return input <= bound;
	}
	
	static bool lt(Decimal128 input, double bound) {
		return input.isLessEqual(Decimal128(bound));
	}

	static bool gt(double input, double bound) {
		return input >= bound;
	}
	
	static bool gt(Decimal128 input, double bound) {
		return input.isGreaterEqual(Decimal128(bound));
	}
};

/*
 * ExclusiveBoundType defines the necessary configuration
 * for exclusively bounded trig functions.
 */
struct ExclusiveBoundType {
	static std::string leftBracket() {
		return "(";
	}

	static std::string rightBracket() {
		return  ")";
	}

	static bool lt(double input, double bound) {
		return input < bound;
	}
	
	static bool lt(Decimal128 input, double bound) {
		return input.isLess(Decimal128(bound));
	}

	static bool gt(double input, double bound) {
		return input > bound;
	}
	
	static bool gt(Decimal128 input, double bound) {
		return input.isGreater(Decimal128(bound));
	}
};

template <typename BoundedTrigType, typename BoundType>
class ExpressionBoundedTrigonometric : public ExpressionSingleNumericArg<BoundedTrigType> {
public:
    explicit ExpressionBoundedTrigonometric(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionSingleNumericArg<BoundedTrigType>(expCtx) {}

	// check lower bound, return false if the input is less than that bound.
    template <typename T>
	bool checkLowerBound(T input) const {
        return BoundType::gt(input, getLowerBound());
    }

	// check upper bound, return false if the input is greater than that bound.
    template <typename T>
    bool checkUpperBound(T input) const {
        return BoundType::lt(input, getUpperBound());
    }

	// convert to string for error message purposes.
    std::string toString(double d) const {
        return str::stream() << d;
    }

	// convert to string for error message purposes.
    std::string toString(Decimal128 d) const {
        return d.toString();
    }

	// check if double is nan
    bool isnan(double d) const {
        return std::isnan(d);
    }

	// check if Decimal128 is nan
    bool isnan(Decimal128 d) const {
        return d.isNaN();
    }

	// check both bounds, works on both double and Decimal128
    template <typename T>
    bool checkBounds(T input) const {
        return checkLowerBound(input) && checkUpperBound(input);
    }

	// assert if checkBounds returns false
    template <typename T>
    void assertBounds(T input) const {
        if (!checkBounds(input)) {
            uassert(50989,
                    str::stream() << "cannot apply " << getOpName() << " to " << toString(input)
                                  << ", value must in "
                                  << BoundType::leftBracket() << getLowerBound()
                                  << ","
                                  << getUpperBound() << BoundType::rightBracket(),
                    false);
        }
    }

	// evaluate the implented trig function on one numericArg
    Value evaluateNumericArg(const Value& numericArg) const {
        switch (numericArg.getType()) {
            case BSONType::NumberDouble: {
                auto input = numericArg.getDouble();
				if (isnan(input)) {
					return numericArg;
				}
                assertBounds(input);
                return Value(doubleFunc(input));
            }
            case BSONType::NumberDecimal: {
                auto input = numericArg.getDecimal();
				if (isnan(input)) {
					return numericArg;
				}
                assertBounds(input);
                return Value(decimalFunc(input));
            }
            default: {
                auto input = static_cast<double>(numericArg.getLong());
				if (isnan(input)) {
					return numericArg;
				}
                assertBounds(input);
                return Value(doubleFunc(input));
            }
        }
    }

	/* Since bounds are always either Infinity or integral values, double has enough
	 * precision.
	 * gets the lower bound of the implented bounded trig function.
	 */

    virtual double getLowerBound() const = 0;
    virtual double getUpperBound() const = 0;
	/*
	 * doubleFunc performs the double version of the implemented trig function, e.g. std::sin()
	 */
    virtual double doubleFunc(double x) const = 0;
	/*
	 * decimalFunc performs the decimal128 version of the implemented trig function, e.g. d.sin()
	 */
    virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
	/*
	 * getOpName returns the name of the operation, e.g., $sin
	 */
    virtual const char* getOpName() const = 0;
};
}  // namespace mongo
