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

	// check lower bound, return false if the input is less than that bound.
    bool checkInclusiveLowerBound(double input) const {
        return !getInclusiveLowerBound() || input >= getInclusiveLowerBound().get();
    }

	// check lower bound, return false if the input is less than that bound.
    bool checkInclusiveLowerBound(Decimal128 input) const {
        return !getInclusiveLowerBound() || input.isGreaterEqual(Decimal128(getInclusiveLowerBound().get()));
    }

	// check upper bound, return false if the input is greater than that bound.
    bool checkInclusiveUpperBound(double input) const {
        return !getInclusiveUpperBound() || input <= getInclusiveUpperBound().get();
    }

	// check upper bound, return false if the input is greater than that bound.
    bool checkInclusiveUpperBound(Decimal128 input) const {
        return !getInclusiveUpperBound() || input.isLessEqual(Decimal128(getInclusiveUpperBound().get()));
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
    bool checkInclusiveBounds(T input) const {
        return checkInclusiveLowerBound(input) && checkInclusiveUpperBound(input);
    }

	// assert if checkBounds returns false
    template <typename T>
    void assertInclusiveBounds(T input) const {
        if (!checkInclusiveBounds(input)) {
            std::string lowerBound;
            std::string upperBound;
            if (getInclusiveLowerBound()) {
                lowerBound = str::stream() << "[" << getInclusiveLowerBound().get();
            } else {
                lowerBound = str::stream() << "[-Infinity";
            }
            if (getInclusiveUpperBound()) {
                upperBound = str::stream() << getInclusiveUpperBound().get() << "]";
            } else {
                upperBound = str::stream() << "Infinity]";
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

	// evaluate the implemted trig function on one numericArg
    Value evaluateNumericArg(const Value& numericArg) const {
        switch (numericArg.getType()) {
            case BSONType::NumberDouble: {
                auto input = numericArg.getDouble();
				if (isnan(input)) {
					return numericArg;
				}
                assertInclusiveBounds(input);
                return Value(doubleFunc(input));
            }
            case BSONType::NumberDecimal: {
                auto input = numericArg.getDecimal();
				if (isnan(input)) {
					return numericArg;
				}
                assertInclusiveBounds(input);
                return Value(decimalFunc(input));
            }
            default: {
                auto input = static_cast<double>(numericArg.getLong());
				if (isnan(input)) {
					return numericArg;
				}
                assertInclusiveBounds(input);
                return Value(doubleFunc(input));
            }
        }
    }

	// gets the inclusive lower bound of the implemted bounded trig function.
    virtual boost::optional<double> getInclusiveLowerBound() const = 0;
	// gets the inclusive upper bound of the implemted bounded trig function.
    virtual boost::optional<double> getInclusiveUpperBound() const = 0;
	// doubleFunc performs the double version of the implemented trig function, e.g. std::sin
    virtual double doubleFunc(double x) const = 0;
	// decimalFunc performs the decimal128 version of the implemented trig function, e.g. d.sin()
    virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
	// getOpName returns the name of the operation, e.g., $sin
    virtual const char* getOpName() const = 0;
};
}  // namespace mongo
