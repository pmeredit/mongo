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

template <typename TrigType>
class ExpressionTrigonometric : public ExpressionSingleNumericArg<TrigType> {
public:
    explicit ExpressionTrigonometric(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionSingleNumericArg<TrigType>(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const override {
        BSONType type = numericArg.getType();
        if (type == NumberDouble) {
            return Value(doubleFunc(numericArg.getDouble()));
        } else if (type == NumberDecimal) {
            return Value(decimalFunc(numericArg.getDecimal()));
        } else {
            auto num = static_cast<double>(numericArg.getLong());
            return Value(doubleFunc(num));
        }
    }

    virtual double doubleFunc(double x) const = 0;
    virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
    virtual const char* getOpName() const = 0;
};

template <typename BoundedTrigType>
class ExpressionBoundedTrigonometric : public ExpressionSingleNumericArg<BoundedTrigType> {
public:
    explicit ExpressionBoundedTrigonometric(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionSingleNumericArg<BoundedTrigType>(expCtx) {}


	template<typename T>
	void assertBounds(T input, bool assertionResult) const {
        uassert(50989,
           str::stream() << "cannot apply " << getOpName() << " to " << input
                         << ", value must in ["
                         << getLowerBound()
                         << ","
                         << getUpperBound()
                         << "]", assertionResult);
	}

	bool checkLowerBound(double input) const {
		if (!getLowerBound()) {
			return true;
		}
		if (isInclusive()) {
			return input >= getLowerBound().get();
		}
		return input > getLowerBound().get();
	}

	bool checkUpperBound(Decimal128 input) const {
		if (!getLowerBound()) {
			return true;
		}
		if (isInclusive()) {
			return input.isGreaterEqual(Decimal128(getLowerBound().get()));
		}
		return input.isGreater(Decimal128(getLowerBound().get()));
	}

    Value evaluateInclusive(const Value& numericArg) const {
        BSONType type = numericArg.getType();
        if (type == NumberDouble) {
            auto input = numericArg.getDouble();
                    assertBounds(input,
					  ((!getLowerBound() || input >= getLowerBound().get()) &&
                       (!getUpperBound() || input <= getUpperBound().get())));
            return Value(doubleFunc(input));
        } else if (type == NumberDecimal) {
            auto input = numericArg.getDecimal();
			        // toString is necessary because operator<< cannot work on
					// str::stream and Decimal128 through a templated function.
                    assertBounds(input.toString(),
				      ((!getLowerBound() || input.isGreaterEqual(Decimal128(getLowerBound().get()))) ||
                       (!getUpperBound() || input.isLessEqual(Decimal128(getUpperBound().get())))));
            return Value(decimalFunc(input));
        } else {
            auto input = static_cast<double>(numericArg.getLong());
                    assertBounds(input,
					  ((!getLowerBound() || input >= getLowerBound().get()) &&
                       (!getUpperBound() || input <= getUpperBound().get())));
            return Value(doubleFunc(input));
        }
    }

    Value evaluateNonInclusive(const Value& numericArg) const {
        BSONType type = numericArg.getType();
        if (type == NumberDouble) {
            auto input = numericArg.getDouble();
                assertBounds(input,
					  ((!getLowerBound() || input > getLowerBound().get()) ||
                       (!getUpperBound() || input < getUpperBound().get())));
            return Value(doubleFunc(input));
        } else if (type == NumberDecimal) {
            auto input = numericArg.getDecimal();
			        // toString is necessary because operator<< cannot work on
					// str::stream and Decimal128 through a templated function.
                    assertBounds(input.toString(),
				      ((!getLowerBound() || input.isGreater(Decimal128(getLowerBound().get()))) ||
                       (!getUpperBound() || input.isLess(Decimal128(getUpperBound().get())))));
            return Value(decimalFunc(input));
        } else {
            auto input = static_cast<double>(numericArg.getLong());
			    assertBounds(input,
                      ((!getLowerBound() || input > getLowerBound().get()) ||
                       (!getUpperBound() || input < getUpperBound().get())));
            return Value(doubleFunc(input));
        }
    }

    Value evaluateNumericArg(const Value& numericArg) const override {
        if (isInclusive()) {
            return evaluateInclusive(numericArg);
        }
        return evaluateNonInclusive(numericArg);
    }

    virtual bool isInclusive() const = 0;
    virtual boost::optional<double> getLowerBound() const = 0;
    virtual boost::optional<double> getUpperBound() const = 0;
    virtual double doubleFunc(double x) const = 0;
    virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
    virtual const char* getOpName() const = 0;
};

class ExpressionArcCosine final : public ExpressionBoundedTrigonometric<ExpressionArcCosine> {
public:
    explicit ExpressionArcCosine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionBoundedTrigonometric(expCtx) {}

    virtual bool isInclusive() const final;
    boost::optional<double> getLowerBound() const final;
    boost::optional<double> getUpperBound() const final;
    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionArcSine final : public ExpressionBoundedTrigonometric<ExpressionArcSine> {
public:
    explicit ExpressionArcSine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionBoundedTrigonometric(expCtx) {}

    virtual bool isInclusive() const final;
    boost::optional<double> getLowerBound() const final;
    boost::optional<double> getUpperBound() const final;
    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionArcTangent final : public ExpressionTrigonometric<ExpressionArcTangent> {
public:
    explicit ExpressionArcTangent(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}

    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcCosine final
    : public ExpressionBoundedTrigonometric<ExpressionHyperbolicArcCosine> {
public:
    explicit ExpressionHyperbolicArcCosine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionBoundedTrigonometric(expCtx) {}

    virtual bool isInclusive() const final;
    boost::optional<double> getLowerBound() const final;
    boost::optional<double> getUpperBound() const final;
    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcSine final
    : public ExpressionTrigonometric<ExpressionHyperbolicArcSine> {
public:
    explicit ExpressionHyperbolicArcSine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}

    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcTangent final
    : public ExpressionBoundedTrigonometric<ExpressionHyperbolicArcTangent> {
public:
    explicit ExpressionHyperbolicArcTangent(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionBoundedTrigonometric(expCtx) {}

    virtual bool isInclusive() const final;
    boost::optional<double> getLowerBound() const final;
    boost::optional<double> getUpperBound() const final;
    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicCosine final
    : public ExpressionTrigonometric<ExpressionHyperbolicCosine> {
public:
    explicit ExpressionHyperbolicCosine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}

    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicSine final : public ExpressionTrigonometric<ExpressionHyperbolicSine> {
public:
    explicit ExpressionHyperbolicSine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}

    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicTangent final
    : public ExpressionTrigonometric<ExpressionHyperbolicTangent> {
public:
    explicit ExpressionHyperbolicTangent(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}


    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionCosine final : public ExpressionTrigonometric<ExpressionCosine> {
public:
    explicit ExpressionCosine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}

    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionSine final : public ExpressionTrigonometric<ExpressionSine> {
public:
    explicit ExpressionSine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}

    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionTangent final : public ExpressionTrigonometric<ExpressionTangent> {
public:
    explicit ExpressionTangent(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionTrigonometric(expCtx) {}

    double doubleFunc(double x) const final;
    Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionArcTangent2 final : public ExpressionDoubleNumericArgs<ExpressionArcTangent2> {
public:
    explicit ExpressionArcTangent2(const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : ExpressionDoubleNumericArgs(expCtx) {}

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
