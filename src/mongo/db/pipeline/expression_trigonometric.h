#include "expression.h"

namespace mongo {
class ExpressionTrigonometric : public ExpressionSingleNumericArg<ExpressionTrigonometric> {
public:
    explicit ExpressionTrigonometric(const boost::intrusive_ptr<ExpressionContext>& expCtx)
      : ExpressionSingleNumericArg<ExpressionTrigonometric>(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const override {
      BSONType type = numericArg.getType();
      if (type == NumberDouble) {
          return Value(doubleFunc(numericArg.getDouble()));
      } else if (type == NumberDecimal) {
          return Value(decimalFunc(numericArg.getDecimal()));
      } else {
          long long num = numericArg.getLong();
          uassert(50968,
                  str::stream() << "can't take "
				  << getOpName()
				  << " of long long min",
                  num != std::numeric_limits<long long>::min());
          return Value(doubleFunc(num));
      }
	}

	virtual double doubleFunc(double x) const {
	    return 0.0;
	}
	virtual Decimal128 decimalFunc(Decimal128 x) const {
        return Decimal128::kNormalizedZero;
	}
	virtual const char* getOpName() const {
        return "$trigFunc";
	}
};

class ExpressionArcCosine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionArcSine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionArcTangent final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcCosine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcSine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcTangent final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicCosine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicSine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicTangent final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionCosine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionSine final : public ExpressionTrigonometric {
public:

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionTangent final : public ExpressionTrigonometric {
public:

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


class ExpressionDegreesToRadians final : public ExpressionSingleNumericArg<ExpressionDegreesToRadians> {
public:
	explicit ExpressionDegreesToRadians(const boost::intrusive_ptr<ExpressionContext>& expCtx)
         : ExpressionSingleNumericArg(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const final;
    const char* getOpName() const final;
};


class ExpressionRadiansToDegrees final : public ExpressionSingleNumericArg<ExpressionRadiansToDegrees> {
public:
	explicit ExpressionRadiansToDegrees(const boost::intrusive_ptr<ExpressionContext>& expCtx)
         : ExpressionSingleNumericArg(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const final;
    const char* getOpName() const final;
};
}
