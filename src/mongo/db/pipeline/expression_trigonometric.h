#include "expression.h"

namespace mongo {

template<typename TrigType>
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
          long long num = numericArg.getLong();
          uassert(50968,
                  str::stream() << "cannot apply trigonometric function to minimum long long",
                  num != std::numeric_limits<long long>::min());
          return Value(doubleFunc(num));
      }
	}

	virtual double doubleFunc(double x) const = 0;
	virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
};

template<typename BoundedTrigType>
class ExpressionBoundedTrigonometric : public ExpressionSingleNumericArg<BoundedTrigType> {
public:
    explicit ExpressionBoundedTrigonometric(const boost::intrusive_ptr<ExpressionContext>& expCtx)
      : ExpressionSingleNumericArg<BoundedTrigType>(expCtx) {}

    Value evaluateNumericArg(const Value& numericArg) const override {
      BSONType type = numericArg.getType();
      if (type == NumberDouble) {
		  auto input = numericArg.getDouble();
          uassert(50969,
                  str::stream() << "cannot apply inverse trigonometric function to "
				  << input <<  ", value must in [-1.0, 1.0]",
                  input >= -1.0 && input <= 1.0);
          return Value(doubleFunc(input));
      } else if (type == NumberDecimal) {
		  auto input = numericArg.getDecimal();
          uassert(50970,
                  str::stream() << "cannot apply inverse trigonometric function to "
				  << input.toDouble() <<  ", value must in [-1.0, 1.0]",
                  input.isGreaterEqual(Decimal128(-1))
					  && input.isLessEqual(Decimal128(1.0)));
          return Value(decimalFunc(numericArg.getDecimal()));
      } else {
          auto input = numericArg.getLong();
          uassert(50989,
                  str::stream() << "cannot take apply inverse trigonometric function to "
				  << input <<  ", value must in [-1.0, 1.0]",
                  input >= -1 && input <= 1);
          return Value(doubleFunc(input));
      }
	}

	virtual double doubleFunc(double x) const = 0;
	virtual Decimal128 decimalFunc(Decimal128 x) const = 0;
};

class ExpressionArcCosine final : public ExpressionBoundedTrigonometric<ExpressionArcCosine> {
public:
	explicit ExpressionArcCosine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
       : ExpressionBoundedTrigonometric(expCtx) {}

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionArcSine final : public ExpressionBoundedTrigonometric<ExpressionArcSine> {
public:
	explicit ExpressionArcSine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
       : ExpressionBoundedTrigonometric(expCtx) {}

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


class ExpressionHyperbolicArcCosine final : public ExpressionTrigonometric<ExpressionHyperbolicArcCosine> {
public:
	explicit ExpressionHyperbolicArcCosine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
       : ExpressionTrigonometric(expCtx) {}

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcSine final : public ExpressionTrigonometric<ExpressionHyperbolicArcSine> {
public:
	explicit ExpressionHyperbolicArcSine(const boost::intrusive_ptr<ExpressionContext>& expCtx)
       : ExpressionTrigonometric(expCtx) {}

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicArcTangent final : public ExpressionBoundedTrigonometric<ExpressionHyperbolicArcTangent> {
public:
	explicit ExpressionHyperbolicArcTangent(const boost::intrusive_ptr<ExpressionContext>& expCtx)
       : ExpressionBoundedTrigonometric(expCtx) {}

	double doubleFunc(double x) const final;
	Decimal128 decimalFunc(Decimal128 x) const final;
    const char* getOpName() const final;
};


class ExpressionHyperbolicCosine final : public ExpressionTrigonometric<ExpressionHyperbolicCosine> {
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


class ExpressionHyperbolicTangent final : public ExpressionTrigonometric<ExpressionHyperbolicTangent> {
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
//
//
//class ExpressionArcTangent2 final : public ExpressionDoubleNumericArgs<ExpressionArcTangent2> {
//public:
//	explicit ExpressionArcTangent2(const boost::intrusive_ptr<ExpressionContext>& expCtx)
//         : ExpressionDoubleNumericArgs(expCtx) {}
//
//	Value evaluateNumericArgs(const Value& numericArg1, const Value& numericArg2) const;
//	const char* getOpName() const final;
//};
//
//
//class ExpressionDegreesToRadians final : public ExpressionSingleNumericArg<ExpressionDegreesToRadians> {
//public:
//	explicit ExpressionDegreesToRadians(const boost::intrusive_ptr<ExpressionContext>& expCtx)
//         : ExpressionSingleNumericArg(expCtx) {}
//
//    Value evaluateNumericArg(const Value& numericArg) const final;
//    const char* getOpName() const final;
//};
//
//
//class ExpressionRadiansToDegrees final : public ExpressionSingleNumericArg<ExpressionRadiansToDegrees> {
//public:
//	explicit ExpressionRadiansToDegrees(const boost::intrusive_ptr<ExpressionContext>& expCtx)
//         : ExpressionSingleNumericArg(expCtx) {}
//
//    Value evaluateNumericArg(const Value& numericArg) const final;
//    const char* getOpName() const final;
//};
//}
}
