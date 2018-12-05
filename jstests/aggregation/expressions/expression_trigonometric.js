// SERVER-32930: Basic integration tests for trigonometric aggregation expressions.

(function() {
    "use strict";
    // For assertErrorCode.
    load("jstests/aggregation/extras/utils.js");

    const coll = db.expression_trigonometric;
    coll.drop();
    // We need at least one document in the collection in order to
	// test expressions, add it here.
    assert.commandWorked(coll.insert({}), {w: 'majority'});

    // Helper for testing that op returns expResult.
    function testOp(op, expResult) {
        const pipeline = [{$project: {_id: 0, result: op}}];
        assert.eq(coll.aggregate(pipeline).toArray(), [{result: expResult}]);
    }

	// Helper for testing that the aggregation expression 'op' returns
	// expResult, approximately, since NumberDecimal has so many
	// representations for a given number (0 versus 0e-40 for instance).
    function testOpApprox(op, expResult) {
        const pipeline = [{$project: {_id: 0, result: {$abs: {$subtract: [op, expResult]}}}}];
        assert.lt(coll.aggregate(pipeline).toArray(), [{result: NumberDecimal("0.00000005")}]);
    }

    // Simple successful int input.
    testOp({$acos: NumberInt(1)}, 0);
    testOp({$acosh: NumberInt(1)}, 0);
    testOp({$asin: NumberInt(0)}, 0);
    testOp({$asinh: NumberInt(0)}, 0);
    testOp({$atan: NumberInt(0)}, 0);
    testOp({$atan2: [NumberInt(0), NumberInt(1)]}, 0);
    testOp({$atanh: NumberInt(0)}, 0);
    testOp({$cos: NumberInt(0)}, 1);
    testOp({$cosh: NumberInt(0)}, 1);
    testOp({$sin: NumberInt(0)}, 0);
    testOp({$sinh: NumberInt(0)}, 0);
    testOp({$tan: NumberInt(0)}, 0);
    testOp({$tanh: NumberInt(0)}, 0);

    // Simple successful long input.
    testOp({$acos: NumberLong(1)}, 0);
    testOp({$acosh: NumberLong(1)}, 0);
    testOp({$asin: NumberLong(0)}, 0);
    testOp({$asinh: NumberLong(0)}, 0);
    testOp({$atan: NumberLong(0)}, 0);
    testOp({$atan2: [NumberLong(0), NumberLong(1)]}, 0);
    testOp({$atanh: NumberLong(0)}, 0);
    testOp({$cos: NumberLong(0)}, 1);
    testOp({$cosh: NumberLong(0)}, 1);
    testOp({$sin: NumberLong(0)}, 0);
    testOp({$sinh: NumberLong(0)}, 0);
    testOp({$tan: NumberLong(0)}, 0);
    testOp({$tanh: NumberLong(0)}, 0);

    // Simple successful double input.
    testOp({$acos: 1}, 0);
    testOp({$acosh: 1}, 0);
    testOp({$asin: 0}, 0);
    testOp({$asinh: 0}, 0);
    testOp({$atan: 0}, 0);
    testOp({$atan2: [0, 1]}, 0);
    testOp({$atanh: 0}, 0);
    testOp({$cos: 0}, 1);
    testOp({$cosh: 0}, 1);
    testOp({$sin: 0}, 0);
    testOp({$sinh: 0}, 0);
    testOp({$tan: 0}, 0);
    testOp({$tanh: 0}, 0);

    // Simple successful decimal input.
    testOpApprox({$acos: NumberDecimal(1)}, NumberDecimal(0));
    testOpApprox({$acosh: NumberDecimal(1)}, NumberDecimal(0));
    testOpApprox({$asin: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$asinh: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$atan: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$atan2: [NumberDecimal(0), 1]}, NumberDecimal(0));
    testOpApprox({$atanh: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$cos: NumberDecimal(0)}, NumberDecimal(1));
    testOpApprox({$cosh: NumberDecimal(0)}, NumberDecimal(1));
    testOpApprox({$sin: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$sinh: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$tan: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$tanh: NumberDecimal(0)}, NumberDecimal(0));

    // Infinities
    assertErrorCode(coll, [{$project: {a: {$acos: -Infinity}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acos: NumberDecimal('-Infinity')}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acos: Infinity}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acos: NumberDecimal('Infinity')}}}], 50989);
    testOp({$acosh: NumberDecimal('Infinity')}, NumberDecimal('Infinity'));
    testOp({$acosh: Infinity}, Infinity);
    assertErrorCode(coll, [{$project: {a: {$acosh: -Infinity}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acosh: NumberDecimal('-Infinity')}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: -Infinity}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: NumberDecimal('-Infinity')}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: Infinity}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: NumberDecimal('Infinity')}}}], 50989);
    testOp({$asinh: NumberDecimal('Infinity')}, NumberDecimal('Infinity'));
    testOp({$asinh: NumberDecimal('-Infinity')}, NumberDecimal('-Infinity'));
    testOp({$asinh: Infinity}, Infinity);
    testOp({$asinh: -Infinity}, -Infinity);
    testOp({$atan: NumberDecimal('Infinity')},
           NumberDecimal('1.570796326794896619231321691639751'));
    testOp({$atan: NumberDecimal('-Infinity')},
           NumberDecimal('-1.570796326794896619231321691639751'));
    testOp({$atan: Infinity}, 1.5707963267948966);
    testOp({$atan: -Infinity}, -1.5707963267948966);
    testOp({$atan2: [NumberDecimal('Infinity'), 0]},
           NumberDecimal("1.570796326794896619231321691639751"));
    testOp({$atan2: [NumberDecimal('-Infinity'), 0]},
           NumberDecimal("-1.570796326794896619231321691639751"));
    testOp({$atan2: [NumberDecimal('-Infinity'), NumberDecimal("Infinity")]},
           NumberDecimal("-0.785398163397448309615660845819876"));
    testOp({$atan2: [NumberDecimal('-Infinity'), NumberDecimal("-Infinity")]},
           NumberDecimal("-2.356194490192344928846982537459627"));
    testOp({$atanh: NumberDecimal(1)}, NumberDecimal('Infinity'));
    testOp({$atanh: NumberDecimal(-1)}, NumberDecimal('-Infinity'));
    testOp({$atanh: 1}, Infinity);
    testOp({$atanh: -1}, -Infinity);
    testOp({$cos: NumberDecimal("Infinity")}, NumberDecimal('NaN'));
    testOp({$cos: NumberDecimal("-Infinity")}, NumberDecimal('NaN'));
    testOp({$cosh: NumberDecimal('Infinity')}, NumberDecimal('Infinity'));
    testOp({$cosh: NumberDecimal('-Infinity')}, NumberDecimal('Infinity'));
    testOp({$cosh: Infinity}, Infinity);
    testOp({$cosh: -Infinity}, Infinity);
    testOp({$sin: NumberDecimal('Infinity')}, NumberDecimal('NaN'));
    testOp({$sin: NumberDecimal('-Infinity')}, NumberDecimal('NaN'));
    testOp({$sinh: NumberDecimal('Infinity')}, NumberDecimal('Infinity'));
    testOp({$sinh: NumberDecimal('-Infinity')}, NumberDecimal('-Infinity'));
    testOp({$sinh: Infinity}, Infinity);
    testOp({$sinh: -Infinity}, -Infinity);
    testOp({$tan: NumberDecimal('Infinity')}, NumberDecimal('NaN'));
    testOp({$tan: NumberDecimal('-Infinity')}, NumberDecimal('NaN'));
    testOp({$tanh: NumberDecimal('Infinity')}, NumberDecimal('1'));
    testOp({$tanh: NumberDecimal('-Infinity')}, NumberDecimal('-1'));
    testOp({$tanh: Infinity}, 1);
    testOp({$tanh: -Infinity}, -1);

    // Double argument out of bounds.
    assertErrorCode(coll, [{$project: {a: {$acos: -1.1}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acos: 1.1}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: -1.1}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: 1.1}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acosh: 0.9}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$atanh: -1.00001}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$atanh: 1.00001}}}], 50989);

    // Decimal argument out of bounds.
    assertErrorCode(coll, [{$project: {a: {$acos: NumberDecimal(-1.1)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acos: NumberDecimal(1.1)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: NumberDecimal(-1.1)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: NumberDecimal(1.1)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acosh: NumberDecimal(0.9)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$atanh: NumberDecimal(-1.00001)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$atanh: NumberDecimal(1.000001)}}}], 50989);

    // Long argument out of bounds.
    assertErrorCode(coll, [{$project: {a: {$acos: NumberLong(-2)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acos: NumberLong(2)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: NumberLong(-2)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$asin: NumberLong(2)}}}], 50989);
    assertErrorCode(coll, [{$project: {a: {$acosh: NumberLong(0)}}}], 50989);

    // Check that NaN still works in bounded trig functions
    testOp({$acos: NumberDecimal('NaN')}, NumberDecimal('NaN'));
    testOp({$asin: NumberDecimal('NaN')}, NumberDecimal('NaN'));
    testOp({$acosh: NumberDecimal('NaN')}, NumberDecimal('NaN'));
    testOp({$atanh: NumberDecimal('NaN')}, NumberDecimal('NaN'));

    testOp({$acos: NaN}, NaN);
    testOp({$asin: NaN}, NaN);
    testOp({$acosh: NaN}, NaN);
    testOp({$atanh: NaN}, NaN);

    // Non-numeric input.
    assertErrorCode(coll, [{$project: {a: {$cos: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$sin: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$tan: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$cosh: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$sinh: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$tanh: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$acos: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$asin: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$atan: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$atan2: ["string", 0.0]}}}], 50981);
    assertErrorCode(coll, [{$project: {a: {$acosh: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$asinh: "string"}}}], 28765);
    assertErrorCode(coll, [{$project: {a: {$atanh: "string"}}}], 28765);
}());
