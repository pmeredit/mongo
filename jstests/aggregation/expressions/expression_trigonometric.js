// SERVER-19548: Add $floor, $ceil, and $trunc aggregation expressions.

// For assertErrorCode.
load("jstests/aggregation/extras/utils.js");

(function() {
    "use strict";
    var coll = db.server19548;
    coll.drop();
    // Seed collection so that the pipeline will execute.
    assert.writeOK(coll.insert({}));

    // Helper for testing that op returns expResult.
    function testOp(op, expResult) {
        var pipeline = [{$project: {_id: 0, result: op}}];
        assert.eq(coll.aggregate(pipeline).toArray(), [{result: expResult}]);
    }

    // Helper for testing that op returns expResult, approximately, since NumberDecimal has
    // so many representations for a given number (0 versus 0e-40 for instance).
    function testOpApprox(op, expResult) {
        var pipeline = [{$project: {_id: 0, result: {$abs: {$subtract: [op, expResult]}}}}];
        assert.lt(coll.aggregate(pipeline).toArray(), [{result: NumberDecimal("0.00000005")}]);
    }

    // Simple successful double input.
    testOp({$cos: 0}, 1);
    testOp({$sin: 0}, 0);
    testOp({$tan: 0}, 0);
    testOp({$cosh: 0}, 1);
    testOp({$sinh: 0}, 0);
    testOp({$tanh: 0}, 0);
    testOp({$acos: 1}, 0);
    testOp({$asin: 0}, 0);
    testOp({$atan: 0}, 0);
    testOp({$atan2: [0, 1]}, 0);
    testOp({$acosh: 1}, 0);
    testOp({$asinh: 0}, 0);
    testOp({$atanh: 0}, 0);

    // Simple successful decimal input.
    testOpApprox({$cos: NumberDecimal(0)}, NumberDecimal(1));
    testOpApprox({$sin: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$tan: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$cosh: NumberDecimal(0)}, NumberDecimal(1));
    testOpApprox({$sinh: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$tanh: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$acos: NumberDecimal(1)}, NumberDecimal(0));
    testOpApprox({$asin: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$atan: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$atan2: [NumberDecimal(0), 1]}, NumberDecimal(0));
    testOpApprox({$acosh: NumberDecimal(1)}, NumberDecimal(0));
    testOpApprox({$asinh: NumberDecimal(0)}, NumberDecimal(0));
    testOpApprox({$atanh: NumberDecimal(0)}, NumberDecimal(0));


	// Infinities
    testOpApprox({$atan2: [NumberDecimal('Infinity'), 0]}, NumberDecimal("1.570796326794896619231321691639751"));
    testOpApprox({$atan2: [NumberDecimal('-Infinity'), 0]}, NumberDecimal("-1.570796326794896619231321691639751"));
    testOpApprox({$atan2: [NumberDecimal('-Infinity'), NumberDecimal("Infinity")]}, 
		NumberDecimal("-0.785398163397448309615660845819876"));
    testOpApprox({$atan2: [NumberDecimal('-Infinity'), NumberDecimal("-Infinity")]}, 
		NumberDecimal("-2.356194490192344928846982537459627"));
    testOpApprox({$atanh: NumberDecimal(1)}, NumberDecimal('Infinity'));
    testOpApprox({$atanh: NumberDecimal(-1)}, NumberDecimal('-Infinity'));
	testOpApprox({$cos: NumberDecimal('Infinity')}, NumberDecimal('Infinity'));

    // Simple successful long input.
    testOpApprox({$cos: NumberLong(0)}, 1);
    testOpApprox({$sin: NumberLong(0)}, 0);
    testOpApprox({$tan: NumberLong(0)}, 0);
    testOpApprox({$cosh: NumberLong(0)}, 1);
    testOpApprox({$sinh: NumberLong(0)}, 0);
    testOpApprox({$tanh: NumberLong(0)}, 0);
    testOpApprox({$acos: NumberLong(1)}, 0);
    testOpApprox({$asin: NumberLong(0)}, 0);
    testOpApprox({$atan: NumberLong(0)}, 0);
    testOpApprox({$atan2: [NumberLong(0), NumberLong(1)]}, 0);
    testOpApprox({$acosh: NumberLong(1)}, 0);
    testOpApprox({$asinh: NumberLong(0)}, 0);
    testOpApprox({$atanh: NumberLong(0)}, 0);

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
