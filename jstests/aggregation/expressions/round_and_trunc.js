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

	// 1 argument $trunc and $round
    testOp({$trunc: NumberLong(4)}, NumberLong(4));
    testOp({$trunc: NaN}, NaN);
    testOp({$trunc: Infinity}, Infinity);
    testOp({$trunc: -Infinity}, -Infinity);
    testOp({$trunc: null}, null);
    testOp({$trunc: -2.0}, -2.0);
    testOp({$trunc: 0.9}, 0.0);
    testOp({$trunc: -1.2}, -1.0);

    testOp({$round: NumberLong(4)}, NumberLong(4));
    testOp({$round: NaN}, NaN);
    testOp({$round: Infinity}, Infinity);
    testOp({$round: -Infinity}, -Infinity);
    testOp({$round: null}, null);
    testOp({$round: -2.0}, -2.0);
    testOp({$round: 0.9}, 1.0);
    testOp({$round: -1.2}, -1.0);

    // 2 argument $trunc and $round.
    testOp({$trunc: [1.298, 0]}, 1);
    testOp({$trunc: [1.298, 1]}, 1.2);
    testOp({$trunc: [23.298, -1]}, 20);
    testOp({$trunc: [NumberDecimal("1.298"), 0]}, NumberDecimal("1"));
    testOp({$trunc: [NumberDecimal("1.298"), 1]}, NumberDecimal("1.2"));
    testOp({$trunc: [NumberDecimal("23.298"), -1]}, NumberDecimal("2E+1"));
    testOp({$trunc: [1.298, 100]}, 1.298);
    testOp({$trunc: [NumberDecimal("1.298912343250054252245154325"), NumberLong("20")]}, NumberDecimal("1.29891234325005425224"));
    testOp({$trunc: [NumberDecimal("1.298"), NumberDecimal("100")]}, NumberDecimal("1.298"));

    testOp({$round: [1.298, 0]}, 1);
    testOp({$round: [1.298, 1]}, 1.3);
    testOp({$round: [23.298, -1]}, 20);
    testOp({$round: [NumberDecimal("1.298"), 0]}, NumberDecimal("1"));
    testOp({$round: [NumberDecimal("1.298"), 1]}, NumberDecimal("1.3"));
    testOp({$round: [NumberDecimal("23.298"), -1]}, NumberDecimal("2E+1"));
    testOp({$round: [1.298, 100]}, 1.298);
    testOp({$round: [NumberDecimal("1.298912343250054252245154325"), NumberLong("20")]}, NumberDecimal("1.29891234325005425225"));
    testOp({$round: [NumberDecimal("1.298"), NumberDecimal("100")]}, NumberDecimal("1.298"));


	// $round overfllow.
	testOp({$round: [NumberInt("2147483647"), -1]}, NumberLong("2147483650"));
	assertErrorCode(coll, [{$project: {a: {$round: [NumberLong("9223372036854775806"), -1]}}}], 51021);

    // More than 2 arguments
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, 2, 3]}}}], 28667);
    assertErrorCode(coll, [{$project: {a: {$round: [1, 2, 3]}}}], 28667);

    // Non-numeric input.
    assertErrorCode(coll, [{$project: {a: {$round: "string"}}}], 51022);
    assertErrorCode(coll, [{$project: {a: {$trunc: "string"}}}], 51022);

	// Out of bounds precision
    assertErrorCode(coll, [{$project: {a: {$round: [1, NumberLong("101")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$round: [1, NumberLong("-21")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$round: [1, NumberDecimal("101")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$round: [1, NumberDecimal("-21")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$round: [1, NumberInt("101")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$round: [1, NumberInt("-21")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$round: [1, 101]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$round: [1, -21]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, NumberLong("101")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, NumberLong("-21")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, NumberDecimal("101")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, NumberDecimal("-21")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, NumberInt("101")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, NumberInt("-21")]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, 101]}}}], 51023);
    assertErrorCode(coll, [{$project: {a: {$trunc: [1, -21]}}}], 51023);
}());
