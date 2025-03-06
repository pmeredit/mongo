/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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

#include <cstdint>

#include <absl/container/node_hash_map.h>

#include "mongo/base/string_data.h"
#include "mongo/db/exec/docval_to_sbeval.h"
#include "mongo/db/exec/sbe/expression_test_base.h"
#include "mongo/db/query/collation/collator_interface_mock.h"
#include "mongo/db/query/optimizer/algebra/operator.h"
#include "mongo/db/query/optimizer/comparison_op.h"
#include "mongo/db/query/stage_builder/sbe/abt_lower.h"
#include "mongo/db/query/stage_builder/sbe/abt_lower_defs.h"
#include "mongo/db/query/stage_builder/sbe/expression_const_eval.h"
#include "mongo/db/query/stage_builder/sbe/tests/abt_unit_test_literals.h"
#include "mongo/db/query/stage_builder/sbe/tests/abt_unit_test_utils.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/unittest/unittest.h"

namespace mongo::stage_builder {
namespace {

using namespace optimizer;
using namespace abt::unit_test_abt_literals;
using namespace mongo::stage_builder::abt;

class AbtToSbeExpression : public sbe::EExpressionTestFixture {
public:
    ABT constFold(ABT tree) {
        stage_builder::ExpressionConstEval{nullptr /* collator */}.optimize(tree);
        return tree;
    }

    // Helper that lowers and compiles an ABT expression and returns the evaluated result.
    // If the expression contains a variable, it will be bound to a slot along with its
    // definition before lowering.
    std::pair<sbe::value::TypeTags, sbe::value::Value> evalExpr(
        const ABT& tree,
        boost::optional<
            std::pair<ProjectionName, std::pair<sbe::value::TypeTags, sbe::value::Value>>> var) {
        auto env = VariableEnvironment::build(tree);

        SlotVarMap map;
        sbe::value::OwnedValueAccessor accessor;
        auto slotId = bindAccessor(&accessor);
        if (var) {
            auto& projName = var.get().first;
            map[projName] = slotId;

            auto [tag, val] = var.get().second;
            accessor.reset(tag, val);
        }

        sbe::InputParamToSlotMap inputParamToSlotMap;
        auto expr =
            SBEExpressionLowering{env, map, *runtimeEnv(), slotIdGenerator(), inputParamToSlotMap}
                .optimize(tree);

        auto compiledExpr = compileExpression(*expr);
        return runCompiledExpression(compiledExpr.get());
    }

    void assertEqualValues(std::pair<sbe::value::TypeTags, sbe::value::Value> res,
                           std::pair<sbe::value::TypeTags, sbe::value::Value> resConstFold) {
        auto [tag, val] = sbe::value::compareValue(
            res.first, res.second, resConstFold.first, resConstFold.second);
        ASSERT_EQ(tag, sbe::value::TypeTags::NumberInt32);
        ASSERT_EQ(val, 0);
    }
};

Constant* constEval(ABT& tree, const CollatorInterface* collator = nullptr) {
    ExpressionConstEval evaluator{collator};
    evaluator.optimize(tree);

    // The result must be Constant.
    Constant* result = tree.cast<Constant>();
    ASSERT(result != nullptr);

    ASSERT_NE(ABT::tagOf<Constant>(), ABT::tagOf<BinaryOp>());
    ASSERT_EQ(tree.tagOf(), ABT::tagOf<Constant>());
    return result;
}

TEST(SbeStageBuilderConstEvalTest, ConstEval) {
    // 1 + 2
    ABT tree = _binary("Add", "1"_cint64, "2"_cint64)._n;
    Constant* result = constEval(tree);
    ASSERT_EQ(result->getValueInt64(), 3);
}


TEST(SbeStageBuilderConstEvalTest, ConstEvalCompose) {
    // (1 + 2) + 3
    ABT tree = _binary("Add", _binary("Add", "1"_cint64, "2"_cint64), "3"_cint64)._n;
    Constant* result = constEval(tree);
    ASSERT_EQ(result->getValueInt64(), 6);
}


TEST(SbeStageBuilderConstEvalTest, ConstEvalCompose2) {
    // 3 - (5 - 4)
    auto tree = _binary("Sub", "3"_cint64, _binary("Sub", "5"_cint64, "4"_cint64))._n;
    Constant* result = constEval(tree);
    ASSERT_EQ(result->getValueInt64(), 2);
}

TEST(SbeStageBuilderConstEvalTest, ConstEval3) {
    // 1.5 + 0.5
    auto tree = _binary("Add", "1.5"_cdouble, "0.5"_cdouble)._n;
    Constant* result = constEval(tree);
    ASSERT_EQ(result->getValueDouble(), 2.0);
}

TEST(SbeStageBuilderConstEvalTest, ConstEval4) {
    // INT32_MAX (as int) + 0 (as double) => INT32_MAX (as double)
    auto tree =
        make<BinaryOp>(Operations::Add, Constant::int32(INT32_MAX), Constant::fromDouble(0));
    Constant* result = constEval(tree);
    ASSERT_EQ(result->getValueDouble(), INT32_MAX);
}

TEST(SbeStageBuilderConstEvalTest, ConstEval5) {
    // -1 + -2
    ABT tree1 = make<BinaryOp>(Operations::Add, Constant::int32(-1), Constant::int32(-2));
    ASSERT_EQ(constEval(tree1)->getValueInt32(), -3);
    // 1 + -1
    ABT tree2 = make<BinaryOp>(Operations::Add, Constant::int32(1), Constant::int32(-1));
    ASSERT_EQ(constEval(tree2)->getValueInt32(), 0);
    // 1 + INT32_MIN
    ABT tree3 = make<BinaryOp>(Operations::Add, Constant::int32(1), Constant::int32(INT32_MIN));
    ASSERT_EQ(constEval(tree3)->getValueInt32(), -2147483647);
}

TEST(SbeStageBuilderConstEvalTest, ConstEval6) {
    // -1 * -2
    ABT tree1 = make<BinaryOp>(Operations::Mult, Constant::int32(-1), Constant::int32(-2));
    ASSERT_EQ(constEval(tree1)->getValueInt32(), 2);
    // 1 * -1
    ABT tree2 = make<BinaryOp>(Operations::Mult, Constant::int32(1), Constant::int32(-1));
    ASSERT_EQ(constEval(tree2)->getValueInt32(), -1);
    // 2 * INT32_MAX
    ABT tree3 = make<BinaryOp>(Operations::Mult, Constant::int32(2), Constant::int32(INT32_MAX));
    ASSERT_EQ(constEval(tree3)->getValueInt64(), 4294967294);
}

TEST(SbeStageBuilderConstEvalTest, IntegerOverflow) {
    auto int32tree =
        make<BinaryOp>(Operations::Add, Constant::int32(INT32_MAX), Constant::int32(1));
    ASSERT_EQ(constEval(int32tree)->getValueInt64(), 2147483648);
}

TEST(SbeStageBuilderConstEvalTest, IntegerUnderflow) {
    auto int32tree =
        make<BinaryOp>(Operations::Add, Constant::int32(INT32_MIN), Constant::int32(-1));
    ASSERT_EQ(constEval(int32tree)->getValueInt64(), -2147483649);

    auto tree =
        make<BinaryOp>(Operations::Add, Constant::int32(INT32_MAX), Constant::int64(INT64_MIN));
    ASSERT_EQ(constEval(tree)->getValueInt64(), -9223372034707292161);
}

TEST(SbeStageBuilderConstEvalTest, ConstVariableInlining) {
    ABT tree = make<Let>("x",
                         Constant::int32(4),
                         make<BinaryOp>(Operations::Add, make<Variable>("x"), make<Variable>("x")));
    ASSERT_EQ(constEval(tree)->getValueInt32(), 8);
}

TEST(SbeStageBuilderConstEvalTest, IfSimplification) {
    ABT trueTree = make<If>(Constant::boolean(true), Constant::int32(1), Constant::int32(2));
    ABT falseTree = make<If>(Constant::boolean(false), Constant::int32(1), Constant::int32(2));
    ASSERT_EQ(constEval(trueTree)->getValueInt32(), 1);
    ASSERT_EQ(constEval(falseTree)->getValueInt32(), 2);
}

TEST(SbeStageBuilderConstEvalTest, EqualSimplification) {
    ABT falseTree = _binary("Eq", "A"_cstr, "B"_cstr)._n;
    ASSERT_FALSE(constEval(falseTree)->getValueBool());
    ABT trueTree = _binary("Eq", "A"_cstr, "A"_cstr)._n;
    ASSERT_TRUE(constEval(trueTree)->getValueBool());
}

TEST(SbeStageBuilderConstEvalTest, EqualSimplificationCollation) {
    const CollatorInterfaceMock collator{CollatorInterfaceMock::MockType::kToLowerString};
    ABT falseTree = _binary("Eq", "A"_cstr, "b"_cstr)._n;
    ASSERT_FALSE(constEval(falseTree, &collator)->getValueBool());
    ABT trueTree = _binary("Eq", "A"_cstr, "a"_cstr)._n;
    ASSERT_TRUE(constEval(trueTree, &collator)->getValueBool());
}

TEST(SbeStageBuilderConstEvalTest, CompareSimplification) {
    ABT tree = _binary("Lt", "A"_cstr, "B"_cstr)._n;
    ASSERT_TRUE(constEval(tree)->getValueBool());
}

TEST(SbeStageBuilderConstEvalTest, CompareSimplificationCollation) {
    const CollatorInterfaceMock collator{CollatorInterfaceMock::MockType::kToLowerString};
    ABT tree = _binary("Lt", "a"_cstr, "B"_cstr)._n;
    ASSERT_TRUE(constEval(tree, &collator)->getValueBool());
}

TEST(SbeStageBuilderConstEvalTest, ConstEvalNotNegate) {
    // !true = false
    ABT tree1 = make<UnaryOp>(Operations::Not, Constant::boolean(true));
    ASSERT_EQ(constEval(tree1)->getValueBool(), false);
    // !false = true
    ABT tree2 = make<UnaryOp>(Operations::Not, Constant::boolean(false));
    ASSERT_EQ(constEval(tree2)->getValueBool(), true);
}

TEST(ConstEvalTest, FoldRedundantExists) {
    ABT exists = make<FunctionCall>("exists", makeSeq(Constant::int32(1)));

    // Eliminates the exists call in favor of a boolean true.
    ASSERT_EQ(constEval(exists)->getValueBool(), true);
}

TEST(ConstEvalTest, AndOrFoldNonNothingLhs) {
    ExpressionConstEval evaluator{nullptr};

    /* OR */
    // nullable lhs (variable) || false -> lhs.
    ABT abt = _binary("Or", _binary("Lte", "x"_var, "2"_cint64), _cbool(false))._n;
    evaluator.optimize(abt);
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "BinaryOp [Lte]\n"
        "|   Const [2]\n"
        "Variable [x]\n",
        abt);

    // nullable lhs (variable) || true -> no change.
    abt = _binary("Or", _binary("Lte", "x"_var, "2"_cint64), _cbool(true))._n;
    evaluator.optimize(abt);
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "BinaryOp [Or]\n"
        "|   Const [true]\n"
        "BinaryOp [Lte]\n"
        "|   Const [2]\n"
        "Variable [x]\n",
        abt);

    // nothing lhs (const) || true -> no change.
    abt = _binary("Or", _cnothing(), _cbool(true))._n;
    evaluator.optimize(abt);
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "BinaryOp [Or]\n"
        "|   Const [true]\n"
        "Const [Nothing]\n",
        abt);

    /* AND */
    // nullable lhs (if) && true -> lhs.
    abt = _binary("And", _if("x"_var, "y"_var, "z"_var), _cbool(true))._n;
    evaluator.optimize(abt);
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "If []\n"
        "|   |   Variable [z]\n"
        "|   Variable [y]\n"
        "Variable [x]\n",
        abt);

    // nullable lhs (if) && false -> no change.
    abt = _binary("And", _if("x"_var, "y"_var, "z"_var), _cbool(false))._n;
    evaluator.optimize(abt);
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "BinaryOp [And]\n"
        "|   Const [false]\n"
        "If []\n"
        "|   |   Variable [z]\n"
        "|   Variable [y]\n"
        "Variable [x]\n",
        abt);
}

TEST(ConstEvalTest, ConstantEquality) {
    ExpressionConstEval evaluator{nullptr};
    auto tree = make<If>(make<BinaryOp>(Operations::Eq, Constant::int32(9), Constant::nothing()),
                         Constant::boolean(true),
                         Constant::boolean(false));
    evaluator.optimize(tree);
    ASSERT_TRUE(tree.is<Constant>());
}

TEST(ConstEvalTest, FoldIsInListForConstants) {
    ExpressionConstEval evaluator{nullptr};
    ABT abt = make<FunctionCall>("isInList", makeSeq(Constant::int32(1)));
    evaluator.optimize(abt);
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "Const [false]\n",
        abt);

    sbe::InList* inList = nullptr;
    abt = make<FunctionCall>(
        "isInList",
        makeSeq(make<Constant>(TypeTags::inList, sbe::value::bitcastFrom<sbe::InList*>(inList))));
    evaluator.optimize(abt);
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "Const [true]\n",
        abt);
}

TEST(Optimizer, ConstFoldIf) {
    ExpressionConstEval evaluator{nullptr};
    auto tree = _if("x"_var, _cbool(true), _cbool(false))._n;
    evaluator.optimize(tree);

    // Simplify "if (x) then true else false" -> x.
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "Variable [x]\n",
        tree);
}

TEST(Optimizer, ConstFoldIf1) {
    ExpressionConstEval evaluator{nullptr};
    auto tree = _if("x"_var, _cbool(false), _cbool(true))._n;
    evaluator.optimize(tree);

    // Simplify "if (x) then false else true" -> NOT x.
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "UnaryOp [Not]\n"
        "Variable [x]\n",
        tree);
}

TEST(Optimizer, ConstFoldIf2) {
    ExpressionConstEval evaluator{nullptr};
    auto tree = _if(_unary("Not", "x"_var), "y"_var, "z"_var)._n;
    evaluator.optimize(tree);

    // Simplify "if (not (x)) then y else z" -> "if (x) then z else y"
    ASSERT_EXPLAIN_V2_AUTO(  // NOLINT
        "If []\n"
        "|   |   Variable [y]\n"
        "|   Variable [z]\n"
        "Variable [x]\n",
        tree);
}

// The following nullability tests verify that ExpressionConstEval, which performs rewrites and
// simplifications based on the nullability value of expressions, does not change the result of
// the evaluation of And and Or. eval(E) == eval(ExpressionConstEval(E))

TEST_F(AbtToSbeExpression, NonNullableLhsOrTrueConstFold) {
    // E = non-nullable lhs (resolvable variable) || true
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("Or", _binary("Gt", "x"_var, "5"_cint32), _cbool(true))._n;
    auto treeConstFold = constFold(tree);

    auto var =
        std::make_pair(ProjectionName{"x"_sd}, sbe::value::makeValue(mongo::Value((int32_t)1)));

    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

TEST_F(AbtToSbeExpression, NonNullableLhsOrFalseConstFold) {
    // E = non-nullable lhs (resolvable variable) || false
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("Or", _binary("Gt", "x"_var, "5"_cint32), _cbool(false))._n;
    auto treeConstFold = constFold(tree);

    auto var =
        std::make_pair(ProjectionName{"x"_sd}, sbe::value::makeValue(mongo::Value((int32_t)1)));

    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

TEST_F(AbtToSbeExpression, NullableLhsOrTrueConstFold) {
    // E = nullable lhs (Nothing) || true
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("Or", _cnothing(), _cbool(true))._n;
    auto treeConstFold = constFold(tree);

    auto var = boost::none;
    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

TEST_F(AbtToSbeExpression, NullableLhsOrFalseConstFold) {
    // E = nullable lhs (Nothing) || false
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("Or", _cnothing(), _cbool(false))._n;
    auto treeConstFold = constFold(tree);

    auto var = boost::none;
    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

TEST_F(AbtToSbeExpression, NonNullableLhsAndFalseConstFold) {
    // E = non-nullable lhs (resolvable variable) && false
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("And", _binary("Gt", "x"_var, "5"_cint32), _cbool(false))._n;
    auto treeConstFold = constFold(tree);

    auto var =
        std::make_pair(ProjectionName{"x"_sd}, sbe::value::makeValue(mongo::Value((int32_t)1)));
    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

TEST_F(AbtToSbeExpression, NonNullableLhsAndTrueConstFold) {
    // E = non-nullable lhs (resolvable variable) && true
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("And", _binary("Gt", "x"_var, "5"_cint32), _cbool(true))._n;
    auto treeConstFold = constFold(tree);

    auto var =
        std::make_pair(ProjectionName{"x"_sd}, sbe::value::makeValue(mongo::Value((int32_t)1)));
    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

TEST_F(AbtToSbeExpression, NullableLhsAndFalseConstFold) {
    // E = nullable lhs (Nothing) && false
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("And", _cnothing(), _cbool(false))._n;
    auto treeConstFold = constFold(tree);

    auto var = boost::none;
    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

TEST_F(AbtToSbeExpression, NullableLhsAndTrueConstFold) {
    // E = nullable lhs (Nothing) && true
    // eval(E) == eval(ExpressionConstEval(E))
    auto tree = _binary("And", _cnothing(), _cbool(true))._n;
    auto treeConstFold = constFold(tree);

    auto var = boost::none;
    auto res = evalExpr(tree, var);
    auto resConstFold = evalExpr(treeConstFold, var);

    assertEqualValues(res, resConstFold);
}

}  // namespace
}  // namespace mongo::stage_builder
