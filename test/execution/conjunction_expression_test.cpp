/**
 * @file conjunction_expression_test.cpp
 * @author sheep
 * @brief conjunction expression test
 * @version 0.1
 * @date 2022-05-27
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/conjunction_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(ConjunctionExpressionTest, BasicTest) {
    auto true1 = std::make_unique<ConstantValueExpression>(ValueFactory::GetBooleanValue(true));
    auto false1 = std::make_unique<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
    auto null1 = std::make_unique<ConstantValueExpression>(Type::Null(TypeId::BOOLEAN));
    auto conj_and = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_AND, true1.get(), false1.get());
    auto conj_and_null = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_AND, true1.get(), null1.get());
    auto conj_or = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_OR, true1.get(), false1.get());
    auto conj_or_null = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_OR, true1.get(), null1.get());
    EXPECT_TRUE(conj_and->Evaluate(nullptr, nullptr).IsFalse());
    EXPECT_TRUE(conj_and_null->Evaluate(nullptr, nullptr).IsNull());
    EXPECT_TRUE(conj_or->Evaluate(nullptr, nullptr).IsTrue());
    EXPECT_TRUE(conj_or_null->Evaluate(nullptr, nullptr).IsTrue());

}

}