/**
 * @file constant_value_expression_test.cpp
 * @author sheep
 * @brief constant value expression test
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/constant_value_expression.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(ConstantValueExpression, BasicTest) {
    auto valueA1 = Value(TypeId::TINYINT, static_cast<int8_t> (1));
    auto constant_value_expression = new ConstantValueExpression(valueA1);
    EXPECT_EQ(constant_value_expression->Evaluate(nullptr, nullptr).CompareEquals(valueA1), CmpBool::CmpTrue);
    delete constant_value_expression;
}

}