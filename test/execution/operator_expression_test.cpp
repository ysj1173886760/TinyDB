/**
 * @file operator_expression_test.cpp
 * @author sheep
 * @brief operator expression test
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/operator_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(OperatorExpressionTest, BasicTest) {
    auto colA = Column("colA", TypeId::TINYINT);
    auto colB = Column("colB", TypeId::INTEGER);
    auto colC = Column("colC", TypeId::BIGINT);
    auto colD = Column("colD", TypeId::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema_left = Schema(cols);

    auto colA2 = Column("colA", TypeId::BIGINT);
    auto colB2 = Column("colB", TypeId::DECIMAL);
    auto schema_right = Schema({colA2, colB2});

    auto column_value_expression1 = new ColumnValueExpression(TypeId::BIGINT, 0, 2, &schema_left);
    auto column_value_expression2 = new ColumnValueExpression(TypeId::BIGINT, 1, 0, &schema_right);
    auto column_value_expression3 = new ColumnValueExpression(TypeId::TINYINT, 0, 0, &schema_left);

    // generate tuple
    Tuple tuple1, tuple2, tuple3;
    auto valueA1 = Value(TypeId::TINYINT, static_cast<int8_t> (1));
    auto valueB1 = Value(TypeId::INTEGER, static_cast<int32_t> (20010310));
    auto valueC1 = Value(TypeId::BIGINT, static_cast<int64_t> (24));
    auto valueD1 = Value(TypeId::DECIMAL, 3.14159);
    tuple1 = Tuple({valueA1, valueB1, valueC1, valueD1}, &schema_left);

    auto valueA2 = Value(TypeId::BIGINT, static_cast<int64_t> (42));
    auto valueB2 = Value(TypeId::DECIMAL, 3.14159);
    tuple2 = Tuple({valueA2, valueB2}, &schema_right);

    tuple3 = Tuple({valueA1, valueB1, valueA2, valueD1}, &schema_left);

    auto operator_expression1 = new OperatorExpression(
        ExpressionType::OperatorExpression_Add, column_value_expression1, column_value_expression2);
    EXPECT_EQ(operator_expression1->GetReturnType(), TypeId::BIGINT);
    EXPECT_EQ(operator_expression1->Evaluate(&tuple1, &tuple2), ValueFactory::GetBigintValue(66));

    auto operator_expression2 = new OperatorExpression(
        ExpressionType::OperatorExpression_Subtract, column_value_expression3, column_value_expression2);
    EXPECT_EQ(operator_expression2->GetReturnType(), TypeId::BIGINT);
    EXPECT_EQ(operator_expression2->Evaluate(&tuple1, &tuple2), ValueFactory::GetBigintValue(-41));

    delete column_value_expression1;
    delete column_value_expression2;
    delete column_value_expression3;
    delete operator_expression1;
    delete operator_expression2;
}

}