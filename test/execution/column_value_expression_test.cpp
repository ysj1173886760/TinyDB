/**
 * @file column_value_expression_test.cpp
 * @author sheep
 * @brief column value expression test
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/column_value_expression.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(ColumnValueExpressionTest, BasicTest) {
    auto colA = Column("colA", TypeId::TINYINT);
    auto colB = Column("colB", TypeId::INTEGER);
    auto colC = Column("colC", TypeId::BIGINT);
    auto colD = Column("colD", TypeId::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);

    auto schema = Schema(cols);

    auto column_value_expression1 = new ColumnValueExpression(TypeId::TINYINT, 0, 0, &schema);
    auto column_value_expression2 = new ColumnValueExpression(TypeId::INTEGER, 1, 1, &schema);

    // generate tuple
    Tuple tuple1, tuple2;
    auto valueA1 = Value(TypeId::TINYINT, static_cast<int8_t> (1));
    auto valueB1 = Value(TypeId::INTEGER, static_cast<int32_t> (20010310));
    auto valueC1 = Value(TypeId::BIGINT, static_cast<int64_t> (42));
    auto valueD1 = Value(TypeId::DECIMAL, 3.14159);
    tuple1 = Tuple({valueA1, valueB1, valueC1, valueD1}, &schema);

    auto valueA2 = Value(TypeId::TINYINT, static_cast<int8_t> (2));
    auto valueB2 = Value(TypeId::INTEGER, static_cast<int32_t> (19700101));
    auto valueC2 = Value(TypeId::BIGINT, static_cast<int64_t> (42));
    auto valueD2 = Value(TypeId::DECIMAL, 3.14159);
    tuple2 = Tuple({valueA2, valueB2, valueC2, valueD2}, &schema);

    EXPECT_EQ(column_value_expression1->Evaluate(&tuple1, &tuple2).CompareEquals(valueA1), CmpBool::CmpTrue);
    EXPECT_EQ(column_value_expression2->Evaluate(&tuple1, &tuple2).CompareEquals(valueB2), CmpBool::CmpTrue);

    delete column_value_expression1;
    delete column_value_expression2;
}

}