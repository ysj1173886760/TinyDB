/**
 * @file schema_test.cpp
 * @author schema test
 * @brief seriously, i don't know how to test schema. So i just tested column offset here
 * @version 0.1
 * @date 2022-05-07
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "catalog/schema.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(SchemaTest, BasicTest) {
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

    EXPECT_EQ(schema.GetColumnCount(), 4);
    EXPECT_EQ(schema.GetUninlinedColumnCount(), 0);
    EXPECT_EQ(schema.GetLength(), 21);

    auto idx = schema.GetColIdx("colA");
    EXPECT_EQ(idx, 0);
    auto c = schema.GetColumn(idx);
    EXPECT_EQ(c.GetOffset(), 0);
    EXPECT_EQ(c.GetType(), TypeId::TINYINT);

    idx = schema.GetColIdx("colB");
    EXPECT_EQ(idx, 1);
    c = schema.GetColumn(idx);
    EXPECT_EQ(c.GetOffset(), 1);
    EXPECT_EQ(c.GetType(), TypeId::INTEGER);

    idx = schema.GetColIdx("colC");
    EXPECT_EQ(idx, 2);
    c = schema.GetColumn(idx);
    EXPECT_EQ(c.GetOffset(), 5);
    EXPECT_EQ(c.GetType(), TypeId::BIGINT);

    idx = schema.GetColIdx("colD");
    EXPECT_EQ(idx, 3);
    c = schema.GetColumn(idx);
    EXPECT_EQ(c.GetOffset(), 13);
    EXPECT_EQ(c.GetType(), TypeId::DECIMAL);

    EXPECT_EQ(schema.IsInlined(), true);
}

TEST(SchemaTest, UninlinedTupleTest) {
    auto colA = Column("colA", TypeId::BIGINT);
    auto colB = Column("colB", TypeId::VARCHAR, 20);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);

    auto schema = Schema(cols);

    EXPECT_EQ(schema.GetColumnCount(), 2);
    EXPECT_EQ(schema.GetUninlinedColumnCount(), 1);
    EXPECT_EQ(schema.GetLength(), 20);

    auto idx = schema.GetColIdx("colA");
    EXPECT_EQ(idx, 0);
    auto c = schema.GetColumn(idx);
    EXPECT_EQ(c.GetOffset(), 0);
    EXPECT_EQ(c.GetType(), TypeId::BIGINT);

    idx = schema.GetColIdx("colB");
    EXPECT_EQ(idx, 1);
    c = schema.GetColumn(idx);
    EXPECT_EQ(c.GetOffset(), 8);
    EXPECT_EQ(c.GetType(), TypeId::VARCHAR);
    EXPECT_EQ(c.GetLength(), 20);

    EXPECT_EQ(schema.IsInlined(), false);

    auto uninlined_idx = schema.GetUninlinedColumns();
    EXPECT_EQ(uninlined_idx.size(), 1);
    EXPECT_EQ(uninlined_idx[0], 1);
}

}