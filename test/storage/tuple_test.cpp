/**
 * @file tuple_test.cpp
 * @author tuple test
 * @brief this is important, since tuple is the basic store unit in our system
 * @version 0.1
 * @date 2022-05-07
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/table/tuple.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(TupleTest, BasicTest) {
    auto colA = Column("colA", TypeId::BIGINT);
    auto colB = Column("colB", TypeId::VARCHAR, 20);
    auto colC = Column("colC", TypeId::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);

    auto valueA = Value(TypeId::BIGINT, static_cast<int64_t> (20010310));   // 8
    auto valueB = Value(TypeId::VARCHAR, "hello world");                    // 15
    auto valueC = Value(TypeId::DECIMAL, 3.14159);                          // 8
    std::vector<Value> values{valueA, valueB, valueC};

    auto tuple = Tuple(values, &schema);

    // this test is testing the implementation, rather than the interface
    // we should remove tests like this in the future, since this will strict our extendibility
    // 8 + 4 + 8 + 15
    EXPECT_EQ(35, tuple.GetLength());

    EXPECT_EQ(tuple.GetValue(&schema, 0).CompareEquals(valueA), CmpBool::CmpTrue);
    EXPECT_EQ(tuple.GetValue(&schema, 1).CompareEquals(valueB), CmpBool::CmpTrue);
    EXPECT_EQ(tuple.GetValue(&schema, 2).CompareEquals(valueC), CmpBool::CmpTrue);

    char storage[40];
    tuple.SerializeToWithSize(storage);

    auto new_tuple = Tuple::DeserializeFromWithSize(storage);

    EXPECT_EQ(new_tuple.GetValue(&schema, 0).CompareEquals(valueA), CmpBool::CmpTrue);
    EXPECT_EQ(new_tuple.GetValue(&schema, 1).CompareEquals(valueB), CmpBool::CmpTrue);
    EXPECT_EQ(new_tuple.GetValue(&schema, 2).CompareEquals(valueC), CmpBool::CmpTrue);

}

}