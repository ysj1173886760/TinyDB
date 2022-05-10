/**
 * @file table_heap_test.cpp
 * @author sheep
 * @brief table heap test
 * @version 0.1
 * @date 2022-05-10
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/table/table_heap.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(TableHeapTest, BasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 3;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    auto colB = Column("colB", TypeId::VARCHAR, 20);
    auto colC = Column("colC", TypeId::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);

    auto valueA = Value(TypeId::BIGINT, static_cast<int64_t> (20010310));   
    auto valueB = Value(TypeId::VARCHAR, "hello world");                    
    auto valueC = Value(TypeId::DECIMAL, 3.14159);                          
    std::vector<Value> values{valueA, valueB, valueC};

    auto valueAA = Value(TypeId::BIGINT, static_cast<int64_t> (0504));   
    auto valueBB = Value(TypeId::VARCHAR, "hello tinydb");                    
    auto valueCC = Value(TypeId::DECIMAL, 0.618);                          
    std::vector<Value> values2{valueAA, valueBB, valueCC};

    auto tuple = Tuple(values, &schema);
    auto tuple_update = Tuple(values2, &schema);

    auto table = TableHeap::CreateNewTableHeap(bpm);

    int tuple_num = 1000;
    std::vector<RID> tuple_list(tuple_num);
    // insert many tuple
    for (int i = 0; i < tuple_num; i++) {
        EXPECT_EQ(table->InsertTuple(tuple, &tuple_list[i]), true);
    }
    // scan them
    for (int i = 0; i < tuple_num; i++) {
        auto tmp = Tuple();
        EXPECT_EQ(table->GetTuple(tuple_list[i], &tmp), true);
        EXPECT_EQ(tmp == tuple, true);
    }
    // update many tuple
    for (int i = 0; i < tuple_num; i++) {
        if (table->UpdateTuple(tuple_update, tuple_list[i]) == false) {
            // if inplace updation failed, then we perform the deletion/insertion
            table->ApplyDelete(tuple_list[i]);
            EXPECT_EQ(table->InsertTuple(tuple_update, &tuple_list[i]), true);
        }
    }
    // scan them
    for (int i = 0; i < tuple_num; i++) {
        auto tmp = Tuple();
        EXPECT_EQ(table->GetTuple(tuple_list[i], &tmp), true);
        EXPECT_EQ(tmp == tuple_update, true);
    }
    // delete many tuple
    for (int i = 0; i < tuple_num; i++) {
        table->ApplyDelete(tuple_list[i]);
    }
    // scan should fail
    for (int i = 0; i < tuple_num; i++) {
        auto tmp = Tuple();
        EXPECT_EQ(table->GetTuple(tuple_list[i], &tmp), false);
    }

    remove(filename.c_str());
    delete table;
    delete bpm;
    delete disk_manager;
}

}