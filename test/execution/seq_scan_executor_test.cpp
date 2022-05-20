/**
 * @file seq_scan_executor_test.cpp
 * @author sheep
 * @brief seq scan executor test
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/seq_scan_executor.h"
#include "storage/table/table_heap.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(SeqScanExecutorTest, BasicTest) {
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
    auto output_schema = Schema({colA, colB});

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
    auto catalog = Catalog(bpm);
    auto table_meta = catalog.CreateTable("table", schema);

    int tuple_num = 1000;
    // insert many tuple
    for (int i = 0; i < tuple_num; i++) {
        RID rid;
        if (i % 2 == 0) {
            EXPECT_EQ(table_meta->table_->InsertTuple(tuple, &rid), true);
        } else {
            EXPECT_EQ(table_meta->table_->InsertTuple(tuple_update, &rid), true);
        }
    }

    ExecutionContext context(&catalog, bpm);

    auto plan = new SeqScanPlan(&output_schema, nullptr, catalog.GetTable("table")->oid_);
    auto executor = new SeqScanExecutor(&context, plan);
    executor->Init();

    auto result1 = tuple.KeyFromTuple(&schema, &output_schema);
    auto result2 = tuple_update.KeyFromTuple(&schema, &output_schema);
    int cnt = 0;
    Tuple tmp;
    while (executor->Next(&tmp)) {
        if (cnt % 2 == 0) {
            EXPECT_EQ(result1, tmp);
        } else {
            EXPECT_EQ(result2, tmp);
        }
        cnt++;
    }
    EXPECT_EQ(cnt, tuple_num);

    delete plan;
    delete executor;
    delete bpm;
    delete disk_manager;
    remove(filename.c_str());
}

}