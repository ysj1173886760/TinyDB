/**
 * @file insert_executor_test.cpp
 * @author sheep
 * @brief insert executor test
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/table_heap.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(InsertExecutorTest, BasicTest) {
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
    catalog.CreateTable("table", schema);

    std::vector<Tuple> tuple_list;
    int tuple_num = 1000;
    // insert many tuple
    for (int i = 0; i < tuple_num; i++) {
        if (i % 2 == 0) {
            tuple_list.push_back(tuple);
        } else {
            tuple_list.push_back(tuple_update);
        }
    }

    ExecutionContext context(&catalog, bpm);

    auto insert_plan = new InsertPlan(std::move(tuple_list), catalog.GetTable("table")->oid_);
    auto insert_executor = new InsertExecutor(&context, insert_plan, nullptr);
    insert_executor->Init();
    insert_executor->Next(nullptr);

    auto scan_plan = new SeqScanPlan(&output_schema, nullptr, catalog.GetTable("table")->oid_);
    auto scan_executor = new SeqScanExecutor(&context, scan_plan);
    scan_executor->Init();

    auto result1 = tuple.KeyFromTuple(&schema, &output_schema);
    auto result2 = tuple_update.KeyFromTuple(&schema, &output_schema);
    int cnt = 0;
    Tuple tmp;
    while (scan_executor->Next(&tmp)) {
        if (cnt % 2 == 0) {
            EXPECT_EQ(result1, tmp);
        } else {
            EXPECT_EQ(result2, tmp);
        }
        cnt++;
    }
    EXPECT_EQ(cnt, tuple_num);

    delete insert_plan;
    delete insert_executor;
    delete scan_plan;
    delete scan_executor;
    delete bpm;
    delete disk_manager;
    remove(filename.c_str());
}

TEST(InsertExecutorTest, BasicTest2) {
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
    catalog.CreateTable("table", schema);
    catalog.CreateTable("table2", schema);

    std::vector<Tuple> tuple_list;
    int tuple_num = 1000;
    // insert many tuple
    for (int i = 0; i < tuple_num; i++) {
        if (i % 2 == 0) {
            tuple_list.push_back(tuple);
        } else {
            tuple_list.push_back(tuple_update);
        }
    }

    ExecutionContext context(&catalog, bpm);

    {
        // perform insertion
        auto insert_plan = new InsertPlan(std::move(tuple_list), catalog.GetTable("table")->oid_);
        auto insert_executor = new InsertExecutor(&context, insert_plan, nullptr);
        insert_executor->Init();
        insert_executor->Next(nullptr);
        delete insert_plan;
        delete insert_executor;
    }

    {
        // scan table and insert into table2
        auto scan_plan = new SeqScanPlan(&schema, nullptr, catalog.GetTable("table")->oid_);
        auto scan_executor = new SeqScanExecutor(&context, scan_plan);
        auto insert_plan = new InsertPlan(scan_plan, catalog.GetTable("table2")->oid_);
        auto insert_executor = new InsertExecutor(&context, insert_plan, std::unique_ptr<AbstractExecutor>(scan_executor));
        insert_executor->Init();
        insert_executor->Next(nullptr);

        // we've moved ownership of scan_executor to insert_executor
        delete scan_plan;
        delete insert_plan;
        delete insert_executor;
    }

    {
        // scan table2
        auto scan_plan = new SeqScanPlan(&output_schema, nullptr, catalog.GetTable("table2")->oid_);
        auto scan_executor = new SeqScanExecutor(&context, scan_plan);
        scan_executor->Init();

        auto result1 = tuple.KeyFromTuple(&schema, &output_schema);
        auto result2 = tuple_update.KeyFromTuple(&schema, &output_schema);
        int cnt = 0;
        Tuple tmp;
        while (scan_executor->Next(&tmp)) {
            if (cnt % 2 == 0) {
                EXPECT_EQ(result1, tmp);
            } else {
                EXPECT_EQ(result2, tmp);
            }
            cnt++;
        }
        EXPECT_EQ(cnt, tuple_num);

        delete scan_plan;
        delete scan_executor;
    }

    delete bpm;
    delete disk_manager;
    remove(filename.c_str());
}

}