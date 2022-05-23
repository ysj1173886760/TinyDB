/**
 * @file update_executor_test.cpp
 * @author sheep
 * @brief update executor test
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/operator_expression.h"
#include "storage/table/table_heap.h"
#include "common/logger.h"
#include "type/value_factory.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(UpdateExecutorTest, BasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 3;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    auto colB = Column("colB", TypeId::INTEGER);
    auto colC = Column("colC", TypeId::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);

    auto valueA = Value(TypeId::BIGINT, static_cast<int64_t> (20010310));   
    auto valueB = ValueFactory::GetIntegerValue(10);
    auto valueC = Value(TypeId::DECIMAL, 3.14159);                          

    auto tuple = Tuple({valueA, valueB, valueC}, &schema);
    auto catalog = Catalog(bpm);
    auto table_meta = catalog.CreateTable("table", schema);

    int tuple_num = 10;
    // insert tuple
    for (int i = 0; i < tuple_num; i++) {
        RID rid;
        EXPECT_EQ(table_meta->table_->InsertTuple(tuple, &rid).IsOk(), true);
    }

    // scenario: set colA = colA + 10, colB = 42

    ExecutionContext context(&catalog, bpm);

    {
        auto seq_plan = new SeqScanPlan(&schema, nullptr, catalog.GetTable("table")->oid_);
        auto seq_executor = new SeqScanExecutor(&context, seq_plan);

        auto getColA = new ColumnValueExpression(TypeId::BIGINT, 0, 0, &table_meta->schema_);
        auto const10 = new ConstantValueExpression(ValueFactory::GetIntegerValue(10));
        auto add = new OperatorExpression(ExpressionType::OperatorExpression_Add, getColA, const10);

        auto const42 = new ConstantValueExpression(ValueFactory::GetIntegerValue(42));

        auto update_plan = new UpdatePlan(seq_plan, catalog.GetTable("table")->oid_, {{add, 0}, {const42, 1}});
        auto update_executor = new UpdateExecutor(&context, update_plan, std::unique_ptr<AbstractExecutor>(seq_executor));
        update_executor->Init();

        int cnt = 0;
        while (update_executor->Next(nullptr)) {
            cnt++;
        }
        EXPECT_EQ(cnt, tuple_num);


        delete seq_plan;
        delete update_executor;
        delete update_plan;
        delete getColA;
        delete const10;
        delete add;
        delete const42;
    }

    {
        auto target_tuple = Tuple({ValueFactory::GetBigintValue(20010320), ValueFactory::GetIntegerValue(42), valueC}, &schema);

        auto seq_plan = new SeqScanPlan(&schema, nullptr, catalog.GetTable("table")->oid_);
        auto seq_executor = new SeqScanExecutor(&context, seq_plan);
        seq_executor->Init();

        int cnt = 0;
        Tuple tuple;
        while (seq_executor->Next(&tuple)) {
            EXPECT_EQ(tuple, target_tuple);
            cnt++;
        }
        EXPECT_EQ(cnt, tuple_num);

        delete seq_plan;
        delete seq_executor;
    }


    delete bpm;
    delete disk_manager;
    remove(filename.c_str());
}

}