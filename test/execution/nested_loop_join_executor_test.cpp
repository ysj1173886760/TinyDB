/**
 * @file nested_loop_join_executor_test.cpp
 * @author sheep
 * @brief test for nested loop join executor
 * @version 0.1
 * @date 2022-05-21
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/operator_expression.h"
#include "execution/executor_factory.h"
#include "execution/execution_engine.h"
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
    auto catalog = Catalog(bpm);
    {
        auto colA = Column("colA", TypeId::BIGINT);
        auto colB = Column("colB", TypeId::INTEGER);
        auto colC = Column("colC", TypeId::DECIMAL);
        auto schema = Schema({colA, colB, colC});
        auto table_meta = catalog.CreateTable("table1", schema);

        // insert 5 tuple
        // sheep: i'm not sure how to write styled code in such cases.
        RID tmp;
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetBigintValue(2001311),
                    ValueFactory::GetIntegerValue(1),
                    ValueFactory::GetDecimalValue(3.14),
                },
                &schema), &tmp);
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetBigintValue(2001312),
                    ValueFactory::GetIntegerValue(2),
                    ValueFactory::GetDecimalValue(3.14),
                },
                &schema), &tmp);
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetBigintValue(2001313),
                    ValueFactory::GetIntegerValue(3),
                    ValueFactory::GetDecimalValue(3.14),
                },
                &schema), &tmp);
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetBigintValue(2001314),
                    ValueFactory::GetIntegerValue(4),
                    ValueFactory::GetDecimalValue(3.14),
                },
                &schema), &tmp);
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetBigintValue(2001315),
                    ValueFactory::GetIntegerValue(5),
                    ValueFactory::GetDecimalValue(3.14),
                },
                &schema), &tmp);
    }
    {
        auto colA = Column("colA", TypeId::INTEGER);
        auto colB = Column("colB", TypeId::TINYINT);
        auto schema = Schema({colA, colB});
        auto table_meta = catalog.CreateTable("table2", schema);

        // insert 3 tuple
        RID tmp;
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetIntegerValue(2),
                    ValueFactory::GetTinyintValue(1),
                },
                &schema), &tmp);
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetIntegerValue(3),
                    ValueFactory::GetTinyintValue(-1),
                },
                &schema), &tmp);
        table_meta->table_->InsertTuple(
            Tuple({ ValueFactory::GetIntegerValue(6),
                    ValueFactory::GetTinyintValue(1),
                },
                &schema), &tmp);
    }

    ExecutionContext context(&catalog, bpm);

    {
        auto seq_plan_t1 = new SeqScanPlan(&catalog.GetTable("table1")->schema_, nullptr, catalog.GetTable("table1")->oid_);
        auto seq_plan_t2 = new SeqScanPlan(&catalog.GetTable("table2")->schema_, nullptr, catalog.GetTable("table2")->oid_);

        // expressions used to generate new tuple
        auto getCol1 = new ColumnValueExpression(TypeId::BIGINT, 0, 0, &catalog.GetTable("table1")->schema_);
        auto getCol2 = new ColumnValueExpression(TypeId::INTEGER, 0, 1, &catalog.GetTable("table1")->schema_);
        auto getCol3 = new ColumnValueExpression(TypeId::INTEGER, 1, 0, &catalog.GetTable("table2")->schema_);
        auto getCol4 = new ColumnValueExpression(TypeId::TINYINT, 1, 1, &catalog.GetTable("table2")->schema_);

        // expression used to evaluate the legality
        // since expression is stateless, so we can reuse the expression
        auto comp = new ComparisonExpression(ExpressionType::ComparisonExpression_Equal, getCol2, getCol3);

        auto colA = Column("colA", TypeId::BIGINT);
        auto colB = Column("colB", TypeId::INTEGER);
        auto colC = Column("colC", TypeId::TINYINT);
        auto output_schema = Schema({colA, colB, colC});
        auto join_plan = new NestedLoopJoinPlan(&output_schema, {seq_plan_t1, seq_plan_t2}, comp, {getCol1, getCol2, getCol4});

        std::vector<Tuple> result;
        ExecutionEngine engine;
        engine.Execute(&context, join_plan, &result);

        // result tuples
        std::vector<Tuple> result_expected;
        result_expected.push_back(
            Tuple({ ValueFactory::GetBigintValue(2001312),
                    ValueFactory::GetIntegerValue(2),
                    ValueFactory::GetTinyintValue(1), }, &output_schema));

        result_expected.push_back(
            Tuple({ ValueFactory::GetBigintValue(2001313),
                    ValueFactory::GetIntegerValue(3),
                    ValueFactory::GetTinyintValue(-1), }, &output_schema));
        
        EXPECT_EQ(result.size(), result_expected.size());
        for (uint i = 0; i < result.size(); i++) {
            EXPECT_EQ(result[i], result_expected[i]);
            // LOG_DEBUG("expect: %s get: %s", result_expected[i].ToString(&output_schema).c_str(), 
            //     result[i].ToString(&output_schema).c_str());
        }

        delete seq_plan_t1;
        delete seq_plan_t2;
        delete join_plan;
        delete getCol1;
        delete getCol2;
        delete getCol3;
        delete getCol4;
        delete comp;
    }


    delete bpm;
    delete disk_manager;
    remove(filename.c_str());
}

}