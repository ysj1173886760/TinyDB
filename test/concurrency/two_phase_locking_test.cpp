/**
 * @file two_phase_locking_test.cpp
 * @author sheep
 * @brief test for 2PL txn manager
 * @version 0.1
 * @date 2022-05-25
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/two_phase_locking.h"
#include "type/value_factory.h"
#include "execution/execution_engine.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/operator_expression.h"

#include <memory>
#include <gtest/gtest.h>
#include <thread>
#include <random>

namespace TinyDB {

bool PerformWrite(ExecutionContext *context, int from, int to, int money) {
    ExecutionEngine engine;
    auto table = context->GetCatalog()->GetTable("table");
    auto txn_id = context->GetTransactionContext()->GetTxnId();
    std::vector<Tuple> result_set;
    {
        auto getID = std::make_unique<ColumnValueExpression>(TypeId::INTEGER, 0, 0, &table->schema_);
        auto constval = std::make_unique<ConstantValueExpression>(ValueFactory::GetIntegerValue(from));
        auto equal = std::make_unique<ComparisonExpression>(ExpressionType::ComparisonExpression_Equal, getID.get(), constval.get());
        // find the tuple with id = from
        auto scan_plan = std::make_unique<SeqScanPlan>(&table->schema_, equal.get(), context->GetCatalog()->GetTable("table")->oid_);
        // then perform update
        auto getMoney = std::make_unique<ColumnValueExpression>(TypeId::INTEGER, 0, 1, &table->schema_);
        auto const_money = std::make_unique<ConstantValueExpression>(ValueFactory::GetIntegerValue(money));
        auto subtract = std::make_unique<OperatorExpression>(ExpressionType::OperatorExpression_Subtract, getMoney.get(), const_money.get());
        auto update_plan = std::make_unique<UpdatePlan>(scan_plan.get(), table->oid_, std::vector<UpdateInfo>{UpdateInfo(subtract.get(), 1)});
        engine.Execute(context, update_plan.get(), &result_set);
    }

    if (!context->GetTransactionManager()->IsTransactionAlive(txn_id)) {
        return false;
    }
    {
        auto getID = std::make_unique<ColumnValueExpression>(TypeId::INTEGER, 0, 0, &table->schema_);
        auto constval = std::make_unique<ConstantValueExpression>(ValueFactory::GetIntegerValue(to));
        auto equal = std::make_unique<ComparisonExpression>(ExpressionType::ComparisonExpression_Equal, getID.get(), constval.get());
        // find the tuple with id = to
        auto scan_plan = std::make_unique<SeqScanPlan>(&table->schema_, equal.get(), context->GetCatalog()->GetTable("table")->oid_);
        // then perform update
        auto getMoney = std::make_unique<ColumnValueExpression>(TypeId::INTEGER, 0, 1, &table->schema_);
        auto const_money = std::make_unique<ConstantValueExpression>(ValueFactory::GetIntegerValue(money));
        auto add = std::make_unique<OperatorExpression>(ExpressionType::OperatorExpression_Add, getMoney.get(), const_money.get());
        auto update_plan = std::make_unique<UpdatePlan>(scan_plan.get(), table->oid_, std::vector<UpdateInfo>{UpdateInfo(add.get(), 1)});
        engine.Execute(context, update_plan.get(), &result_set);
    }
    if (!context->GetTransactionManager()->IsTransactionAlive(txn_id)) {
        return false;
    }
    // commit txn
    context->GetTransactionManager()->Commit(context->GetTransactionContext());
    return true;
}

TEST(TwoPhaseLockingTest, BasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 3;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("ID", TypeId::INTEGER);
    auto colC = Column("Money", TypeId::INTEGER);
    auto schema = Schema({colA, colC});
    auto catalog = Catalog(bpm);
    catalog.CreateTable("table", schema);
    int total = 0;
    int account_num = 100;
    auto lock_manager = std::make_unique<LockManager>(DeadLockResolveProtocol::DL_DETECT);
    auto txn_manager = new TwoPLManager(std::move(lock_manager));
    {
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dis(100, 1000);

        auto table = catalog.GetTable("table");
        // insert "account_num" accounts
        RID rid;
        for (int i = 0; i < account_num; i++) {
            int cur = dis(mt);
            total += cur;
            table->table_->InsertTuple(
                Tuple(
                    {
                        ValueFactory::GetIntegerValue(i),
                        ValueFactory::GetIntegerValue(cur)
                    },
                    &table->schema_),
                &rid);
        }
    }

    std::vector<std::thread> worker_list;
    int iteration_num = 10;
    // 8 writer
    for (int i = 0; i < 8; i++) {
        worker_list.push_back(std::thread([&](int i) {
            std::random_device rd;
            std::mt19937 mt(rd());
            // generate account num
            std::uniform_int_distribution<int> account_gen(0, account_num);
            // generate money num
            std::uniform_int_distribution<int> money_gen(1, 10);

            for (int j = 0; j < iteration_num; j++) {
                // begin a transaction
                auto txn_context = txn_manager->Begin(IsolationLevel::SERIALIZABLE);
                auto context = ExecutionContext(&catalog, bpm, txn_manager, txn_context);
                int from = account_gen(mt);
                int to = account_gen(mt);
                int money = money_gen(mt);
                PerformWrite(&context, from, to, money);
            }
        }, i));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    {
        auto context = ExecutionContext(&catalog, bpm);
        auto table = catalog.GetTable("table");
        auto scan_plan = std::make_unique<SeqScanPlan>(&table->schema_, nullptr, table->oid_);
        ExecutionEngine engine;
        std::vector<Tuple> result_list;
        engine.Execute(&context, scan_plan.get(), &result_list);

        int cnt = 0;
        for (uint i = 0; i < result_list.size(); i++) {
            cnt += result_list[i].GetValue(&table->schema_, 1).GetAs<int>();
        }
        EXPECT_EQ(cnt, total);
    }

    remove(filename.c_str());
    delete txn_manager;
    delete disk_manager;
    delete bpm;
}

}