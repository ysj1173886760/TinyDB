/**
 * @file recovery_test.cpp
 * @author sheep
 * @brief recovery test
 * @version 0.1
 * @date 2022-06-02
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "recovery/recovery_manager.h"
#include "recovery/log_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "catalog/catalog.h"
#include "concurrency/two_phase_locking.h"
#include "execution/execution_context.h"
#include "execution/execution_engine.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "type/value_factory.h"

#include <gtest/gtest.h>
#include <random>

namespace TinyDB {

bool PerformInsertion(ExecutionContext *context, const Tuple &tuple) {
    ExecutionEngine engine;
    auto table = context->GetCatalog()->GetTable("table");
    std::vector<Tuple> result_set;
    {
        std::vector<Tuple> tuples{tuple};
        auto insert_plan = std::make_unique<InsertPlan>(std::move(tuples), table->oid_);
        engine.Execute(context, insert_plan.get(), &result_set);
    }
    return !context->GetTransactionContext()->IsAborted();
}

TEST(RecoveryTest, RedoTest) {
    remove("test.db");
    remove("test.log");

    auto colA = Column("ID", TypeId::INTEGER);
    auto colC = Column("Money", TypeId::INTEGER);
    auto schema = Schema({colA, colC});
    
    LOG_TIMEOUT = std::chrono::milliseconds(300);

    // id -> money
    std::unordered_map<int, int> accounts;
    {
        auto dm = new DiskManager("test.db");
        auto lm = new LogManager(dm);
        auto bpm = new BufferPoolManager(10, dm, lm);
        auto lock_manager = std::make_unique<LockManager>(DeadLockResolveProtocol::DL_DETECT);
        auto tm = new TwoPLManager(std::move(lock_manager), lm);
        
        auto catalog = Catalog(bpm);
        catalog.CreateTable("table", schema);
        // scenario: insert many tuple then shutdown the database
        // after recovery, we should see all of insertions

        std::random_device rd;
        std::mt19937 mt(rd());
        // generate account num
        std::uniform_int_distribution<int> money_gen;
        int num_of_txn = 1;
        int op_per_txn = 10;
        for (int i = 0; i < num_of_txn; i++) {
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
            for (int j = 0; j < op_per_txn; j++) {
                auto ID = ValueFactory::GetIntegerValue(i * num_of_txn + j);
                auto money = ValueFactory::GetIntegerValue(money_gen(mt));
                auto tuple = Tuple({ID, money}, &schema);
                accounts[ID.GetAs<int>()] = money.GetAs<int>();
                if (!PerformInsertion(&exec_context, tuple)) {
                    tm->Abort(txn_context);
                    LOG_INFO("Txn %d aborted", txn_context->GetTxnId());
                    break;
                }
            }
            if (!txn_context->IsAborted()) {
                tm->Commit(txn_context);
            }
        }

        {
            std::vector<Tuple> result_set;
            {
                auto scan_plan = std::make_unique<SeqScanPlan>(&schema, nullptr, catalog.GetTable("table")->oid_);
                auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
                auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
                ExecutionEngine engine;
                engine.Execute(&exec_context, scan_plan.get(), &result_set);
                tm->Commit(txn_context);
            }
            EXPECT_EQ(result_set.size(), accounts.size());
            for (uint i = 0; i < result_set.size(); i++) {
                auto id = result_set[i].GetValue(&schema, 0).GetAs<int>();
                auto money = result_set[i].GetValue(&schema, 1).GetAs<int>();
                EXPECT_EQ(money, accounts[id]);
            }
        }

        delete tm;
        delete bpm;
        delete lm;
        delete dm;
    }

    {
        // restart database
        auto dm = new DiskManager("test.db");
        auto lm = new LogManager(dm);
        auto bpm = new BufferPoolManager(10, dm, lm);
        auto lock_manager = std::make_unique<LockManager>(DeadLockResolveProtocol::DL_DETECT);
        auto tm = new TwoPLManager(std::move(lock_manager), lm);
        
        // fake the new catalog, since we aren't logging metadata now
        auto catalog = Catalog(bpm);
        catalog.CreateTable("table", schema);

        // perform recovery
        auto rm = new RecoveryManager(dm, bpm);
        rm->ARIES();

        // after recovery, try to scan the tuple
        std::vector<Tuple> result_set;
        {
            auto scan_plan = std::make_unique<SeqScanPlan>(&schema, nullptr, catalog.GetTable("table")->oid_);
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
            ExecutionEngine engine;
            engine.Execute(&exec_context, scan_plan.get(), &result_set);
            tm->Commit(txn_context);
        }
        EXPECT_EQ(result_set.size(), accounts.size());
        for (uint i = 0; i < result_set.size(); i++) {
            auto id = result_set[i].GetValue(&schema, 0).GetAs<int>();
            auto money = result_set[i].GetValue(&schema, 1).GetAs<int>();
            EXPECT_EQ(money, accounts[id]);
        }

        delete tm;
        delete bpm;
        delete lm;
        delete dm;
        delete rm;
    }

    remove("test.db");
    remove("test.log");
}

}