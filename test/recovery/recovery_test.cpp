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
        
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }
        // scenario: insert many tuple then shutdown the database
        // after recovery, we should see all of insertions

        std::random_device rd;
        std::mt19937 mt(rd());
        // generate account num
        std::uniform_int_distribution<int> money_gen;
        int num_of_txn = 3;
        int op_per_txn = 10;
        for (int i = 0; i < num_of_txn; i++) {
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
            for (int j = 0; j < op_per_txn; j++) {
                auto ID = ValueFactory::GetIntegerValue(i * op_per_txn + j);
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

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
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }

        // perform recovery
        auto rm = new RecoveryManager(dm, bpm, lm);
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

        delete tm;
        delete bpm;
        delete lm;
        delete dm;
        delete rm;
    }

    remove("test.db");
    remove("test.log");
}

TEST(RecoveryTest, ConcurrentRedoTest) {
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
        
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }
        // scenario: insert many tuple then shutdown the database
        // after recovery, we should see all of insertions

        std::random_device rd;
        std::mt19937 mt(rd());
        // generate account num
        std::uniform_int_distribution<int> money_gen;
        int num_of_txn = 3;
        int num_of_worker = 8;
        int op_per_txn = 10;
        std::vector<std::thread> worker_list;
        std::mutex mu;

        for (int k = 0; k < num_of_worker; k++) {
            worker_list.push_back(std::thread([&](int k){
                for (int i = 0; i < num_of_txn; i++) {
                    auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
                    auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
                    for (int j = 0; j < op_per_txn; j++) {
                        auto ID = ValueFactory::GetIntegerValue(k * num_of_txn * op_per_txn + i * op_per_txn + j);
                        auto money = ValueFactory::GetIntegerValue(money_gen(mt));
                        auto tuple = Tuple({ID, money}, &schema);

                        mu.lock();
                        accounts[ID.GetAs<int>()] = money.GetAs<int>();
                        mu.unlock();
                        if (!PerformInsertion(&exec_context, tuple)) {
                            LOG_INFO("Txn %d aborted", txn_context->GetTxnId());
                            break;
                        }
                    }
                    if (!txn_context->IsAborted()) {
                        tm->Commit(txn_context);
                    }
                }
            }, k));
        }

        for (int i = 0; i < num_of_worker; i++) {
            worker_list[i].join();
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

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
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }

        // perform recovery
        auto rm = new RecoveryManager(dm, bpm, lm);
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

        delete tm;
        delete bpm;
        delete lm;
        delete dm;
        delete rm;
    }

    remove("test.db");
    remove("test.log");
}

TEST(RecoveryTest, AbortRedoTest) {
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
        
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }
        // scenario: insert many tuple then shutdown the database
        // after recovery, we should see all of insertions

        std::random_device rd;
        std::mt19937 mt(rd());
        // generate account num
        std::uniform_int_distribution<int> money_gen;
        std::uniform_int_distribution<int> abort(0, 9);
        int num_of_txn = 3;
        int num_of_worker = 8;
        int op_per_txn = 10;
        std::vector<std::thread> worker_list;
        std::mutex mu;

        for (int k = 0; k < num_of_worker; k++) {
            worker_list.push_back(std::thread([&](int k){
                for (int i = 0; i < num_of_txn; i++) {
                    auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
                    auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
                    auto txn_id = txn_context->GetTxnId();
                    std::vector<std::pair<int, int>> list;
                    for (int j = 0; j < op_per_txn; j++) {
                        auto ID = ValueFactory::GetIntegerValue(k * num_of_txn * op_per_txn + i * op_per_txn + j);
                        auto money = ValueFactory::GetIntegerValue(money_gen(mt));
                        auto tuple = Tuple({ID, money}, &schema);

                        list.push_back(std::make_pair(ID.GetAs<int>(), money.GetAs<int>()));
                        if (!PerformInsertion(&exec_context, tuple)) {
                            LOG_INFO("Txn %d aborted", txn_id);
                            break;
                        }
                    }

                    // abort manually
                    if (tm->IsTransactionAlive(txn_id) && abort(mt) == 0) {
                        tm->Abort(txn_context);
                    }

                    if (tm->IsTransactionAlive(txn_id)) {
                        mu.lock();
                        for (uint j = 0; j < list.size(); j++) {
                            accounts[list[j].first] = list[j].second;
                        }
                        mu.unlock();

                        tm->Commit(txn_context);
                    }
                }
            }, k));
        }

        for (int i = 0; i < num_of_worker; i++) {
            worker_list[i].join();
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

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
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }

        // perform recovery
        auto rm = new RecoveryManager(dm, bpm, lm);
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

        delete tm;
        delete bpm;
        delete lm;
        delete dm;
        delete rm;
    }

    remove("test.db");
    remove("test.log");
}

TEST(RecoveryTest, UndoTest) {
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
        
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }
        // scenario: insert many tuple then shutdown the database
        // after recovery, we should see all of insertions

        std::random_device rd;
        std::mt19937 mt(rd());
        // generate account num
        std::uniform_int_distribution<int> money_gen;
        std::uniform_int_distribution<int> abort(0, 9);
        int num_of_txn = 3;
        int num_of_worker = 8;
        int op_per_txn = 10;
        std::vector<std::thread> worker_list;
        std::mutex mu;

        for (int k = 0; k < num_of_worker; k++) {
            worker_list.push_back(std::thread([&](int k){
                for (int i = 0; i < num_of_txn; i++) {
                    auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
                    auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
                    auto txn_id = txn_context->GetTxnId();
                    std::vector<std::pair<int, int>> list;
                    for (int j = 0; j < op_per_txn; j++) {
                        auto ID = ValueFactory::GetIntegerValue(k * num_of_txn * op_per_txn + i * op_per_txn + j);
                        auto money = ValueFactory::GetIntegerValue(money_gen(mt));
                        auto tuple = Tuple({ID, money}, &schema);

                        list.push_back(std::make_pair(ID.GetAs<int>(), money.GetAs<int>()));
                        if (!PerformInsertion(&exec_context, tuple)) {
                            LOG_INFO("Txn %d aborted", txn_id);
                            break;
                        }
                    }

                    // abort manually
                    if (tm->IsTransactionAlive(txn_id) && abort(mt) == 0) {
                        tm->Abort(txn_context);
                    }

                    if (tm->IsTransactionAlive(txn_id)) {
                        mu.lock();
                        for (uint j = 0; j < list.size(); j++) {
                            accounts[list[j].first] = list[j].second;
                        }
                        mu.unlock();

                        tm->Commit(txn_context);
                    }
                }
            }, k));
        }

        for (int i = 0; i < num_of_worker; i++) {
            worker_list[i].join();
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

        for (int i = 0; i < num_of_txn; i++) {
            // begin an transaction and don't commit it
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            auto exec_context = ExecutionContext(&catalog, bpm, tm, txn_context);
            for (int j = 0; j < op_per_txn; j++) {
                auto ID = ValueFactory::GetIntegerValue(money_gen(mt));
                auto money = ValueFactory::GetIntegerValue(money_gen(mt));
                auto tuple = Tuple({ID, money}, &schema);

                if (!PerformInsertion(&exec_context, tuple)) {
                    THROW_UNKNOWN_TYPE_EXCEPTION("Unexpected expection");
                    break;
                }
            }
        }

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

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
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }

        // perform recovery
        auto rm = new RecoveryManager(dm, bpm, lm);
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

        delete tm;
        delete bpm;
        delete lm;
        delete dm;
        delete rm;
    }

    {
        // restart database again
        auto dm = new DiskManager("test.db");
        auto lm = new LogManager(dm);
        auto bpm = new BufferPoolManager(10, dm, lm);
        auto lock_manager = std::make_unique<LockManager>(DeadLockResolveProtocol::DL_DETECT);
        auto tm = new TwoPLManager(std::move(lock_manager), lm);
        
        // fake the new catalog, since we aren't logging metadata now
        auto catalog = Catalog(bpm, lm);
        {
            // use txn to create table
            auto txn_context = tm->Begin(IsolationLevel::SERIALIZABLE);
            catalog.CreateTable("table", schema, txn_context);
            tm->Commit(txn_context);
        }

        // perform recovery
        auto rm = new RecoveryManager(dm, bpm, lm);
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

        LOG_INFO("%s", tm->GetTimeConsumption().c_str());
        LOG_INFO("%s", bpm->GetTimeConsumption().c_str());
        LOG_INFO("%s", lm->GetTimeConsumption().c_str());
        LOG_INFO("%s", dm->GetTimeConsumption().c_str());

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