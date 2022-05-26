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

#include <gtest/gtest.h>
#include <thread>
#include <random>

namespace TinyDB {

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

    {
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dis(100, 1000);

        auto table = catalog.GetTable("table");
        // insert "account_num" accounts
        int account_num = 100;
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
            std::uniform_int_distribution<int> dis(0, 1);
            for (int j = 0; j < iteration_num; j++) {
            }
        }, i));
    }
    // 2 reader
    for (int i = 8; i < 10; i++) {
        worker_list.push_back(std::thread([&](int i) {
            for (int j = 0; j < iteration_num; j++) {
            }
        }, i));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    remove(filename.c_str());
    delete disk_manager;
    delete bpm;
}

}