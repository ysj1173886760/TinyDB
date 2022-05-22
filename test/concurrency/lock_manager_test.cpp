/**
 * @file lock_manager_test.cpp
 * @author sheep
 * @brief test for lock manager
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_context.h"
#include "concurrency/two_phase_locking.h"

#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <random>

namespace TinyDB {

TEST(LockManagerTest, BasicTest) {
    // scenario: two account A and B, with 200 total money
    // then we perform the reading and writing concurrently.
    auto lock_manager = std::make_unique<LockManager>(DeadLockResolveProtocol::DL_DETECT);
    // two account
    int a = 100;
    int b = 100;
    RID rid1(0, 0);
    RID rid2(0, 1);
    std::vector<std::thread> worker_list;
    int iteration_num = 100;
    // 8 writer
    for (int i = 0; i < 8; i++) {
        worker_list.push_back(std::thread([&](int i) {
            std::random_device rd;
            std::mt19937 mt(rd());
            std::uniform_int_distribution<int> dis(0, 1);
            for (int j = 0; j < iteration_num; j++) {
                txn_id_t txn_id = i * iteration_num + j;
                auto context = std::make_unique<TwoPLContext> (txn_id, IsolationLevel::SERIALIZABLE);
                EXPECT_TRUE(lock_manager->LockExclusive(context.get(), rid1).IsOk());
                EXPECT_TRUE(lock_manager->LockExclusive(context.get(), rid2).IsOk());
                int dir = dis(mt);
                if (dir == 0) {
                    a -= 1;
                    b += 1;
                } else {
                    a += 1;
                    b -= 1;
                }
                // release the lock
                EXPECT_TRUE(lock_manager->Unlock(context.get(), rid1).IsOk());
                EXPECT_TRUE(lock_manager->Unlock(context.get(), rid2).IsOk());
                EXPECT_TRUE(context->GetSharedLockSet()->empty());
                EXPECT_TRUE(context->GetExclusiveLockSet()->empty());
            }
        }, i));
    }
    // 2 reader
    for (int i = 8; i < 10; i++) {
        worker_list.push_back(std::thread([&](int i) {
            for (int j = 0; j < iteration_num; j++) {
                txn_id_t txn_id = i * iteration_num + j;
                auto context = std::make_unique<TwoPLContext> (txn_id, IsolationLevel::SERIALIZABLE);
                EXPECT_TRUE(lock_manager->LockShared(context.get(), rid1).IsOk());
                EXPECT_TRUE(lock_manager->LockShared(context.get(), rid2).IsOk());
                // we should read a consistent view
                EXPECT_EQ(a + b, 200);
                EXPECT_TRUE(lock_manager->Unlock(context.get(), rid1).IsOk());
                EXPECT_TRUE(lock_manager->Unlock(context.get(), rid2).IsOk());
                EXPECT_TRUE(context->GetSharedLockSet()->empty());
                EXPECT_TRUE(context->GetExclusiveLockSet()->empty());
            }
        }, i));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }
}
    
} // namespace TinyDB
