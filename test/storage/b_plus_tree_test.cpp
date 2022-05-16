/**
 * @file b_plus_tree_test.cpp
 * @author sheep
 * @brief B+tree test
 * @version 0.1
 * @date 2022-05-15
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/index/b_plus_tree.h"
#include "storage/index/generic_key.h"

#include <thread>
#include <gtest/gtest.h>
#include <random>


namespace TinyDB {

TEST(BPlusTreeTest, SequentialInsertTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<8> comparator(&schema);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("basic_test", bpm, comparator);
    GenericKey<8> index_key;
    RID rid;
    BPlusTreeExecutionContext context;

    int key_num = 10000;
    std::vector<int64_t> keys(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
    }
    // insert  keys
    for (auto key : keys) {
        context.Reset();
        int64_t value = key;
        rid = RID(value);
        auto tmp = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(tmp);
        EXPECT_EQ(tree.Insert(index_key, rid, &context), true);
    }
    
    // read them
    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
        EXPECT_EQ(result[0], RID(key));
    }

    // delete them
    for (uint i = 0; i < keys.size(); i++) {
        context.Reset();
        auto k = Tuple({Value(TypeId::BIGINT, keys[i])}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.Remove(index_key, &context), true);
    }

    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), false);
    }

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

TEST(BPlusTreeTest, RandomInsertTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<8> comparator(&schema);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("basic_test", bpm, comparator);
    GenericKey<8> index_key;
    RID rid;
    BPlusTreeExecutionContext context;

    int key_num = 10000;
    std::vector<int64_t> keys(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // insert  keys
    for (auto key : keys) {
        context.Reset();
        int64_t value = key;
        rid = RID(value);
        auto tmp = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(tmp);
        EXPECT_EQ(tree.Insert(index_key, rid, &context), true);
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // read them
    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
        EXPECT_EQ(result[0], RID(key));
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // delete them
    for (uint i = 0; i < keys.size(); i++) {
        context.Reset();
        auto k = Tuple({Value(TypeId::BIGINT, keys[i])}, &schema);
        index_key.SetFromKey(k);

        EXPECT_EQ(tree.Remove(index_key, &context), true);
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), false);
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // check whether we've returned the page back to disk
    EXPECT_EQ(disk_manager->GetAllocateCount(), disk_manager->GetDeallocateCount());

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

TEST(BPlusTreeTest, ConcurrentBasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<8> comparator(&schema);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("basic_test", bpm, comparator);

    int key_num = 1000;
    int worker_num = 5;
    std::vector<int64_t> keys(key_num * worker_num);
    for (int i = 0; i < key_num * worker_num; i++) {
        keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    std::vector<std::thread> worker_list;

    EXPECT_EQ(bpm->CheckPinCount(), true);

    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            BPlusTreeExecutionContext context;
            GenericKey<8> index_key;
            for (int j = begin; j < end; j++) {
                context.Reset();
                auto rid = RID(keys[j]);
                auto tmp = Tuple({Value(TypeId::BIGINT, keys[j])}, &schema);
                index_key.SetFromKey(tmp);
                EXPECT_EQ(tree.Insert(index_key, rid, &context), true);

                // could we read what we just write?
                std::vector<RID> result;
                EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
                EXPECT_EQ(result[0], RID(keys[j]));
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);
    
    // read them
    for (auto key : keys) {
        BPlusTreeExecutionContext context;
        std::vector<RID> result;
        GenericKey<8> index_key;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
        EXPECT_EQ(result[0], RID(key));
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // delete them
    worker_list.clear();
    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            BPlusTreeExecutionContext context;
            GenericKey<8> index_key;
            for (int j = begin; j < end; j++) {
                context.Reset();
                auto k = Tuple({Value(TypeId::BIGINT, keys[j])}, &schema);
                index_key.SetFromKey(k);
                EXPECT_EQ(tree.Remove(index_key, &context), true);
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    for (auto key : keys) {
        BPlusTreeExecutionContext context;
        GenericKey<8> index_key;
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), false);
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // check whether we've returned the page back to disk
    EXPECT_EQ(disk_manager->GetAllocateCount(), disk_manager->GetDeallocateCount());

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

TEST(BPlusTreeTest, ConcurrentStrictTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<8> comparator(&schema);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("basic_test", bpm, comparator);

    int key_num = 2000;
    int worker_num = 8;
    std::vector<int64_t> keys(key_num * worker_num);
    for (int i = 0; i < key_num * worker_num; i++) {
        keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    std::vector<std::thread> worker_list;
    std::vector<std::thread> daemon_list;

    std::atomic_bool shutdown(false);
    // there will be a worker keep on reading
    daemon_list.emplace_back(std::thread([&](){
        BPlusTreeExecutionContext context;
        GenericKey<8> index_key;
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dis(0, key_num * worker_num - 1);
        while (shutdown.load() == false) {
            context.Reset();
            int idx = dis(mt);
            std::vector<RID> result;
            auto tmp = Tuple({Value(TypeId::BIGINT, keys[idx])}, &schema);
            index_key.SetFromKey(tmp);
            tree.GetValue(index_key, &result, &context);
        }
    }));

    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            BPlusTreeExecutionContext context;
            GenericKey<8> index_key;
            for (int j = begin; j < end; j++) {
                context.Reset();
                auto rid = RID(keys[j]);
                auto tmp = Tuple({Value(TypeId::BIGINT, keys[j])}, &schema);
                index_key.SetFromKey(tmp);
                EXPECT_EQ(tree.Insert(index_key, rid, &context), true);

                // could we read what we just write?
                std::vector<RID> result;
                EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
                EXPECT_EQ(result[0], RID(keys[j]));
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }
    
    // read them
    for (auto key : keys) {
        BPlusTreeExecutionContext context;
        std::vector<RID> result;
        GenericKey<8> index_key;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
        EXPECT_EQ(result[0], RID(key));
    }

    // delete them
    worker_list.clear();
    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            BPlusTreeExecutionContext context;
            GenericKey<8> index_key;
            for (int j = begin; j < end; j++) {
                context.Reset();
                auto k = Tuple({Value(TypeId::BIGINT, keys[j])}, &schema);
                index_key.SetFromKey(k);
                EXPECT_EQ(tree.Remove(index_key, &context), true);

                // we shouldn't see what we just deleted
                std::vector<RID> result;
                EXPECT_EQ(tree.GetValue(index_key, &result, &context), false);
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    for (auto key : keys) {
        BPlusTreeExecutionContext context;
        GenericKey<8> index_key;
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), false);
    }

    shutdown.store(true);
    for (uint i = 0; i < daemon_list.size(); i++) {
        daemon_list[i].join();
    }

    // check whether we've returned the page back to disk
    EXPECT_EQ(disk_manager->GetAllocateCount(), disk_manager->GetDeallocateCount());

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

TEST(BPlusTreeTest, BasicIteratorTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<8> comparator(&schema);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("basic_test", bpm, comparator);
    GenericKey<8> index_key;
    RID rid;
    BPlusTreeExecutionContext context;

    int key_num = 10000;
    std::vector<int64_t> keys(key_num);
    std::vector<int64_t> keys_sorted(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
        keys_sorted[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // insert  keys
    for (auto key : keys) {
        context.Reset();
        int64_t value = key;
        rid = RID(value);
        auto tmp = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(tmp);
        EXPECT_EQ(tree.Insert(index_key, rid, &context), true);
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // read them by iterator
    int ptr = 0;
    for (auto it = tree.Begin(); !it->IsEnd(); it->Advance()) {
        EXPECT_EQ(it->Get(), RID(keys_sorted[ptr]));
        ptr++;
    }
    EXPECT_EQ(ptr, key_num);

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // delete them
    for (uint i = 0; i < keys.size(); i++) {
        context.Reset();
        auto k = Tuple({Value(TypeId::BIGINT, keys[i])}, &schema);
        index_key.SetFromKey(k);

        EXPECT_EQ(tree.Remove(index_key, &context), true);
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // read them by iterator
    ptr = 0;
    for (auto it = tree.Begin(); !it->IsEnd(); it->Advance()) {
        EXPECT_EQ(it->Get(), RID(keys_sorted[ptr]));
        ptr++;
    }
    EXPECT_EQ(ptr, 0);

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // check whether we've returned the page back to disk
    EXPECT_EQ(disk_manager->GetAllocateCount(), disk_manager->GetDeallocateCount());

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

TEST(BPlusTreeTest, ConcurrentIteratorTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 100;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<8> comparator(&schema);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("basic_test", bpm, comparator);

    int key_num = 2000;
    int worker_num = 8;
    std::vector<int64_t> keys(key_num * worker_num);
    std::vector<int64_t> keys_sorted(key_num * worker_num);
    for (int i = 0; i < key_num * worker_num; i++) {
        keys[i] = i;
        keys_sorted[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    std::vector<std::thread> worker_list;
    std::vector<std::thread> daemon_list;

    std::atomic_bool shutdown(false);
    // there will be a worker keep on reading
    daemon_list.emplace_back(std::thread([&](){
        while (shutdown.load() == false) {
            // INVARIANT: the result should always be an ascending sequence
            int last = -1;
            int read_cnt = 0;
            auto it = tree.Begin();
            for (; !it->IsEnd(); it->Advance()) {
                int cur = it->Get().Get();
                EXPECT_GT(cur, last);
                last = cur;
                read_cnt++;
            }
            // LOG_INFO("retry times %d read_cnt %d", it.GetRetryCnt(), read_cnt);
        }
    }));

    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            BPlusTreeExecutionContext context;
            GenericKey<8> index_key;
            for (int j = begin; j < end; j++) {
                context.Reset();
                auto rid = RID(keys[j]);
                auto tmp = Tuple({Value(TypeId::BIGINT, keys[j])}, &schema);
                index_key.SetFromKey(tmp);
                EXPECT_EQ(tree.Insert(index_key, rid, &context), true);

                // could we read what we just write?
                std::vector<RID> result;
                EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
                EXPECT_EQ(result[0], RID(keys[j]));
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }
    
    // read them by iterator
    int ptr = 0;
    for (auto it = tree.Begin(); !it->IsEnd(); it->Advance()) {
        EXPECT_EQ(it->Get(), RID(keys_sorted[ptr]));
        ptr++;
    }
    EXPECT_EQ(ptr, key_num * worker_num);

    // delete them
    worker_list.clear();
    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            BPlusTreeExecutionContext context;
            GenericKey<8> index_key;
            for (int j = begin; j < end; j++) {
                context.Reset();
                auto k = Tuple({Value(TypeId::BIGINT, keys[j])}, &schema);
                index_key.SetFromKey(k);
                EXPECT_EQ(tree.Remove(index_key, &context), true);

                // we shouldn't see what we just deleted
                std::vector<RID> result;
                EXPECT_EQ(tree.GetValue(index_key, &result, &context), false);
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    ptr = 0;
    for (auto it = tree.Begin(); !it->IsEnd(); it->Advance()) {
        EXPECT_EQ(it->Get(), RID(keys_sorted[ptr]));
        ptr++;
    }
    EXPECT_EQ(ptr, 0);

    shutdown.store(true);
    for (uint i = 0; i < daemon_list.size(); i++) {
        daemon_list[i].join();
    }

    EXPECT_EQ(bpm->CheckPinCount(), true);

    // check whether we've returned the page back to disk
    EXPECT_EQ(disk_manager->GetAllocateCount(), disk_manager->GetDeallocateCount());

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

}