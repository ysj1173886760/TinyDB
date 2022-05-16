/**
 * @file index_test.cpp
 * @author sheep
 * @brief test the index interface
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/index/index_builder.h"
#include "storage/index/index.h"

#include <gtest/gtest.h>
#include <thread>

namespace TinyDB {

TEST(IndexTest, InterfaceTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto index_schema = Schema(cols);
    auto table_schema = Schema(cols);
    std::vector<uint32_t> key_attrs = {0};

    auto index_metadata = std::make_unique<IndexMetadata>("test",
                                                          "test", 
                                                          &table_schema, 
                                                          key_attrs,
                                                          IndexType::BPlusTreeType, 
                                                          8);
    auto index = IndexBuilder::Build(std::move(index_metadata), bpm);

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
            auto it = index->Begin();
            for (; !it.IsEnd(); it.Advance()) {
                int cur = it.Get().Get();
                EXPECT_GT(cur, last);
                last = cur;
                read_cnt++;
            }
            // LOG_INFO("retry times %d read_cnt %d", it.GetRetryCnt(), read_cnt);
        }
    }));

    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            for (int j = begin; j < end; j++) {
                auto rid = RID(keys[j]);
                auto tmp = Tuple({Value(TypeId::BIGINT, keys[j])}, &index_schema);
                index->InsertEntry(tmp, rid);

                // could we read what we just write?
                std::vector<RID> result;
                index->ScanKey(tmp, &result);
                EXPECT_EQ(result.size(), 1);
                EXPECT_EQ(result[0], RID(keys[j]));
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }
    
    // read them by iterator
    int ptr = 0;
    for (auto it = index->Begin(); !it.IsEnd(); it.Advance()) {
        EXPECT_EQ(it.Get(), RID(keys_sorted[ptr]));
        ptr++;
    }
    EXPECT_EQ(ptr, key_num * worker_num);

    // delete them
    worker_list.clear();
    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            for (int j = begin; j < end; j++) {
                auto k = Tuple({Value(TypeId::BIGINT, keys[j])}, &index_schema);
                index->DeleteEntry(k, RID());

                // we shouldn't see what we just deleted
                std::vector<RID> result;
                index->ScanKey(k, &result);
                EXPECT_EQ(result.size(), 0);
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    ptr = 0;
    for (auto it = index->Begin(); !it.IsEnd(); it.Advance()) {
        EXPECT_EQ(it.Get(), RID(keys_sorted[ptr]));
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