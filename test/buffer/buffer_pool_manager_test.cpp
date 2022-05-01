#include "storage/disk/disk_manager.h"
#include "buffer/buffer_pool_manager.h"

#include <gtest/gtest.h>
#include <string>
#include <random>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

namespace TinyDB {

TEST(BufferPoolManagerTest, BinaryDataTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 10;

    std::random_device rd;
    std::mt19937 mt(rd());
    // generate random number range from 0 to max
    std::uniform_int_distribution<char> dis(0);

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    int page_id[buffer_pool_size * 2];
    auto page0 = bpm->NewPage(&page_id[0]);

    // we shouldn't have any assumption on the page number, since it's decided by disk manager
    // buffer pool is empty, we should be able to create a new page
    EXPECT_NE(nullptr, page0);

    char random_binary_data[PAGE_SIZE];
    // Generate random binary data
    for (char &i : random_binary_data) {
        i = dis(mt);
    }

    // insert terminal characters both in the middle and at end
    random_binary_data[PAGE_SIZE / 2] = '\0';
    random_binary_data[PAGE_SIZE - 1] = '\0';

    std::memcpy(page0->GetData(), random_binary_data, PAGE_SIZE);
    EXPECT_EQ(std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE), 0);

    // we should be able to allocate there pages
    for (size_t i = 1; i < buffer_pool_size; i++) {
        EXPECT_NE(nullptr, bpm->NewPage(&page_id[i]));
    }

    // we should not be able to allocate any new pages
    for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; i++) {
        EXPECT_EQ(nullptr, bpm->NewPage(&page_id[i]));
    }

    // unpin 5 pages and allocate 5 new pages
    // which will lead the previous pages swapped out to disk
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(bpm->UnpinPage(page_id[i], true), true);
    }

    int more_pages[5];
    for (int i = 0; i < 5; i++) {
        EXPECT_NE(bpm->NewPage(&more_pages[i]), nullptr);
        bpm->UnpinPage(more_pages[i], false);
    }

    // fetch first page from disk again
    page0 = bpm->FetchPage(page_id[0]);
    EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));
    EXPECT_EQ(true, bpm->UnpinPage(page_id[0], true));

    delete bpm;
    delete disk_manager;

    remove(filename.c_str());
}

TEST(BufferPoolManagerTest, ConcurrentTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 8;
    const size_t worker_size = 6;
    const size_t total_page_size = 10;
    const size_t iteration_num = 10;

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    std::vector<page_id_t> page_list(total_page_size);
    // allocate total_page_size pages
    for (size_t i = 0; i < total_page_size; i++) {
        EXPECT_NE(bpm->NewPage(&page_list[i]), nullptr);
        EXPECT_EQ(bpm->UnpinPage(page_list[i], false), true);
    }

    const size_t BEGIN_OFFSET = 0;
    const size_t MIDDLE_OFFSET = PAGE_SIZE / 2;
    const size_t END_OFFSET = PAGE_SIZE - sizeof(int);

    std::vector<std::thread> worker_list;
    for (size_t i = 0; i < worker_size; i++) {
        worker_list.emplace_back(std::thread([&]() {
            // thread local random engine
            std::random_device rd;
            std::mt19937 mt(rd());
            std::vector<page_id_t> access_list = page_list;
            
            for (size_t i = 0; i < iteration_num; i++) {
                // shuffle the access list
                std::shuffle(access_list.begin(), access_list.end(), mt);

                for (auto page_id : access_list) {
                    // increase the counter that is located at three area
                    auto page = bpm->FetchPage(page_id);
                    // first lock this page
                    page->WLatch();
                    int *begin_ptr = reinterpret_cast<int *> (page->GetData() + BEGIN_OFFSET);
                    *begin_ptr = *begin_ptr + 1;
                    int *middle_ptr = reinterpret_cast<int *> (page->GetData() + MIDDLE_OFFSET);
                    *middle_ptr = *middle_ptr + 1;
                    int *end_ptr = reinterpret_cast<int *> (page->GetData() + END_OFFSET);
                    *end_ptr = *end_ptr + 1;
                    page->WUnlatch();
                    // unpin the page and mark it dirty
                    bpm->UnpinPage(page_id, true);
                }
            }
        }));
    }

    // wait for the thread
    for (size_t i = 0; i < worker_size; i++) {
        worker_list[i].join();
    }

    // check the value
    for (auto page_id : page_list) {
        auto page = bpm->FetchPage(page_id);
        EXPECT_NE(page, nullptr);
        int *begin_ptr = reinterpret_cast<int *> (page->GetData() + BEGIN_OFFSET);
        EXPECT_EQ(*begin_ptr, iteration_num * worker_size);
        int *middle_ptr = reinterpret_cast<int *> (page->GetData() + MIDDLE_OFFSET);
        EXPECT_EQ(*middle_ptr, iteration_num * worker_size);
        int *end_ptr = reinterpret_cast<int *> (page->GetData() + END_OFFSET);
        EXPECT_EQ(*end_ptr, iteration_num * worker_size);
        bpm->UnpinPage(page_id, false);
    }

    delete bpm;
    delete disk_manager;

    remove(filename.c_str());
}

}