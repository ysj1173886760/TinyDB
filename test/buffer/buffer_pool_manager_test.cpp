#include "storage/disk/disk_manager.h"
#include "buffer/buffer_pool_manager.h"

#include <gtest/gtest.h>
#include <string>
#include <random>
#include <cstring>

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

    page_id_t page_id_tmp;
    auto page0 = bpm->NewPage(&page_id_tmp);

    // buffer pool is empty, we should be able to create a new page
    // and page id is 0
    EXPECT_NE(nullptr, page0);
    EXPECT_EQ(0, page_id_tmp);

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
        EXPECT_NE(nullptr, bpm->NewPage(&page_id_tmp));
    }

    // we should not be able to allocate any new pages
    for (size_t i = 0; i < buffer_pool_size; i++) {
        EXPECT_EQ(nullptr, bpm->NewPage(&page_id_tmp));
    }

    // unpin 5 pages and allocate 5 new pages
    // which will lead the previous pages swapped out to disk
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(bpm->UnpinPage(i, true), true);
    }
    for (int i = 0; i < 5; i++) {
        EXPECT_NE(bpm->NewPage(&page_id_tmp), nullptr);
        bpm->UnpinPage(page_id_tmp, false);
    }

    // fetch page 0 from disk again
    page0 = bpm->FetchPage(0);
    EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));
    EXPECT_EQ(true, bpm->UnpinPage(0, true));

    delete bpm;
    delete disk_manager;

    remove(filename.c_str());
}

}