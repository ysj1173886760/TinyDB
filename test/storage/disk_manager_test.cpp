#include <gtest/gtest.h>
#include <string>
#include <random>
#include <cstring>

#include "storage/disk/disk_manager.h"
#include "common/logger.h"

namespace TinyDB {

TEST(DiskManagerTest, SimpleIOTest) {
    std::string filename = "test.db";
    DiskManager diskManager(filename);

    std::string testString1 = "hello world";
    std::string testString2 = "hello tinydb";
    
    char writeBuffer[4096];
    memset(writeBuffer, 0, sizeof(writeBuffer));
    strcpy(writeBuffer, testString1.c_str());
    int page0 = diskManager.allocatePage();
    diskManager.writePage(page0, writeBuffer);

    memset(writeBuffer, 0, sizeof(writeBuffer));
    strcpy(writeBuffer, testString2.c_str());
    int page1 = diskManager.allocatePage();
    diskManager.writePage(page1, writeBuffer);

    char readBuffer[4096];
    diskManager.readPage(page0, readBuffer);
    // LOG_DEBUG("%s", readBuffer);

    int res = strcmp(readBuffer, testString1.c_str());
    EXPECT_EQ(0, res);

    diskManager.readPage(page1, readBuffer);
    // LOG_DEBUG("%s\n", readBuffer);
    res = strcmp(readBuffer, testString2.c_str());
    EXPECT_EQ(0, res);

    remove(filename.c_str());
}

TEST(DiskManagerTest, StrongTest) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dis(0, 255);
    std::string filename = "test.db";
    auto dm = new DiskManager(filename);
    const int test_num = 100;
    std::vector<char *> data_list(test_num);

    for (int i = 0; i < test_num; i++) {
        int pgid = dm->allocatePage();

        // generate a page trash
        char *data = new char[PAGE_SIZE];
        for (int j = 0; j < PAGE_SIZE; j++) {
            data[j] = dis(mt);
        }

        dm->writePage(pgid, data);
        data_list[i] = data;
    }
    delete dm;

    // reopen it and test whether the content is stable
    auto dm2 = new DiskManager(filename);

    for (int i = 0; i < test_num; i++) {
        char buffer[PAGE_SIZE];
        dm2->readPage(i, buffer);

        EXPECT_EQ(std::memcmp(buffer, data_list[i], PAGE_SIZE), 0);

        // free memory
        delete[] data_list[i];
    }

    delete dm2;

    remove(filename.c_str());
}

}