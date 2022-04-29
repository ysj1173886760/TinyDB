#include <gtest/gtest.h>
#include <string>

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

}