#include <gtest/gtest.h>
#include <string>

#include "disk_manager.h"
#include "logger.h"

namespace TinyDB {

TEST(DiskManagerTest, SimpleIOTest) {
    DiskManager diskManager("test.db");

    std::string testString1 = "hello world";
    std::string testString2 = "hello tinydb";
    
    char writeBuffer[4096];
    memset(writeBuffer, 0, sizeof(writeBuffer));
    strcpy(writeBuffer, testString1.c_str());
    diskManager.writePage(0, writeBuffer);

    memset(writeBuffer, 0, sizeof(writeBuffer));
    strcpy(writeBuffer, testString2.c_str());
    diskManager.writePage(1, writeBuffer);

    char readBuffer[4096];
    diskManager.readPage(0, readBuffer);
    LOG_INFO("%s", readBuffer);

    int res = strcmp(readBuffer, testString1.c_str());
    EXPECT_EQ(0, res);

    diskManager.readPage(1, readBuffer);
    LOG_INFO("%s\n", readBuffer);
    res = strcmp(readBuffer, testString2.c_str());
    EXPECT_EQ(0, res);

}

}