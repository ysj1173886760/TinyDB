/**
 * @file lru_replacer_test.cpp
 * @author sheep
 * @brief unit test for lru replacer
 * @version 0.1
 * @date 2022-04-30
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include <gtest/gtest.h>

#include "buffer/lru_replacer.h"

namespace TinyDB {

/**
 * @brief test from bustub
 */
TEST(LRUReplacerTest, SimpleTest) {
    // currently num_pages is invalid
    auto lru = LRUReplacer(0);

    lru.Unpin(1);
    lru.Unpin(2);
    lru.Unpin(3);
    lru.Unpin(4);
    lru.Unpin(5);
    lru.Unpin(6);
    lru.Unpin(1);
    EXPECT_EQ(6, lru.Size());

    // get victims from lru
    int value;
    lru.Evict(&value);
    EXPECT_EQ(1, value);
    lru.Evict(&value);
    EXPECT_EQ(2, value);
    lru.Evict(&value);
    EXPECT_EQ(3, value);

    // pin frames in replacer
    // since 3 is evicted, so pin 3 should have no effect
    lru.Pin(3);
    lru.Pin(4);
    EXPECT_EQ(2, lru.Size());

    lru.Unpin(4);
    
    // evict them all
    // we expect to see 5, 6, 4 since we have used 4 recently
    lru.Evict(&value);
    EXPECT_EQ(5, value);
    lru.Evict(&value);
    EXPECT_EQ(6, value);
    lru.Evict(&value);
    EXPECT_EQ(4, value);

    EXPECT_EQ(false, lru.Evict(&value));

    EXPECT_EQ(0, lru.Size());
}

/**
 * @brief you won't believe this test case is from leetcode
 * 
 */
TEST(LRUReplacerTest, SimpleTest2) {
    // currently num_pages is invalid
    auto lru = LRUReplacer(0);

    int value;

    lru.Unpin(1);
    lru.Unpin(2);
    EXPECT_EQ(2, lru.Size());

    // mimic that we have used frame 1
    lru.Pin(1);
    lru.Unpin(1);

    // evict 2
    lru.Evict(&value);
    EXPECT_EQ(2, value);

    // insert 3
    lru.Unpin(3);
    EXPECT_EQ(2, lru.Size());

    // evict 1
    lru.Evict(&value);
    EXPECT_EQ(1, value);

    // insert 4
    lru.Unpin(4);
    EXPECT_EQ(2, lru.Size());

    lru.Evict(&value);
    EXPECT_EQ(3, value);

    lru.Evict(&value);
    EXPECT_EQ(4, value);

    EXPECT_EQ(false, lru.Evict(&value));
}

}