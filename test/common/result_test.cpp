/**
 * @file result_test.cpp
 * @author sheep
 * @brief test for result type
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "common/result.h"

#include <gtest/gtest.h>
#include <string>

namespace TinyDB {

TEST(ResultTest, BasicTest) {
    Result<int> result1(10);
    Result<int> result2(ErrorCode::OUT_OF_MEMORY);

    EXPECT_EQ(result1.IsOk(), true);
    EXPECT_EQ(result1.GetOk(), 10);
    EXPECT_EQ(result2.IsErr(), true);
    EXPECT_EQ(result2.GetErr(), ErrorCode::OUT_OF_MEMORY);
}

}