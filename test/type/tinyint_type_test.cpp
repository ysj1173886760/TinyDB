/**
 * @file tinyint_type_test.cpp
 * @author sheep
 * @brief tinyint test
 * @version 0.1
 * @date 2022-05-04
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TINYINT_TYPE_TEST_CPP
#define TINYINT_TYPE_TEST_CPP

#include "type/tinyint_type.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(TinyintTest, BasicTest) {
    auto x = Value(TypeId::TINYINT, static_cast<int8_t>(13));
    auto y = Value(TypeId::TINYINT, static_cast<int8_t>(7));

    auto sum = x.Add(y);
    auto diff = x.Subtract(y);
    auto product = x.Multiply(y);
    auto quot = x.Divide(y);
    auto rem = x.Modulo(y);

    // internal value test
    EXPECT_EQ(sum.GetAs<int8_t>(), static_cast<int8_t>(20));
    EXPECT_EQ(diff.GetAs<int8_t>(), static_cast<int8_t>(6));
    EXPECT_EQ(product.GetAs<int8_t>(), static_cast<int8_t>(91));
    EXPECT_EQ(quot.GetAs<int8_t>(), static_cast<int8_t>(1));
    EXPECT_EQ(rem.GetAs<int8_t>(), static_cast<int8_t>(6));
}

}

#endif