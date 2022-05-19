/**
 * @file value_factory_test.cpp
 * @author sheep
 * @brief value factory test
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/value_factory.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(ValueFactoryTest, BasicTest) {
    auto value = ValueFactory::GetBooleanValue(true);
    auto value2 = ValueFactory::GetBooleanValue(10);
    EXPECT_EQ(value.CompareEquals(value2), CmpBool::CmpTrue);
}

}