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
    auto maxx = x.Max(y);
    auto minn = x.Min(y);

    // internal value test
    EXPECT_EQ(sum.GetAs<int8_t>(), static_cast<int8_t>(20));
    EXPECT_EQ(diff.GetAs<int8_t>(), static_cast<int8_t>(6));
    EXPECT_EQ(product.GetAs<int8_t>(), static_cast<int8_t>(91));
    EXPECT_EQ(quot.GetAs<int8_t>(), static_cast<int8_t>(1));
    EXPECT_EQ(rem.GetAs<int8_t>(), static_cast<int8_t>(6));
    EXPECT_EQ(maxx.GetAs<int8_t>(), static_cast<int8_t>(13));
    EXPECT_EQ(minn.GetAs<int8_t>(), static_cast<int8_t>(7));
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpFalse);

    // value test
    EXPECT_EQ(sum.CompareEquals(Value(TypeId::TINYINT, static_cast<int8_t>(20))), CmpBool::CmpTrue);
    EXPECT_EQ(diff.CompareEquals(Value(TypeId::TINYINT, static_cast<int8_t>(6))), CmpBool::CmpTrue);
    EXPECT_EQ(product.CompareEquals(Value(TypeId::TINYINT, static_cast<int8_t>(91))), CmpBool::CmpTrue);
    EXPECT_EQ(quot.CompareEquals(Value(TypeId::TINYINT, static_cast<int8_t>(1))), CmpBool::CmpTrue);
    EXPECT_EQ(rem.CompareEquals(Value(TypeId::TINYINT, static_cast<int8_t>(6))), CmpBool::CmpTrue);
    EXPECT_EQ(maxx.CompareEquals(Value(TypeId::TINYINT, static_cast<int8_t>(13))), CmpBool::CmpTrue);
    EXPECT_EQ(minn.CompareEquals(Value(TypeId::TINYINT, static_cast<int8_t>(7))), CmpBool::CmpTrue);
}

TEST(TinyintTest, NullTest) {
    auto x = Value(TypeId::TINYINT, static_cast<int8_t>(0));
    auto y = Type::Null(TypeId::TINYINT);

    auto sum = x.Add(y);
    auto diff = x.Subtract(y);
    auto product = x.Multiply(y);
    auto quot = x.Divide(y);
    auto rem = x.Modulo(y);
    auto maxx = x.Max(y);
    auto minn = x.Min(y);

    EXPECT_EQ(x.IsZero(), true);
    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(sum.IsNull(), true);
    EXPECT_EQ(diff.IsNull(), true);
    EXPECT_EQ(product.IsNull(), true);
    EXPECT_EQ(quot.IsNull(), true);
    EXPECT_EQ(rem.IsNull(), true);
    EXPECT_EQ(maxx.IsNull(), true);
    EXPECT_EQ(minn.IsNull(), true);
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpNull);
}

TEST(TinyintTest, SerializeDeserializeTest) {
    auto x = Value(TypeId::TINYINT, static_cast<int8_t>(16));
    auto y = Type::Null(TypeId::TINYINT);

    char buffer[40] = {0};

    x.SerializeTo(buffer);
    auto tmp = Value::DeserializeFrom(buffer, TypeId::TINYINT);

    EXPECT_EQ(x.CompareEquals(tmp), CmpBool::CmpTrue);

    y.SerializeTo(buffer);
    tmp = Value::DeserializeFrom(buffer, TypeId::TINYINT);

    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(tmp.CompareEquals(y), CmpBool::CmpNull);
}

}

#endif