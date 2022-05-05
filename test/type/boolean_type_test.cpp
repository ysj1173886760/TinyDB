/**
 * @file boolean_type_test.cpp
 * @author sheep
 * @brief boolean type test
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/boolean_type.h"
#include "type/value.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(BooleanTest, BasicTest) {
    // x should be automatically casted to 1 
    auto x = Value(TypeId::BOOLEAN, static_cast<int8_t>(42));
    auto y = Value(TypeId::BOOLEAN, static_cast<int8_t>(0));

    EXPECT_EQ(x.IsTrue(), true);
    EXPECT_EQ(y.IsFalse(), true);

    // internal value test
    EXPECT_EQ(x.GetAs<int8_t>(), static_cast<int8_t>(1));
    EXPECT_EQ(y.GetAs<int8_t>(), static_cast<int8_t>(0));
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpTrue);

    // value test
    EXPECT_EQ(x.CompareEquals(Value(TypeId::BOOLEAN, static_cast<int8_t>(30))), CmpBool::CmpTrue);
    EXPECT_EQ(y.CompareEquals(Value(TypeId::BOOLEAN, static_cast<int8_t>(0))), CmpBool::CmpTrue);
}

TEST(BooleanTest, NullTest) {
    auto x = Value(TypeId::BOOLEAN, static_cast<int8_t>(0));
    auto y = Type::Null(TypeId::BOOLEAN);

    // throw not implement exception
    // EXPECT_ANY_THROW(x.IsZero());
    EXPECT_EQ(y.IsNull(), true);
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpNull);
}

TEST(BooleanTest, SerializeDeserializeTest) {
    auto x = Value(TypeId::BOOLEAN, static_cast<int8_t>(16));
    auto y = Type::Null(TypeId::BOOLEAN);

    char buffer[40] = {0};

    x.SerializeTo(buffer);
    auto tmp = Value::DeserializeFrom(buffer, TypeId::BOOLEAN);

    EXPECT_EQ(x.CompareEquals(tmp), CmpBool::CmpTrue);

    y.SerializeTo(buffer);
    tmp = Value::DeserializeFrom(buffer, TypeId::BOOLEAN);

    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(tmp.CompareEquals(y), CmpBool::CmpNull);
}

}