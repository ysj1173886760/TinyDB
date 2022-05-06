/**
 * @file varlen_type_test.cpp
 * @author sheep
 * @brief varlen type test
 * @version 0.1
 * @date 2022-05-06
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/value.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(VarlenTypeTest, BasicTest) {
    auto x = Value(TypeId::VARCHAR, "abc");
    auto y = Value(TypeId::VARCHAR, "def");
    const char *val = "abc";
    auto z = Value(TypeId::VARCHAR, val, 3);
    auto w = Value(TypeId::VARCHAR, "abcd");

    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareEquals(z), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareLessThan(w), CmpBool::CmpTrue);

    EXPECT_EQ(x.GetLength(), 3);
    EXPECT_EQ(y.GetLength(), 3);
    EXPECT_EQ(z.GetLength(), 3);
    EXPECT_EQ(w.GetLength(), 4);
}

TEST(VarlenTypeTest, NullTest) {
    auto x = Value(TypeId::VARCHAR, "abc");
    auto y = Type::Null(TypeId::VARCHAR);

    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpNull);
}

TEST(VarlenTypeTest, SerializeDeserializeTest) {
    auto x = Value(TypeId::VARCHAR, "abc");
    auto y = Type::Null(TypeId::VARCHAR);

    char buffer[40] = {0};

    x.SerializeTo(buffer);
    auto tmp = Value::DeserializeFrom(buffer, TypeId::VARCHAR);
    EXPECT_EQ(x.CompareEquals(tmp), CmpBool::CmpTrue);

    y.SerializeTo(buffer);
    tmp = Value::DeserializeFrom(buffer, TypeId::VARCHAR);
    EXPECT_EQ(tmp.IsNull(), true);
}

TEST(VarlenTypeTest, CastTest) {
    // cast to/from every thing is a import feature in varchar type
    // tinyint
    auto x = Value(TypeId::TINYINT, static_cast<int8_t>(1));
    auto varchar = x.CastAs(TypeId::VARCHAR);
    EXPECT_EQ(varchar.CompareEquals(Value(TypeId::VARCHAR, "1")), CmpBool::CmpTrue);
    // then cast it back
    auto back = varchar.CastAs(TypeId::TINYINT);
    EXPECT_EQ(back.CompareEquals(x), CmpBool::CmpTrue);

    // smallint
    x = Value(TypeId::SMALLINT, static_cast<int16_t>(256));
    varchar = x.CastAs(TypeId::VARCHAR);
    EXPECT_EQ(varchar.CompareEquals(Value(TypeId::VARCHAR, "256")), CmpBool::CmpTrue);
    back = varchar.CastAs(TypeId::SMALLINT);
    EXPECT_EQ(back.CompareEquals(x), CmpBool::CmpTrue);

    // integer
    x = Value(TypeId::INTEGER, static_cast<int32_t>(20010310));
    varchar = x.CastAs(TypeId::VARCHAR);
    EXPECT_EQ(varchar.CompareEquals(Value(TypeId::VARCHAR, "20010310")), CmpBool::CmpTrue);
    back = varchar.CastAs(TypeId::INTEGER);
    EXPECT_EQ(back.CompareEquals(x), CmpBool::CmpTrue);

    // bigint
    x = Value(TypeId::BIGINT, static_cast<int64_t>(21474836470));
    varchar = x.CastAs(TypeId::VARCHAR);
    EXPECT_EQ(varchar.CompareEquals(Value(TypeId::VARCHAR, "21474836470")), CmpBool::CmpTrue);
    back = varchar.CastAs(TypeId::BIGINT);
    EXPECT_EQ(back.CompareEquals(x), CmpBool::CmpTrue);

    // decimal
    x = Value(TypeId::DECIMAL, static_cast<double>(3.14159));
    varchar = x.CastAs(TypeId::VARCHAR);
    // to_string for double is strange here, so i will test casting directly
    // EXPECT_EQ(varchar.CompareEquals(Value(TypeId::VARCHAR, "3.14159")), CmpBool::CmpTrue);
    back = varchar.CastAs(TypeId::DECIMAL);
    EXPECT_EQ(back.CompareEquals(x), CmpBool::CmpTrue);
}

}