/**
 * @file integer_type_test.cpp
 * @author sheep
 * @brief integer type test
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/integer_type.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(IntegerTest, BasicTest) {
    auto x = Value(TypeId::INTEGER, static_cast<int32_t>(20010310));
    auto y = Value(TypeId::INTEGER, static_cast<int32_t>(42));

    auto sum = x.Add(y);
    auto dif = x.Subtract(y);
    auto pro = x.Multiply(y);
    auto quo = x.Divide(y);
    auto rem = x.Modulo(y);
    auto max = x.Max(y);
    auto min = x.Min(y);

    auto sum_ans = static_cast<int32_t>(20010352);
    auto dif_ans = static_cast<int32_t>(20010268);
    auto pro_ans = static_cast<int32_t>(840433020);
    auto quo_ans = static_cast<int32_t>(476435);
    auto rem_ans = static_cast<int32_t>(40);
    auto max_ans = static_cast<int32_t>(20010310);
    auto min_ans = static_cast<int32_t>(42);

    // internal value test
    EXPECT_EQ(sum.GetAs<int32_t>(), sum_ans);
    EXPECT_EQ(dif.GetAs<int32_t>(), dif_ans);
    EXPECT_EQ(pro.GetAs<int32_t>(), pro_ans);
    EXPECT_EQ(quo.GetAs<int32_t>(), quo_ans);
    EXPECT_EQ(rem.GetAs<int32_t>(), rem_ans);
    EXPECT_EQ(max.GetAs<int32_t>(), max_ans);
    EXPECT_EQ(min.GetAs<int32_t>(), min_ans);
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpFalse);

    // value test
    EXPECT_EQ(sum.CompareEquals(Value(TypeId::INTEGER, sum_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(dif.CompareEquals(Value(TypeId::INTEGER, dif_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(pro.CompareEquals(Value(TypeId::INTEGER, pro_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(quo.CompareEquals(Value(TypeId::INTEGER, quo_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(rem.CompareEquals(Value(TypeId::INTEGER, rem_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(max.CompareEquals(Value(TypeId::INTEGER, max_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(min.CompareEquals(Value(TypeId::INTEGER, min_ans)), CmpBool::CmpTrue);
}

TEST(IntegerTest, NullTest) {
    auto x = Value(TypeId::INTEGER, static_cast<int32_t>(0));
    auto y = Type::Null(TypeId::INTEGER);

    auto sum = x.Add(y);
    auto dif = x.Subtract(y);
    auto pro = x.Multiply(y);
    auto quo = x.Divide(y);
    auto rem = x.Modulo(y);
    auto max = x.Max(y);
    auto min = x.Min(y);

    EXPECT_EQ(x.IsZero(), true);
    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(sum.IsNull(), true);
    EXPECT_EQ(dif.IsNull(), true);
    EXPECT_EQ(pro.IsNull(), true);
    EXPECT_EQ(quo.IsNull(), true);
    EXPECT_EQ(rem.IsNull(), true);
    EXPECT_EQ(max.IsNull(), true);
    EXPECT_EQ(min.IsNull(), true);
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpNull);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpNull);
}

TEST(IntegerTest, SerializeDeserializeTest) {
    auto x = Value(TypeId::INTEGER, static_cast<int32_t>(20010310));
    auto y = Type::Null(TypeId::INTEGER);

    char buffer[40] = {0};

    x.SerializeTo(buffer);
    auto tmp = Value::DeserializeFrom(buffer, TypeId::INTEGER);

    EXPECT_EQ(x.CompareEquals(tmp), CmpBool::CmpTrue);

    y.SerializeTo(buffer);
    tmp = Value::DeserializeFrom(buffer, TypeId::INTEGER);

    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(tmp.CompareEquals(y), CmpBool::CmpNull);
}

}