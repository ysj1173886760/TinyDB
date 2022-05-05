/**
 * @file bigint_type_test.cpp
 * @author sheep
 * @brief bigint type test
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/bigint_type.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(BigintTest, BasicTest) {
    auto x = Value(TypeId::BIGINT, static_cast<int64_t>(20010310));
    auto y = Value(TypeId::BIGINT, static_cast<int64_t>(20000915));

    auto sum = x.Add(y);
    auto dif = x.Subtract(y);
    auto pro = x.Multiply(y);
    auto quo = x.Divide(y);
    auto rem = x.Modulo(y);
    auto max = x.Max(y);
    auto min = x.Min(y);

    auto sum_ans = static_cast<int64_t>(40011225);
    auto dif_ans = static_cast<int64_t>(9395);
    auto pro_ans = static_cast<int64_t>(400224509433650);
    auto quo_ans = static_cast<int64_t>(1);
    auto rem_ans = static_cast<int64_t>(9395);
    auto max_ans = static_cast<int64_t>(20010310);
    auto min_ans = static_cast<int64_t>(20000915);

    // internal value test
    EXPECT_EQ(sum.GetAs<int64_t>(), sum_ans);
    EXPECT_EQ(dif.GetAs<int64_t>(), dif_ans);
    EXPECT_EQ(pro.GetAs<int64_t>(), pro_ans);
    EXPECT_EQ(quo.GetAs<int64_t>(), quo_ans);
    EXPECT_EQ(rem.GetAs<int64_t>(), rem_ans);
    EXPECT_EQ(max.GetAs<int64_t>(), max_ans);
    EXPECT_EQ(min.GetAs<int64_t>(), min_ans);
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpFalse);

    // value test
    EXPECT_EQ(sum.CompareEquals(Value(TypeId::BIGINT, sum_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(dif.CompareEquals(Value(TypeId::BIGINT, dif_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(pro.CompareEquals(Value(TypeId::BIGINT, pro_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(quo.CompareEquals(Value(TypeId::BIGINT, quo_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(rem.CompareEquals(Value(TypeId::BIGINT, rem_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(max.CompareEquals(Value(TypeId::BIGINT, max_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(min.CompareEquals(Value(TypeId::BIGINT, min_ans)), CmpBool::CmpTrue);
}

TEST(BigintTest, NullTest) {
    auto x = Value(TypeId::BIGINT, static_cast<int64_t>(0));
    auto y = Type::Null(TypeId::BIGINT);

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

TEST(BigintTest, SerializeDeserializeTest) {
    auto x = Value(TypeId::BIGINT, static_cast<int64_t>(20010310));
    auto y = Type::Null(TypeId::BIGINT);

    char buffer[40] = {0};

    x.SerializeTo(buffer);
    auto tmp = Value::DeserializeFrom(buffer, TypeId::BIGINT);

    EXPECT_EQ(x.CompareEquals(tmp), CmpBool::CmpTrue);

    y.SerializeTo(buffer);
    tmp = Value::DeserializeFrom(buffer, TypeId::BIGINT);

    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(tmp.CompareEquals(y), CmpBool::CmpNull);
}

}