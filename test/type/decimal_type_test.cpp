/**
 * @file decimal_type_test.cpp
 * @author sheep
 * @brief decimal type test
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "type/decimal_type.h"
#include "common/logger.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(DecimalTest, BasicTest) {
    const double eps = 1e-10;
    auto x_origin = static_cast<double>(20010310);
    auto y_origin = static_cast<double>(42);
    auto x = Value(TypeId::DECIMAL, x_origin);
    auto y = Value(TypeId::DECIMAL, y_origin);

    auto sum = x.Add(y);
    auto dif = x.Subtract(y);
    auto pro = x.Multiply(y);
    auto quo = x.Divide(y);
    auto rem = x.Modulo(y);
    auto max = x.Max(y);
    auto min = x.Min(y);

    auto sum_ans = static_cast<double>(x_origin + y_origin);
    auto dif_ans = static_cast<double>(x_origin - y_origin);
    auto pro_ans = static_cast<double>(x_origin * y_origin);
    auto quo_ans = static_cast<double>(x_origin / y_origin);
    auto rem_ans = static_cast<double>(x_origin - std::trunc(quo_ans) * y_origin);
    auto max_ans = static_cast<double>(x_origin);
    auto min_ans = static_cast<double>(y_origin);

    // internal value test
    EXPECT_LE(abs(sum.GetAs<double>() - sum_ans), eps);
    EXPECT_LE(abs(dif.GetAs<double>() - dif_ans), eps);
    EXPECT_LE(abs(pro.GetAs<double>() - pro_ans), eps);
    EXPECT_LE(abs(quo.GetAs<double>() - quo_ans), eps);
    EXPECT_LE(abs(rem.GetAs<double>() - rem_ans), eps);
    EXPECT_LE(abs(max.GetAs<double>() - max_ans), eps);
    EXPECT_LE(abs(min.GetAs<double>() - min_ans), eps);
    
    // compare test
    EXPECT_EQ(x.CompareEquals(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareNotEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThan(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareGreaterThanEquals(y), CmpBool::CmpTrue);
    EXPECT_EQ(x.CompareLessThan(y), CmpBool::CmpFalse);
    EXPECT_EQ(x.CompareLessThanEquals(y), CmpBool::CmpFalse);

    // value test
    EXPECT_EQ(sum.CompareEquals(Value(TypeId::DECIMAL, sum_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(dif.CompareEquals(Value(TypeId::DECIMAL, dif_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(pro.CompareEquals(Value(TypeId::DECIMAL, pro_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(quo.CompareEquals(Value(TypeId::DECIMAL, quo_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(rem.CompareEquals(Value(TypeId::DECIMAL, rem_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(max.CompareEquals(Value(TypeId::DECIMAL, max_ans)), CmpBool::CmpTrue);
    EXPECT_EQ(min.CompareEquals(Value(TypeId::DECIMAL, min_ans)), CmpBool::CmpTrue);
}

TEST(DecimalTest, NullTest) {
    auto x = Value(TypeId::DECIMAL, static_cast<double>(0));
    auto y = Type::Null(TypeId::DECIMAL);

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

TEST(DecimalTest, SerializeDeserializeTest) {
    auto x = Value(TypeId::DECIMAL, static_cast<double>(20010310));
    auto y = Type::Null(TypeId::DECIMAL);

    char buffer[40] = {0};

    x.SerializeTo(buffer);
    auto tmp = Value::DeserializeFrom(buffer, TypeId::DECIMAL);

    EXPECT_EQ(x.CompareEquals(tmp), CmpBool::CmpTrue);

    y.SerializeTo(buffer);
    tmp = Value::DeserializeFrom(buffer, TypeId::DECIMAL);

    EXPECT_EQ(y.IsNull(), true);
    EXPECT_EQ(tmp.CompareEquals(y), CmpBool::CmpNull);
}

}