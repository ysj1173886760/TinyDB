/**
 * @file integer_parent_type.h
 * @author sheep
 * @brief 
 * @version 0.1
 * @date 2022-05-03
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef INTEGER_PARENT_TYPE_H
#define INTEGER_PARENT_TYPE_H

#include "type/numeric_type.h"
#include "common/exception.h"
#include "type/value.h"

#include <string>

namespace TinyDB {

// integer value of common sizes
class IntegerParentType: public NumericType {
public: 
    explicit IntegerParentType(TypeId type_id);
    ~IntegerParentType() override = default;

    // whether you declare it's virtual or not, it will be virtual
    // and = 0 indicate that this is pure method, which has no implementation
    // this will force the sub-class to implement those operations and not
    // fallback to base class to throw exception
    // i.e. use compiler to force the implementation, instead of runtime exception

    Value Add(const Value &left, const Value &right) const override = 0;
    Value Subtract(const Value &left, const Value &right) const override = 0;
    Value Multiply(const Value &left, const Value &right) const override = 0;
    Value Divide(const Value &left, const Value &right) const override = 0;
    Value Modulo(const Value &left, const Value &right) const override = 0;
    Value Min(const Value &left, const Value &right) const override = 0;
    Value Max(const Value &left, const Value &right) const override = 0;
    Value Sqrt(const Value &val) const override = 0;
    Value OperateNull(const Value &val, const Value &right) const override = 0;
    bool IsZero(const Value &val) const override = 0;

    CmpBool CompareEquals(const Value &left, const Value &right) const = 0;
    CmpBool CompareNotEquals(const Value &left, const Value &right) const = 0;
    CmpBool CompareLessThan(const Value &left, const Value &right) const = 0;
    CmpBool CompareLessThanEquals(const Value &left, const Value &right) const = 0;
    CmpBool CompareGreaterThan(const Value &left, const Value &right) const = 0;
    CmpBool CompareGreaterThanEquals(const Value &left, const Value &right) const = 0;

    // integer types are always inlined
    bool IsInlined(const Value &val) const override { return true; }

    // for debug purpose
    std::string ToString(const Value &val) const override = 0;

    
    std::string SerializeToString(const Value &val) const override = 0;
    Value DeserializeFromString(const std::string &data) const override = 0;
    
    void SerializeTo(const Value &val, char *storage) const override = 0;
    Value DeserializeFrom(const char *storage) const override = 0;

    Value Copy(const Value &val) const override = 0;

    Value CastAs(const Value &val, TypeId type_id) const override = 0;

protected:
    template <class T1, class T2>
    Value AddValue(const Value &lhs, const Value &rhs) const;

    template <class T1, class T2>
    Value SubtractValue(const Value &lhs, const Value &rhs) const;

    template <class T1, class T2>
    Value MultiplyValue(const Value &lhs, const Value &rhs) const;

    template <class T1, class T2>
    Value DivideValue(const Value &lhs, const Value &rhs) const;

    template <class T1, class T2>
    Value ModuloValue(const Value &lhs, const Value &rhs) const;
};

// template code should stay in header file
// otherwise you will fail to instantiate the functions

// FIXME: is this too expensive?
template <class T1, class T2>
Value IntegerParentType::AddValue(const Value &lhs, const Value &rhs) const {
    auto x = lhs.GetAs<T1>();
    auto y = rhs.GetAs<T2>();
    auto sum1 = static_cast<T1> (x + y);
    auto sum2 = static_cast<T2> (x + y);

    if ((x + y) != sum1 && (x + y) != sum2) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "Integer value out of range");
    }

    // overflow detection
    if (sizeof(x) >= sizeof(y)) {
        if ((x > 0 && y > 0 && sum1 < 0) || 
            (x < 0 && y < 0 && sum1 > 0)) {
            throw Exception(ExceptionType::OUT_OF_RANGE, "Integer value out of range");
        }
        return Value(lhs.GetTypeId(), sum1);
    }

    if ((x > 0 && y > 0 && sum2 < 0) || 
        (x < 0 && y < 0 && sum2 > 0)) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "Integer value out of range");
    }
    return Value(right.GetTypeId(), sum2);
}

template <class T1, class T2>
Value IntegerParentType::SubtractValue(const Value &lhs, const Value &rhs) const {
    auto x = lhs.GetAs<T1>();
    auto y = rhs.GetAs<T2>();
    auto diff1 = static_cast<T1> (x - y);
    auto diff2 = static_cast<T2> (x - y);

    // i think this check can be avoided
    // by only do the up casting
    // i.e. convert x and y to the bigger type then do numeric operation
    if ((x - y) != diff1 && (x - y) != diff2) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "Integer value our of range");
    }

    if (sizeof(x) >= sizeof(y)) {
        if ((x > 0 && y < 0 && diff1 < 0) ||
            (x < 0 && y > 0 && diff1 > 0)) {
            throw Exception(ExceptionType::OUT_OF_RANGE, "Integer value our of range");
        }
        return Value(lhs.GetTypeId(), diff1);
    }

    if ((x > 0 && y < 0 && diff2 < 0) ||
        (x < 0 && y > 0 && diff2 > 0)) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "Integer value our of range");
    }
    return Value(rhs.GetTypeId(), diff2);
}

template <class T1, class T2>
Value IntegerParentType::MultiplyValue(const Value &lhs, const Value &rhs) const {
    auto x = lhs.GetAs<T1>();
    auto y = rhs.GetAs<T2>();

}

}

#endif