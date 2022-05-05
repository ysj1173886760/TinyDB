/**
 * @file numeric_type.h
 * @author sheep
 * @brief 
 * @version 0.1
 * @date 2022-05-03
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef NUMERIC_TYPE_H
#define NUMERIC_TYPE_H

#include "type/type.h"
#include "type/value.h"

#include <cmath>
#include <assert.h>

namespace TinyDB {
// comments from bustub
// A numeric value is an abstract type representing a number. Numerics can be
// either integral or non-integral (decimal), but must provide arithmetic
// operations on its value.
class NumericType: public Type {
public:
    explicit NumericType(TypeId type_id): Type(type_id) {}
    ~NumericType() override = default;

    // enforce the numeric type to implement those operations
    // we didn't support comparsion functions for decimal type value

    Value Add(const Value &lhs, const Value &rhs) const override = 0;
    Value Subtract(const Value &lhs, const Value &rhs) const override = 0;
    Value Multiply(const Value &lhs, const Value &rhs) const override = 0;
    Value Divide(const Value &lhs, const Value &rhs) const override = 0;
    Value Modulo(const Value &lhs, const Value &rhs) const override = 0;
    Value Sqrt(const Value &val) const override = 0;

    bool IsZero(const Value &val) const override = 0;
    // maybe we should hide this function
    Value OperateNull(const Value &lhs, const Value &rhs) const override = 0;

    Value Min(const Value &lhs, const Value &rhs) const override {
        assert(lhs.CheckComparable(rhs));
        if (lhs.IsNull() || rhs.IsNull()) {
            return lhs.OperateNull(rhs);
        }

        if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
            return lhs.Copy();
        }
        return rhs.Copy();
    }

    Value Max(const Value &lhs, const Value &rhs) const override {
        assert(lhs.CheckComparable(rhs));
        if (lhs.IsNull() || rhs.IsNull()) {
            return lhs.OperateNull(rhs);
        }

        if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
            return rhs.Copy();
        }
        return lhs.Copy();
    }

protected:
    // modulo for float point values
    static inline double ValMod(double x, double y) {
        return x - std::trunc(x / y) * y;
    }
};

}

#endif