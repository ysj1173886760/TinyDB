/**
 * @file boolean_type.cpp
 * @author sheep
 * @brief boolean type implementation
 * @version 0.1
 * @date 2022-05-05
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef BOOLEAN_TYPE_CPP
#define BOOLEAN_TYPE_CPP

#include "type/boolean_type.h"
#include "type/value.h"
#include "common/exception.h"

#include <assert.h>

namespace TinyDB {

#define BOOLEAN_CHECK(x) assert(x.value_.boolean_ == 0 || x.value_.boolean_ == 1)

CmpBool BooleanType::CompareEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    assert(lhs.GetTypeId() == TypeId::BOOLEAN);
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BOOLEAN_CHECK(lhs);

    switch (rhs.GetTypeId()) {
    case TypeId::BOOLEAN:
        BOOLEAN_CHECK(rhs);
        return lhs.value_.boolean_ == rhs.value_.boolean_ ?
                CmpBool::CmpTrue : CmpBool::CmpFalse;
    case TypeId::VARCHAR: {
        // cast it then check
        auto tmp = rhs.CastAs(TypeId::BOOLEAN);
        // also check new value
        BOOLEAN_CHECK(tmp);
        return lhs.value_.boolean_ == tmp.value_.boolean_ ?
                CmpBool::CmpTrue : CmpBool::CmpFalse;
    }
    default:
        break;
    }

    // throw logic error, since we've checked whether they are comparable or not
    THROW_UNREACHABLE_EXCEPTION("logic error");
}

CmpBool BooleanType::CompareNotEquals(const Value &lhs, const Value &rhs) const {
    assert(lhs.CheckComparable(rhs));
    assert(lhs.GetTypeId() == TypeId::BOOLEAN);
    if (lhs.IsNull() || rhs.IsNull()) {
        return CmpBool::CmpNull;
    }

    BOOLEAN_CHECK(lhs);

    switch (rhs.GetTypeId()) {
    case TypeId::BOOLEAN:
        BOOLEAN_CHECK(rhs);
        return lhs.value_.boolean_ != rhs.value_.boolean_ ?
                CmpBool::CmpTrue : CmpBool::CmpFalse;
    case TypeId::VARCHAR: {
        auto tmp = rhs.CastAs(TypeId::BOOLEAN);
        BOOLEAN_CHECK(tmp);
        return lhs.value_.boolean_ != tmp.value_.boolean_ ?
                CmpBool::CmpTrue : CmpBool::CmpFalse;
    }
    default:
        break;
    }

    THROW_UNREACHABLE_EXCEPTION("logic error");
}

bool BooleanType::IsTrue(const Value &val) const {
    assert(val.GetTypeId() == TypeId::BOOLEAN);

    return val.value_.boolean_ == 1;
}

bool BooleanType::IsFalse(const Value &val) const {
    assert(val.GetTypeId() == TypeId::BOOLEAN);

    return val.value_.boolean_ == 0;
}

std::string BooleanType::ToString(const Value &val) const {
    assert(val.GetTypeId() == TypeId::BOOLEAN);

    if (val.IsNull()) {
        return "boolean_null";
    }
    BOOLEAN_CHECK(val);

    if (val.value_.boolean_ == 1) {
        return "true";
    } else {
        return "false";
    }
}

void BooleanType::SerializeTo(const Value &val, char *storage) const {
    *reinterpret_cast<int8_t *>(storage) = val.value_.boolean_;
}

Value BooleanType::DeserializeFrom(const char *storage) const {
    auto val = *reinterpret_cast<const int8_t *>(storage);
    return Value(TypeId::BOOLEAN, val);
}

Value BooleanType::Copy(const Value &val) const {
    return Value(TypeId::BOOLEAN, val.value_.boolean_);
}

Value BooleanType::CastAs(const Value &val, TypeId type_id) const {
    assert(IsCoercableFrom(type_id));

    if (val.IsNull()) {
        return Type::Null(type_id);
    }
    switch (type_id) {
    case TypeId::BOOLEAN:
        return Copy(val);
    case TypeId::VARCHAR:
        return Value(type_id, val.ToString());
    default:
        break;
    }

    // throw logic error, since we've checked whether we are coercable from type_id
    THROW_UNREACHABLE_EXCEPTION("logic error");
}

}

#endif