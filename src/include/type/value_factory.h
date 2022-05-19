/**
 * @file value_factory.h
 * @author sheep
 * @brief value factory
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef VALUE_FACTORY_H
#define VALUE_FACTORY_H

#include "type/value.h"
#include "common/macros.h"
#include "common/logger.h"

namespace TinyDB {

template <class F, class T, class = T>
struct is_static_castable : std::false_type
{};

template <class F, class T>
struct is_static_castable<F, T, decltype(static_cast<T>(std::declval<F>()))> : std::true_type
{};

class ValueFactory {
public:
    /**
     * @brief Get the Boolean Value object
     * I didn't cast "val" to bool and provide a constructor for bool type. Because i've done it before, 
     * and it cause a lot of type error since inside c++ (i think) boolean will be interpreted to int, char or 
     * something else. So don't use boolean type as an initialier and use int8_t instead.
     * @tparam F 
     * @param val 
     * @return Value 
     */
    template <class F>
    static Value GetBooleanValue(F val) {
        static_assert(is_static_castable<F, int8_t>::value, "Failed to convert to boolean");
        return Value(TypeId::BOOLEAN, static_cast<int8_t>(val));
    }

    static Value GetTinyintValue(int8_t val) {
        return Value(TypeId::TINYINT, val);
    }

    static Value GetSmallintValue(int16_t val) {
        return Value(TypeId::SMALLINT, val);
    }

    static Value GetIntegerValue(int32_t val) {
        return Value(TypeId::INTEGER, val);
    }

    static Value GetBigintValue(int64_t val) {
        return Value(TypeId::BIGINT, val);
    }

    static Value GetDecimalValue(double val) {
        return Value(TypeId::DECIMAL, val);
    }

    static Value GetVarcharValue(const std::string val) {
        return Value(TypeId::VARCHAR, val);
    }

    static Value GetVarcharValue(const char *data, size_t size) {
        return Value(TypeId::VARCHAR, data, size);
    }
};

}

#endif