/**
 * @file column.cpp
 * @author column implementation
 * @brief nothing, maybe we should just embed this to header
 * @version 0.1
 * @date 2022-05-07
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "catalog/column.h"

#include <string>
#include <sstream>

namespace TinyDB {

std::string Column::ToString() const {
    std::ostringstream os;
    os << "Column[" << column_name_ << ", " << Type::TypeToString(column_type_)
       << ", Offset:" << column_offset_ << ", ";
    if (IsInlined()) {
        os << "FixedLength:" << fixed_length_;
    } else {
        os << "VariableLength:" << variable_length_;
    }
    os << "]";
    return os.str();
}

}