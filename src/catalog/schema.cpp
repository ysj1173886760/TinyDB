/**
 * @file schema.cpp
 * @author sheep
 * @brief implementation of schema
 * @version 0.1
 * @date 2022-05-07
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "catalog/schema.h"

#include <sstream>
#include <string>

namespace TinyDB {

Schema::Schema(const std::vector<Column> &columns) {
    uint32_t cur_offset = 0;
    is_tuple_inlined_ = true;

    for (size_t i = 0; i < columns.size(); i++) {
        columns_.push_back(columns[i]);
        if (!columns[i].IsInlined()) {
            is_tuple_inlined_ = false;
            uninlined_columns_.push_back(i);
        }

        columns_[i].column_offset_ = cur_offset;
        cur_offset += columns_[i].GetFixedLength();
    }

    // set tuple length
    length_ = cur_offset;
}

Schema *Schema::CopySchema(const Schema *from, const std::vector<uint32_t> &column_indices) {
    std::vector<Column> cols;
    for (const auto i : column_indices) {
        cols.push_back(from->GetColumn(i));
    }
    return new Schema(cols);
}

std::string Schema::ToString() const {
    std::ostringstream os;

    os << "Schema["
       << "NumColumns:" << GetColumnCount() << ", "
       << "IsInlined:" << IsInlined() << ", "
       << "Length:" << GetLength() << "]";
    
    os << " :: (";
    int len = static_cast<int>(columns_.size());
    for (int i = 0; i < len - 1; i++) {
        os << columns_[i].ToString() << ", ";
    }
    os << columns_.back().ToString() << ")";

    return os.str();
}

}