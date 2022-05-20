/**
 * @file update_executor.cpp
 * @author sheep
 * @brief update executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/update_executor.h"

namespace TinyDB {

void UpdateExecutor::Init() {
    // initialize child executor recursively
    child_->Init();
    // initialize metadata that we will use while performing updation
    auto node = GetPlanNode<UpdatePlan>();
    table_info_ = context_->GetCatalog()->GetTable(node.table_oid_);
    table_schema_ = &table_info_->schema_;
    indexes_ = context_->GetCatalog()->GetTableIndexes(table_info_->name_);

    // check whether type matches
    for (const auto &info : node.update_list_) {
        auto type = table_schema_->GetColumn(info.column_idx_).GetType();
        if (info.type_ == UpdateType::Set) {
            TINYDB_ASSERT(type == info.value_.GetTypeId(), 
                          "Type doesn't match");
        } else {
            TINYDB_ASSERT(type == info.expression_->GetReturnType(), 
                          "Type doesn't match");
        }
    }
}

bool UpdateExecutor::Next(Tuple *tuple) {

}

}