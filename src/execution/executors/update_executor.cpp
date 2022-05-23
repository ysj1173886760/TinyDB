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
        TINYDB_ASSERT(type == info.expression_->GetReturnType(), 
                      "Type doesn't match");
    }

    // i wonder is that possible that child schema is not the same at table schema?
    // for the simplicity, i will assume output schema of child will always be the same
    // as table schema
    TINYDB_ASSERT(child_->GetOutputSchema()->Equal(*table_schema_), "Schema doesn't match");
}

bool UpdateExecutor::Next(Tuple *tuple) {
    Tuple tmp;
    if (child_->Next(&tmp)) {
        Tuple newTuple = GenerateUpdatedTuple(tmp);
        // first update table
        if (table_info_->table_->UpdateTuple(newTuple, tmp.GetRID()).IsErr()) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Failed to perform updation");
        }

        // then update index
        for (const auto &index_info : indexes_) {
            // don't delete index entry in the context of transaction
            // only delete it when txn commits
            index_info->index_->DeleteEntryTupleSchema(tmp, tmp.GetRID());
            index_info->index_->InsertEntryTupleSchema(newTuple, tmp.GetRID());
        }
        return true;
    }

    return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &tuple) {
    auto node = GetPlanNode<UpdatePlan>();
    // tuple is from child executor, evaluate the expression to generate new value
    // first destruct the value
    std::vector<Value> value_list;
    for (uint32_t i = 0; i < table_schema_->GetColumnCount(); i++) {
        value_list.push_back(tuple.GetValue(table_schema_, i));
    }
    // then perform necessary changes
    for (const auto &update_info : node.update_list_) {
        value_list[update_info.column_idx_] = 
            update_info.expression_->Evaluate(&tuple, nullptr);
    }
    // construct the tuple
    auto res = Tuple(value_list, table_schema_);
    // set rid
    res.SetRID(tuple.GetRID());
    return res;
}

}