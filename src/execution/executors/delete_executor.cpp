/**
 * @file delete_executor.cpp
 * @author sheep
 * @brief delete executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/delete_executor.h"
#include "common/macros.h"

namespace TinyDB {

void DeleteExecutor::Init() {
    child_->Init();
    // initialize metadata that we will use while performing updation
    auto node = GetPlanNode<DeletePlan>();
    table_info_ = context_->GetCatalog()->GetTable(node.table_oid_);
    table_schema_ = &table_info_->schema_;
    indexes_ = context_->GetCatalog()->GetTableIndexes(table_info_->name_);
    txn_manager_ = context_->GetTransactionManager();
    txn_context_ = context_->GetTransactionContext();
}

bool DeleteExecutor::Next(UNUSED_ATTRIBUTE Tuple *tuple) {
    if (!txn_manager_) {
        return NextWithoutTxn(tuple);
    } else {
        return NextWithTxn(tuple);
    }
}

bool DeleteExecutor::NextWithTxn(Tuple *tuple) {
    Tuple tmp;
    if (child_->Next(&tmp)) {
        // txn manager will handle delete for us
        txn_manager_->Delete(txn_context_, tmp, tmp.GetRID(), table_info_);

        return true;
    }

    return false;
}

bool DeleteExecutor::NextWithoutTxn(Tuple *tuple) {
    Tuple tmp;
    if (child_->Next(&tmp)) {
        if (table_info_->table_->MarkDelete(tmp.GetRID()).IsErr()) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Failed to MarkDelete");
        }

        for (const auto &index_info : indexes_) {
            // still, we need to perform the deletion when commiting the txn
            index_info->index_->DeleteEntryTupleSchema(tmp, tmp.GetRID());
        }
        return true;
    }

    return false;
}

}