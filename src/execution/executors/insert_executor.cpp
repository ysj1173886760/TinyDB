/**
 * @file insert_executor.cpp
 * @author sheep
 * @brief insert executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/insert_executor.h"

namespace TinyDB {

void InsertExecutor::Init() {
    auto node = GetPlanNode<InsertPlan>();
    if (!node.IsRawInsert()) {
        // initialize child executor
        TINYDB_ASSERT(child_.get() != nullptr, "Logic error");
        child_->Init();
    }

    table_info_ = context_->GetCatalog()->GetTable(node.table_oid_);
    table_schema_ = &table_info_->schema_;
    indexes_ = context_->GetCatalog()->GetTableIndexes(table_info_->name_);
    txn_manager_ = context_->GetTransactionManager();
    txn_context_ = context_->GetTransactionContext();
}

bool InsertExecutor::Next(Tuple *tuple) {
    if (!txn_manager_) {
        return NextWithoutTxn(tuple);
    } else {
        return NextWithTxn(tuple);
    }
}

bool InsertExecutor::NextWithTxn(Tuple *tuple) {
    auto node = GetPlanNode<InsertPlan>();
    if (node.IsRawInsert()) {
        for (const auto &tuple: node.tuples_) {
            RID rid;
            txn_manager_->Insert(txn_context_, tuple, &rid, table_info_);
        }
    } else {
        TINYDB_ASSERT(child_->GetOutputSchema()->Equal(*table_schema_), "Tuple schema not match");
        Tuple tmp_tuple;
        RID rid;
        while (child_->Next(&tmp_tuple)) {
            txn_manager_->Insert(txn_context_, tmp_tuple, &rid, table_info_);
        }
    }

    return false;
}

bool InsertExecutor::NextWithoutTxn(Tuple *tuple) {
    // insert executor will not return any tuple, and thus always return false
    auto node = GetPlanNode<InsertPlan>();
    if (node.IsRawInsert()) {
        RawValueInsertion();
    } else {
        NonRawValueInsertion();
    }

    return false;
}

void InsertExecutor::RawValueInsertion() {
    auto node = GetPlanNode<InsertPlan>();
    for (const auto &tuple: node.tuples_) {
        InsertTuple(tuple);
    }
}

void InsertExecutor::NonRawValueInsertion() {
    // i wonder should we only insert one tuple at a time?
    auto node = GetPlanNode<InsertPlan>();
    // let's first check whether two schema matches
    TINYDB_ASSERT(child_->GetOutputSchema()->Equal(*table_schema_), "Tuple schema not match");
    Tuple tuple;
    // sheep: should we pop out all value first to avoid endless loop?
    while (child_->Next(&tuple)) {
        InsertTuple(tuple);
    }
}

void InsertExecutor::InsertTuple(const Tuple &tuple) {
    RID rid;
    if (table_info_->table_->InsertTuple(tuple, &rid).IsOk()) {
        // insert tuple into indexes
        for (auto index_info : indexes_) {
            index_info->index_->InsertEntryTupleSchema(tuple, rid);
        }
    } else {
        THROW_UNKNOWN_TYPE_EXCEPTION("Failed to insert tuple");
    }
}

}