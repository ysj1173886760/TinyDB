/**
 * @file seq_scan_executor.cpp
 * @author sheep
 * @brief seq scan executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace TinyDB {

void SeqScanExecutor::Init() {
    auto plan = GetPlanNode<SeqScanPlan>();
    // store table info
    table_info_ = context_->GetCatalog()->GetTable(plan.GetTableOid());
    // initialize the iterator
    iterator_ = table_info_->table_->Begin();
    table_schema_ = &table_info_->schema_;
    txn_context_ = context_->GetTransactionContext();
    txn_manager_ = context_->GetTransactionManager();
}

bool SeqScanExecutor::Next(Tuple *tuple) {
    if (!txn_manager_) {
        return NextWithoutTxn(tuple);
    } else {
        return NextWithTxn(tuple);
    }
}

bool SeqScanExecutor::NextWithoutTxn(Tuple *tuple) {
    auto plan = GetPlanNode<SeqScanPlan>();

    while (!iterator_.IsEnd()) {
        // read the tuple
        auto tmp = iterator_.Get();
        // advance the iterator
        iterator_.Advance();

        // processing tuple
        // tuple should always be valid
        // sheep: is above statement true? because we might read some invalid RID from index
        // but that won't happends in seq scan
        TINYDB_ASSERT(tmp.IsValid(), "tuple should always valid");

        // check the legality
        if (!(plan.GetPredicate() == nullptr ||
            plan.GetPredicate()->Evaluate(&tmp, nullptr).IsTrue())) {
            continue;
        }

        // generate tuple based on output schema
        // this method only support convertion the schema based on column name.
        // more generic method shoud be based on column position.
        *tuple = tmp.KeyFromTuple(table_schema_, plan.GetSchema());

        return true;
    }

    // table is consumed
    return false;
}

bool SeqScanExecutor::NextWithTxn(Tuple *tuple) {
    auto plan = GetPlanNode<SeqScanPlan>();

    Tuple tmp_tuple;
    while (!iterator_.IsEnd()) {
        // read the tuple
        auto rid = iterator_.GetRID();
        // advance the iterator
        iterator_.Advance();

        if (txn_manager_->Read(txn_context_, &tmp_tuple, rid, table_info_).IsErr()) {
            // skip this tuple
            continue;
        }

        // check the legality
        // if this tuple is not legal, should we release the ownership?
        if (!(plan.GetPredicate() == nullptr ||
            plan.GetPredicate()->Evaluate(&tmp_tuple, nullptr).IsTrue())) {
            continue;
        }

        // same as the version without txn support
        *tuple = tmp_tuple.KeyFromTuple(table_schema_, plan.GetSchema());

        return true;
    }

    return false;
}

}