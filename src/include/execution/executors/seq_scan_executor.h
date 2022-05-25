/**
 * @file seq_scan_executor.h
 * @author sheep
 * @brief seq scan executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef SEQ_SCAN_EXECUTOR_H
#define SEQ_SCAN_EXECUTOR_H

#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "catalog/catalog.h"

namespace TinyDB {

/**
 * @brief 
 * Execute a sequential scan over a table
 */
class SeqScanExecutor : public AbstractExecutor {
public:
    SeqScanExecutor(ExecutionContext *context, AbstractPlan *node)
        : AbstractExecutor(context, node) {
        TINYDB_ASSERT(node->GetType() == PlanType::SeqScanPlan, "Invalid plan type");
    }

    void Init() override;

    bool Next(Tuple *tuple) override;

private:
    // helper function

    bool NextWithTxn(Tuple *tuple);
    bool NextWithoutTxn(Tuple *tuple);

    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    // iterator used to scan table
    TableIterator iterator_;
    // cache the table schema
    Schema *table_schema_;
    // cache txn manager to avoid indirection
    TransactionManager *txn_manager_;
    // cache txn context to avoid indirection
    TransactionContext *txn_context_;
};

}

#endif