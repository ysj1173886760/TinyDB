/**
 * @file delete_executor.h
 * @author sheep
 * @brief delete executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/plans/delete_plan.h"
#include "execution/executors/abstract_executor.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager.h"

namespace TinyDB {

class DeleteExecutor : public AbstractExecutor {
public:
    /**
     * @brief Construct a new Delete Executor object
     * 
     * @param context execution context
     * @param node corresponding plan node
     * @param child child executor
     */
    DeleteExecutor(ExecutionContext *context, AbstractPlan *node, std::unique_ptr<AbstractExecutor> &&child)
        : AbstractExecutor(context, node),
          child_(std::move(child)) {}

    void Init() override;

    bool Next(Tuple *tuple) override;

private:
    // helper function
    // calling next with txn support
    bool NextWithTxn(Tuple *tuple);
    // calling next without txn support
    bool NextWithoutTxn(Tuple *tuple);

    // child executor
    std::unique_ptr<AbstractExecutor> child_;
    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    // cache the table schema
    Schema *table_schema_;
    // caching all the indexes that we need to insert
    std::vector<IndexInfo *> indexes_;
    // cache txn manager to avoid indirection
    TransactionManager *txn_manager_;
    // cache txn context to avoid indirection
    TransactionContext *txn_context_;
};

}