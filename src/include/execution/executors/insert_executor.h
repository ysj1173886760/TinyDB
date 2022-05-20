/**
 * @file insert_executor.h
 * @author sheep
 * @brief insert executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "catalog/catalog.h"

#include <memory>

namespace TinyDB {

/**
 * @brief 
 * InsertExecutor. Check InsertPlan for more details
 */
class InsertExecutor : public AbstractExecutor {
public:
    InsertExecutor(ExecutionContext *context, AbstractPlan *node, std::unique_ptr<AbstractExecutor> &&child)
        : AbstractExecutor(context, node),
          child_(std::move(child)) {}

    void Init() override;

    bool Next(Tuple *tuple) override;

private:
    // helper function

    // insert tuple from raw value
    void RawValueInsertion();
    // insert tuple from child
    void NonRawValueInsertion();
    // perform insertion
    void InsertTuple(const Tuple &tuple);

    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    // cache the table schema
    Schema *table_schema_;
    // child executor, could be null when we are inserting raw values
    std::unique_ptr<AbstractExecutor> child_;
    // caching all the indexes that we need to insert
    std::vector<IndexInfo *> indexes_;
};

}