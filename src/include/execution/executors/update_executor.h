/**
 * @file update_executor.h
 * @author sheep
 * @brief update executor
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/abstract_executor.h"
#include "execution/plans/update_plan.h"

#include <memory>

namespace TinyDB {

/**
 * @brief 
 * UpdateExecutor, check UpdatePlan for more details.
 */
class UpdateExecutor : public AbstractExecutor {
public:
    /**
     * @brief Construct a new Update Executor object.
     * 
     * @param context execution context
     * @param node plan node corresponding to current executor
     * @param child child executor
     */
    UpdateExecutor(ExecutionContext *context, AbstractPlan *node, std::unique_ptr<AbstractExecutor> &&child)
        : AbstractExecutor(context, node),
          child_(std::move(child)) {}
        
    void Init() override;

    bool Next(Tuple *tuple) override;

private:
    // child executor
    std::unique_ptr<AbstractExecutor> child_;
    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    // cache the table schema
    Schema *table_schema_;
    // caching all the indexes that we need to insert
    std::vector<IndexInfo *> indexes_;
};

}