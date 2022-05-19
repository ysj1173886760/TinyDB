/**
 * @file abstract_executor.h
 * @author sheep
 * @brief abstract executor
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef ABSTRACT_EXECUTOR_H
#define ABSTRACT_EXECUTOR_H

#include "execution/plans/abstract_plan.h"
#include "execution/execution_context.h"

namespace TinyDB {

class AbstractExecutor {
public:
    /**
     * @brief Construct a new Abstract Executor object
     * @param context 
     * @param node 
     * @param children 
     */
    AbstractExecutor(ExecutionContext *context, AbstractPlan *node, std::vector<AbstractExecutor *> &&children)
        : context_(context), node_(node), children_(std::move(children)) {}
    
    virtual ~AbstractExecutor() = default;

    /**
     * @brief 
     * Initialize this executor
     */
    virtual void Init() = 0;

    /**
     * @brief 
     * Get a tuple from child executor
     * @param tuple 
     * @param rid 
     * @return true when succeed, false when there are no more tuples
     */
    virtual bool Next(Tuple *tuple, RID *rid) = 0;

    /**
     * @brief 
     * convenience method to return plan node corresponding to this executor
     * @tparam T 
     * @return const T& 
     */
    template <class T>
    inline const T &GetPlanNode() {
        const T *node = dynamic_cast<const T *>(node_);
        TINYDB_ASSERT(node != nullptr, "invalid casting");
        return *node;
    }

protected:
    // execution context
    ExecutionContext *context_;
    // plan node corresponding to this executor
    AbstractPlan *node_;
    // child nodes
    // since we are forming tree structure in PlanNode, i wonder do we 
    // need to store these nodes in executor?
    std::vector<AbstractExecutor *> children_;
};

}

#endif