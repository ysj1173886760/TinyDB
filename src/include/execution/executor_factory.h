/**
 * @file executor_factory.h
 * @author sheep
 * @brief executor factory
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef EXECUTOR_FACTORY_H
#define EXECUTOR_FACTORY_H

#include "execution/executors/abstract_executor.h"
#include "execution/plans/abstract_plan.h"

#include <memory>

namespace TinyDB {

/**
 * @brief 
 * Generate executor node based on plan nodes
 */
class ExecutorFactory {
public:
    /**
     * @brief
     * Creates a new executor based on given plan node.
     * It will recursively create the executor tree based on plan node tree.
     * @param context 
     * @param plan 
     * @return std::unique_ptr<AbstractExecutor> root node
     */
    static std::unique_ptr<AbstractExecutor> CreateExecutor(ExecutionContext *context, AbstractPlan *plan);
};

}

#endif