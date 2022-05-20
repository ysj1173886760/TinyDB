/**
 * @file execution_engine.h
 * @author sheep
 * @brief execution engine
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef EXECUTION_ENGINE_H
#define EXECUTION_ENGINE_H

#include "catalog/catalog.h"
#include "execution/execution_context.h"
#include "execution/plans/abstract_plan.h"
#include "execution/executor_factory.h"

namespace TinyDB {

/**
 * @brief 
 * ExecutionEngine is used to perform the execution based on PlanTree
 */
class ExecutionEngine {
public:
    ExecutionEngine() = default;

    // i think even engines can be copied or moved, it won't cause any error
    DISALLOW_COPY_AND_MOVE(ExecutionEngine);

    void Execute(ExecutionContext *context, AbstractPlan *plan, std::vector<Tuple> *result_set) {
        auto executor = ExecutorFactory::CreateExecutor(context, plan);

        // initialize executor
        executor->Init();

        // execute
        try {
            Tuple tuple;
            while (executor->Next(&tuple)) {
                if (result_set != nullptr) {
                    result_set->push_back(tuple);
                }
            }
        } catch (std::exception &e) {
            LOG_INFO("%s", e.what());
        }
    }

};

}

#endif