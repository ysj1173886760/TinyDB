/**
 * @file executor_factory.cpp
 * @author sheep
 * @brief implementation of executor factory
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executor_factory.h"
#include "execution/executors/delete_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/update_executor.h"
#include "execution/executors/nested_loop_join_executor.h"

namespace TinyDB {

std::unique_ptr<AbstractExecutor> ExecutorFactory::CreateExecutor(ExecutionContext *context, AbstractPlan *node) {
    switch (node->GetType()) {
    case PlanType::SeqScanPlan: {
        return std::move(std::make_unique<SeqScanExecutor>(context, node));
    }
    case PlanType::DeletePlan: {
        auto child_executor = ExecutorFactory::CreateExecutor(context, node->GetChildAt(0));
        return std::make_unique<DeleteExecutor>(context, node, std::move(child_executor));
    }
    case PlanType::InsertPlan: {
        auto insert_plan = dynamic_cast<InsertPlan *>(node);
        if (insert_plan->IsRawInsert()) {
            return std::make_unique<InsertExecutor>(context, node, nullptr);
        } else {
            auto child_executor = ExecutorFactory::CreateExecutor(context, node->GetChildAt(0));
            return std::make_unique<InsertExecutor>(context, node, std::move(child_executor));
        }
    }
    case PlanType::UpdatePlan: {
        auto child_executor = ExecutorFactory::CreateExecutor(context, node->GetChildAt(0));
        return std::make_unique<UpdateExecutor>(context, node, std::move(child_executor));
    }
    case PlanType::NestedLoopJoinPlan: {
        auto left_child = ExecutorFactory::CreateExecutor(context, node->GetChildAt(0));
        auto right_child = ExecutorFactory::CreateExecutor(context, node->GetChildAt(1));
        return std::make_unique<NestedLoopJoinExecutor>(context, node, std::move(left_child), std::move(right_child));
    }

    default:
        TINYDB_ASSERT(false, "Invalid plan type");
    }

    UNREACHABLE("logic error");
}

}