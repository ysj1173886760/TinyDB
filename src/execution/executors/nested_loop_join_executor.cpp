/**
 * @file nested_loop_join_executor.cpp
 * @author sheep
 * @brief implementation of nested loop join executor
 * @version 0.1
 * @date 2022-05-21
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/executors/nested_loop_join_executor.h"

namespace TinyDB {

void NestedLoopJoinExecutor::Init() {
    left_child_->Init();
    right_child_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple) {
    Tuple outerTuple;
    Tuple innerTuple;
    auto node = GetPlanNode<NestedLoopJoinPlan>();
    while (left_child_->Next(&outerTuple)) {
        while (right_child_->Next(&innerTuple)) {
            if (node.predicate_ == nullptr ||
                node.predicate_->Evaluate(&outerTuple, &innerTuple).IsTrue()) {
                uint column_count = node.GetSchema()->GetColumnCount();
                std::vector<Value> vals(column_count);
                for (uint i = 0; i < column_count; i++) {
                    vals[i] = node.value_expressions_[i]->Evaluate(&outerTuple, &innerTuple);
                }
                *tuple = Tuple(vals, node.GetSchema());
                // no need to set rid, since these tuple are temporary tuples
                return true;
            }
        }
        // reset right child
        right_child_->Init();
    }
    return false;
}

}