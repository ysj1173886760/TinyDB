/**
 * @file nested_loop_join_plan.h
 * @author sheep
 * @brief nested loop join plan
 * @version 0.1
 * @date 2022-05-21
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef NESTED_LOOP_JOIN_PLAN_H
#define NESTED_LOOP_JOIN_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace TinyDB {

/**
 * @brief 
 * Plan used to perform nested loop join. Requires 2 child plan to generate tuple and a predicate to evaluate the
 * legality. We will simply use the cartesian product to generate tuples then use a predicate to filter them.
 */
class NestedLoopJoinPlan : public AbstractPlan {
    friend class NestedLoopJoinExecutor;
public:
    /**
     * @brief Construct a new Nested Loop Join Plan object.
     * 
     * @param schema output schema
     * @param children children plan
     * @param predicate predicate used to evaluate the legality of output tuple.
     * @param value_expressions expressions used to generate value for new tuple
     */
    NestedLoopJoinPlan(Schema *schema, std::vector<AbstractPlan *> &&children, AbstractExpression *predicate,
                       std::vector<AbstractExpression *>&& value_expressions)
        : AbstractPlan(PlanType::NestedLoopJoinPlan, schema, std::move(children)),
          predicate_(predicate),
          value_expressions_(std::move(value_expressions)) {
        TINYDB_ASSERT(predicate_->GetReturnType() == TypeId::BOOLEAN, "Invalid predicate");
        TINYDB_ASSERT(children_.size() == 2, "Wrong children size");
        // we will do the check as early as we can
        // check the expression type here
        TINYDB_ASSERT(schema_->GetColumnCount() == value_expressions_.size(), "Wrong expressions size");
        for (uint i = 0; i < schema_->GetColumnCount(); i++) {
            TINYDB_ASSERT(schema_->GetColumn(i).GetType() == value_expressions_[i]->GetReturnType(), "Type doesn't match");
        }
    }

private:
    // predicate used to evaluate the legality
    AbstractExpression *predicate_;
    // we also need something to generate new tuple based on output schema
    // and there is two approaches.
    // 1. use a list of column value expression, let say output schema has 5 columns, then
    // we can have an array contains 5 column value expression to generate the corresponding value.
    // 2. use 2 key_attrs array, then we can extract the value based on index. This is a little bit easier, 
    // but it can't deal with reordering issue.
    // Since method 2 will essentially converted to method 1, we will chose to store an array of column value expression
    std::vector<AbstractExpression *> value_expressions_;
};

}

#endif