/**
 * @file update_plan.h
 * @author sheep
 * @brief update plan
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef UPDATE_PLAN_H
#define UPDATE_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"
#include "catalog/catalog.h"

namespace TinyDB {

enum UpdateType {
    Set,
    Operation
};

struct UpdateInfo {
    UpdateType type_;
    Value value_;
    AbstractExpression *expression_{nullptr};

    // we may not read the whole tuple from child executor, and the corresponding
    // index information is stored in expression tree. i.e. the "expression_" has strong relation
    // to the output schema of child executor
    // we will perform the operation on child tuple, then install the value based on the table_idx_
    // then we perform the updation on real data

    // column idx for table schema.
    uint32_t column_idx_;

    // TODO: provide an move constructor for Value type

    UpdateInfo(const Value &val, uint32_t column_idx)
        : type_(UpdateType::Set), value_(val), column_idx_(column_idx) {}

    UpdateInfo(AbstractExpression *expression, uint32_t column_idx)
        : type_(UpdateType::Operation), expression_(expression), column_idx_(column_idx) {}

    ~UpdateInfo() {};
};

/**
 * @brief 
 * Update plan node. it will read tuple from child executor, then perform the updation.
 * Update operation could either be a Set, which we need to specify the exact value to be set,
 * or it could be a Operation, e.g. a = a + 1, which we need to specify the OperationExpression. 
 * note that for OperationExpression, we don't need to specify the constant value since it will be the
 * leaf node in expression tree. i.e. constant value expression
 */
class UpdatePlan : public AbstractPlan {
    friend class UpdateExecutor;
public:
    /**
     * @brief Construct a new Update Plan object.
     * 
     * @param node child node
     * @param table_oid table oid to perform updation
     * @param update_list list that contains the information to perform updation
     */
    UpdatePlan(AbstractPlan *node, table_oid_t table_oid, std::vector<UpdateInfo> &&update_list)
        : AbstractPlan(PlanType::UpdatePlan, nullptr, {node}),
          table_oid_(table_oid),
          update_list_(std::move(update_list)) {}
    
private:
    table_oid_t table_oid_;
    std::vector<UpdateInfo> update_list_;
};

}

#endif