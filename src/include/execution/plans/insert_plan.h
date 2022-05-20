/**
 * @file insert_plan.h
 * @author sheep
 * @brief insert plan
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef INSERT_PLAN_H
#define INSERT_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"
#include "catalog/catalog.h"

namespace TinyDB {

/**
 * @brief 
 * InsertPlan is used to perform the insertion to a table. The value could be
 * "Raw Value", i.e. user specify the exact value, or it could be the output from 
 * child executor.
 * For the "Raw Value", we will let the front-end to check the value integrity and 
 * construct the final tuple before entering InsertPlanNode
 */
class InsertPlan : public AbstractPlan {
    // it's not necessary to write Getter function for private members
    friend class InsertExecutor;
public:
    /**
     * @brief Construct a new Insert Plan object.
     * This is the constructor for inserting raw values.
     * @param tuples 
     * @param table_oid 
     */
    InsertPlan(std::vector<Tuple> &&tuples, table_oid_t table_oid)
        : AbstractPlan(PlanType::InsertPlan, nullptr, {}),
          tuples_(std::move(tuples)),
          table_oid_(table_oid) {}
    /**
     * @brief Construct a new Insert Plan object.
     * This is the constructor for inserting values that is the output from child executor.
     * @param child 
     * @param table_oid 
     */
    InsertPlan(AbstractPlan *child, table_oid_t table_oid)
        : AbstractPlan(PlanType::InsertPlan, nullptr, {child}),
          table_oid_(table_oid) {}
    
    /**
     * @brief 
     * 
     * @return whether this insert plan is inserting raw value
     */
    bool IsRawInsert() {
        return GetChildren().empty();
    }

private:
    // raw value to be inserted
    std::vector<Tuple> tuples_;
    // table to be inserted to
    table_oid_t table_oid_;
};

}

#endif