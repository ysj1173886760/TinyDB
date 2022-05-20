/**
 * @file delete_plan.h
 * @author sheep
 * @brief delete plan
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef DELETE_PLAN_H
#define DELETE_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "catalog/catalog.h"

namespace TinyDB {

/**
 * @brief 
 * DeletePlan, it will delete all the tuple from child executor
 */
class DeletePlan : public AbstractPlan {
    friend class DeleteExecutor;
public:
    /**
     * @brief Construct a new Delete Plan object.
     * 
     * @param child child plan to perform the scan
     * @param table_oid table oid
     */
    DeletePlan(AbstractPlan *child, table_oid_t table_oid)
        : AbstractPlan(PlanType::DeletePlan, nullptr, {child}),
          table_oid_(table_oid) {}

private:
    table_oid_t table_oid_;
};

}

#endif