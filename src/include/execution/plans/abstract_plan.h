/**
 * @file abstract_plan.h
 * @author sheep
 * @brief abstract plan
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef ABSTRACT_PLAN_H
#define ABSTRACT_PLAN_H

#include "catalog/schema.h"

#include <vector>

namespace TinyDB {

enum class PlanType {
    AbstractPlan,
    SeqScanPlan,
};

/**
 * @brief 
 * Base abstract class for plan nodes.
 */
class AbstractPlan {
public:
    AbstractPlan(PlanType type, Schema *schema, std::vector<AbstractPlan *> &&children)
        : schema_(schema), children_(std::move(children)), type_(type) {}
    
    virtual ~AbstractPlan() = default;

    Schema *GetSchema() const {
        return schema_;
    }

    /**
     * @brief
     * Get the child node though idx
     * @param idx 
     * @return AbstractExpression* 
     */
    AbstractPlan *GetChildAt(uint32_t idx) const {
        return children_[idx];
    }

    std::vector<AbstractPlan *> GetChildren() const {
        return children_;
    }

    PlanType GetType() const {
        return type_;
    }

protected:
    // out put schema for current node
    Schema *schema_;
    // children nodes
    std::vector<AbstractPlan *> children_;
    // plan type
    PlanType type_;
};

}

#endif