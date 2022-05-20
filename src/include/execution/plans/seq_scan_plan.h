/**
 * @file seq_scan_plan.h
 * @author sheep
 * @brief seq scan plan
 * @version 0.1
 * @date 2022-05-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef SEQ_SCAN_PLAN_H
#define SEQ_SCAN_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "catalog/catalog.h"

namespace TinyDB {

/**
 * @brief 
 * SeqScan plan. we perform the seqential scan on a table with an optional predicate
 */
class SeqScanPlan : public AbstractPlan {
public:
    /**
     * @brief Construct a new Seq Scan Plan object.
     * Since SeqScanPlan must be the leaf node, we don't need to provide child nodes here.
     * @param schema Output Schema
     * @param predicate Optional Preicate
     * @param table_oid Oid of table that we scanned
     */
    SeqScanPlan(Schema *schema, AbstractExpression *predicate, table_oid_t table_oid)
        : AbstractPlan(PlanType::SeqScanPlan, schema, {}),
          predicate_(predicate),
          table_oid_(table_oid) {}
        
    AbstractExpression *GetPredicate() const {
        return predicate_;
    }

    table_oid_t GetTableOid() const {
        return table_oid_;
    }
          
private:
    AbstractExpression *predicate_;
    table_oid_t table_oid_;
};

}

#endif