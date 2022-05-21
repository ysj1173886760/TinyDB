/**
 * @file two_phase_locking.h
 * @author sheep
 * @brief concurrency control -- 2PL
 * @version 0.1
 * @date 2022-05-21
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager.h"

#include <memory>
#include <unordered_set>

namespace TinyDB {

/**
 * @brief 
 * transaction context for 2pl protocol
 */
class TwoPLContext : public TransactionContext {
public:
    TwoPLContext(txn_id_t txn_id, IsolationLevel isolation_level)
        : TransactionContext(txn_id, isolation_level) {}
        
private:
    // the set of shared-locked tuple held by this transaction
    std::unique_ptr<std::unordered_set<RID>> shared_lock_set_;
    // the set of exclusive-locked tuple held by this transaction
    std::unique_ptr<std::unordered_set<RID>> exclusive_lock_set_;
};

}