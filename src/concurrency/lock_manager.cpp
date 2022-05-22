/**
 * @file lock_manager.cpp
 * @author sheep
 * @brief implementation of lock manager
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_context.h"
#include "concurrency/two_phase_locking.h"
#include "common/logger.h"

namespace TinyDB {

Result<> LockManager::LockShared(TransactionContext *txn_context, const RID &rid) {
    std::unique_lock<std::mutex> latch(latch_);
    auto context = txn_context->Cast<TwoPLContext>();

    // some assertions
    if (context->stage_ == LockStage::SHRINKING) {
        // i think this is rather an coding error instead of runtime error
        TINYDB_ASSERT(false, "Acquire lock on shrinking phase");
    }

    // read uncommitted doesn't need shared lock
    if (context->isolation_level_ == IsolationLevel::READ_UNCOMMITTED) {
        TINYDB_ASSERT(false, "trying to acquire shared lock on read uncommitted isolation level");
    }

    // acquire locks

    // first try to construct lock request queue
    if (lock_table_.count(rid) == 0) {
        // use piecewise_construct to avoid ambiguous
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    }

    // find the lock queue and append the lock request
    auto *lock_queue = &lock_table_[rid];
    lock_queue->request_queue_.emplace_back(LockRequest(context->GetTxnId(), LockMode::SHARED));
    // get the reference to current lock request
    // is that rbegin also works here?
    auto it = std::prev(lock_queue->request_queue_.end());

    // if someone is holding the lock in exclusive mode, then we must wait
    if (lock_queue->writing_) {
        lock_queue->cv_.wait(latch, 
            [lock_queue, context]() {
                return context->GetTxnState() ==  TransactionState::ABORTED ||
                       !lock_queue->writing_; });
    }

    // check whether we are still alive, or we are killed by others
    if (context->GetTxnState() == TransactionState::ABORTED) {
        // erase the request
        lock_queue->request_queue_.erase(it);
        throw TransactionAbortException(context->GetTxnId(), "Deadlock");
    }

    // should we also check whether is an upgrading?

    context->shared_lock_set_->emplace(rid);
    it->granted_ = true;
    lock_queue->shared_count_ += 1;

    return Result({});
}

}