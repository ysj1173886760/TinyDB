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
    
    // LOG_INFO("txn %d acquire shared lock on %s", context->GetTxnId(), rid.ToString().c_str());

    return Result();
}

Result<> LockManager::LockExclusive(TransactionContext *txn_context, const RID &rid) {
    std::unique_lock<std::mutex> latch(latch_);
    auto context = txn_context->Cast<TwoPLContext>();

    // some assertions
    if (context->stage_ == LockStage::SHRINKING) {
        TINYDB_ASSERT(false, "Acquire lock on shrinking phase");
    }

    // acquire locks

    // first try to construct lock request queue
    if (lock_table_.count(rid) == 0) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    }

    auto *lock_queue = &lock_table_[rid];
    lock_queue->request_queue_.emplace_back(LockRequest(context->GetTxnId(), LockMode::EXCLUSIVE));
    auto it = std::prev(lock_queue->request_queue_.end());

    // wait until there is no other writer and reader
    if (lock_queue->writing_ || lock_queue->shared_count_ > 0) {
        lock_queue->cv_.wait(latch, 
            [lock_queue, context]() {
                return context->GetTxnState() ==  TransactionState::ABORTED ||
                       (!lock_queue->writing_ && lock_queue->shared_count_ == 0); });
    }

    if (context->GetTxnState() == TransactionState::ABORTED) {
        // erase the request
        lock_queue->request_queue_.erase(it);
        throw TransactionAbortException(context->GetTxnId(), "Deadlock");
    }

    context->exclusive_lock_set_->insert(rid);
    lock_queue->writing_ = true;
    it->granted_ = true;

    // LOG_INFO("txn %d acquire exclusive lock on %s", context->GetTxnId(), rid.ToString().c_str());

    return Result();
}

Result<> LockManager::LockUpgrade(TransactionContext *txn_context, const RID &rid) {
    std::unique_lock<std::mutex> latch(latch_);
    auto context = txn_context->Cast<TwoPLContext>();

    // some assertions
    if (context->stage_ == LockStage::SHRINKING) {
        TINYDB_ASSERT(false, "Acquire lock on shrinking phase");
    }

    // acquire locks

    // first try to construct lock request queue
    if (lock_table_.count(rid) == 0) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    }

    auto *lock_queue = &lock_table_[rid];

    // only one transaction can wait for upgrading the lock
    // otherwise, we will encounter deadlock situation
    if (lock_queue->upgrading_) {
        // first comes win
        context->SetAborted();
        throw TransactionAbortException(context->GetTxnId(), "Upgrade Conflict");
    }

    // find the corresponding request in queue
    auto it = lock_queue->request_queue_.begin();
    for (; it != lock_queue->request_queue_.end(); it++) {
        if (it->txn_id_ == context->GetTxnId()) {
            break;
        }
    }

    // we didn't even hold the lock, this should be the logic error
    // or lock didn't granted
    // or lock mode is not shared
    if (it == lock_queue->request_queue_.end() || 
        it->granted_ == false ||
        it->lock_mode_ != LockMode::SHARED) {
        TINYDB_ASSERT(false, "upgrade lock");
    }

    // upgrade the request
    it->granted_ = false;
    it->lock_mode_ = LockMode::EXCLUSIVE;
    lock_queue->shared_count_ -= 1;
    lock_queue->upgrading_ = true;
    context->shared_lock_set_->erase(rid);

    // wait until there is no other writer and reader
    if (lock_queue->writing_ || lock_queue->shared_count_ > 0) {
        lock_queue->cv_.wait(latch, 
            [lock_queue, context]() {
                return context->GetTxnState() ==  TransactionState::ABORTED ||
                       (!lock_queue->writing_ && lock_queue->shared_count_ == 0); });
    }

    if (context->GetTxnState() == TransactionState::ABORTED) {
        // erase the request
        lock_queue->request_queue_.erase(it);
        throw TransactionAbortException(context->GetTxnId(), "Deadlock");
    }

    context->exclusive_lock_set_->insert(rid);
    lock_queue->writing_ = true;
    lock_queue->upgrading_ = false;
    it->granted_ = true;

    return Result();
}

Result<> LockManager::Unlock(TransactionContext *txn_context, const RID &rid) {
    std::lock_guard<std::mutex> latch(latch_);
    auto context = txn_context->Cast<TwoPLContext>();

    // erase the lock
    context->shared_lock_set_->erase(rid);
    context->exclusive_lock_set_->erase(rid);
    
    auto *lock_queue = &lock_table_[rid];

    // find the request
    auto it = lock_queue->request_queue_.begin();
    for (; it != lock_queue->request_queue_.end(); it++) {
        if (it->txn_id_ == context->GetTxnId()) {
            break;
        }
    }

    // we didn't even hold the lock, this should be the logic error
    // or lock didn't granted
    // this is a logic error, we will crash the program immediately
    if (it == lock_queue->request_queue_.end() || 
        it->granted_ == false ) {
        TINYDB_ASSERT(false, "upgrade lock");
    }

    bool should_notify = false;
    if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        should_notify = true;
        lock_queue->writing_ = false;
        if (context->stage_ == LockStage::GROWING) {
            context->stage_ = LockStage::SHRINKING;
        }
    } else {
        lock_queue->shared_count_ -= 1;
        if (lock_queue->shared_count_ == 0) {
            should_notify = true;
        }

        // for read committed, we will always release the lock after reading them
        // so we won't count for it in LockStage
        if (context->isolation_level_ != IsolationLevel::READ_COMMITTED &&
            context->stage_ == LockStage::GROWING) {
            context->stage_ = LockStage::SHRINKING;
        }
    }

    // LOG_INFO("txn %d release lock on %s", context->GetTxnId(), rid.ToString().c_str());

    lock_queue->request_queue_.erase(it);

    // notify all other blocking transactions
    // because newer transactions will either wait for write to quit, or wait for all readers to quit
    if (should_notify) {
        lock_queue->cv_.notify_all();
    }

    return Result();
}

}