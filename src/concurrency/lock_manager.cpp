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
#include "common/config.h"

#include <queue>

namespace TinyDB {

LockManager::LockManager(DeadLockResolveProtocol resolve_protocol)
    : resolve_protocol_(resolve_protocol) {
    if (resolve_protocol_ == DeadLockResolveProtocol::DL_DETECT) {
        LOG_INFO("Deadlock Detection Thread Started...");
        enable_cycle_detection_.store(true);
        cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    }
}
    
LockManager::~LockManager() {
    if (resolve_protocol_ == DeadLockResolveProtocol::DL_DETECT) {
        enable_cycle_detection_.store(false);
        cycle_detection_thread_->join();
        delete cycle_detection_thread_;
        LOG_INFO("Deadlock Detection Thread Stopped...");
    }
}

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
            [lock_queue, context, it]() {
                return context->GetTxnState() ==  TransactionState::ABORTED ||
                       it->should_abort_ == true ||
                       !lock_queue->writing_; });
    }

    // notified by deadlock lock detection thread
    if (it->should_abort_) {
        context->SetAborted();
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
            [lock_queue, context, it]() {
                return context->GetTxnState() ==  TransactionState::ABORTED ||
                       it->should_abort_ == true ||
                       (!lock_queue->writing_ && lock_queue->shared_count_ == 0); });
    }
    
    // notified by deadlock lock detection thread
    if (it->should_abort_) {
        context->SetAborted();
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
            [lock_queue, context, it]() {
                return context->GetTxnState() ==  TransactionState::ABORTED ||
                       it->should_abort_ == true ||
                       (!lock_queue->writing_ && lock_queue->shared_count_ == 0); });
    }

    // notified by deadlock lock detection thread
    if (it->should_abort_) {
        context->SetAborted();
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

void LockManager::RunCycleDetection() {
    while (enable_cycle_detection_.load()) {
        std::this_thread::sleep_for(CYCLE_DETECTION_INTERVAL);
        {
            // record txn is waiting for what RID
            std::unordered_map<txn_id_t, RID> rid_map;
            // graph
            std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for;

            // acquire the latch first
            std::lock_guard<std::mutex> latch(latch_);
            for (const auto &[rid, lock_queue] : lock_table_) {
                for (const auto &lock_request : lock_queue.request_queue_) {
                    if (lock_request.granted_) {
                        continue;
                    }

                    // i'm waiting on rid
                    rid_map[lock_request.txn_id_] = rid;

                    for (const auto &granted_request : lock_queue.request_queue_) {
                        if (!granted_request.granted_) {
                            continue;
                        }
                        // lock_request.txn_id is waiting for granted_request.txn_id
                        waits_for[lock_request.txn_id_].push_back(granted_request.txn_id_);
                    }
                }
            }

            // LOG_INFO("print graph:");
            // for (const auto &[txn_id, list] : waits_for) {
            //     for (const auto &to : list) {
            //         LOG_INFO("%d %d", txn_id, to);
            //     }
            // }

            txn_id_t txn_id;
            while (HasCycle(&txn_id, waits_for)) {
                // if there is a cycle
                // remove this transaction in wait_for graph
                waits_for.erase(txn_id);
                for (auto &[vertex, edges]: waits_for) {
                    auto it = std::find(edges.begin(), edges.end(), txn_id);
                    if (it != edges.end()) {
                        edges.erase(it);
                    }
                }

                // notify this transaction should abort
                auto lock_queue = &lock_table_[rid_map[txn_id]];
                for (auto &request : lock_queue->request_queue_) {
                    if (request.txn_id_ == txn_id) {
                        // set to abort
                        request.should_abort_ = true;
                    }
                }
                // notify the aborted transaction
                // then it will realize it has been aborted
                // txn manager will trigger the abort call, which will release all locks
                // then deadlock is resolved
                lock_table_[rid_map[txn_id]].cv_.notify_all();
            }

        }
    }
}

bool LockManager::HasCycle(txn_id_t *txn_id, std::unordered_map<txn_id_t, std::vector<txn_id_t>> &wait_for) {
    // table for avoiding duplicated access
    std::unordered_set<txn_id_t> finished;
    // current access stack, used to detect cycle
    std::unordered_set<txn_id_t> stack;
    // flag indicate whether we've found cycle point
    bool found_cycle = false;

    std::function<void(txn_id_t)> dfs = [&](txn_id_t cur) {
        if (found_cycle || finished.count(cur) != 0) {
            return;
        }

        // push into stack
        stack.insert(cur);
        // iterate outgoing vertices
        for (const auto &to : wait_for[cur]) {
            if (stack.count(to) != 0 && !found_cycle) {
                *txn_id = to;
                found_cycle = true;
                return;
            }
            dfs(to);

            // as long as we found the cycle point, we will return immediately
            if (found_cycle) {
                return;
            }
        }

        stack.erase(cur);
        finished.insert(cur);
    };

    // iterate all vertex and detect the cycle though DFS.
    // because there might be several isolated sub-graphs
    for (const auto &cur_pair : wait_for) {
        if (finished.count(cur_pair.first) != 0) {
            continue;
        }

        stack.clear();
        dfs(cur_pair.first);
        if (found_cycle) {
            break;
        }
    }

    // use some option to enable logging in single module
    // if (found_cycle) {
    //     LOG_INFO("Found Cycle Point %d", *txn_id);
    // }
    return found_cycle;
}

}