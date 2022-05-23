/**
 * @file two_phase_locking.cpp
 * @author sheep
 * @brief transaction manager for two phase locking
 * @version 0.1
 * @date 2022-05-23
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/two_phase_locking.h"
#include "concurrency/transaction_map.h"

namespace TinyDB {


void TwoPLManager::Read(TransactionContext *txn_context, Tuple *tuple, RID rid, TableInfo *table_info) {

}

void TwoPLManager::Insert(TransactionContext *txn_context, const Tuple &tuple, RID *rid, TableInfo *table_info) {

}

void TwoPLManager::Delete(TransactionContext *txn_context, RID rid, TableInfo *table_info) {

}

void TwoPLManager::Update(TransactionContext *txn_context, const Tuple &tuple, RID rid, TableInfo *table_info) {

}

TransactionContext *TwoPLManager::Begin(IsolationLevel isolation_level) {
    auto txn_id = next_txn_id_.fetch_add(1);
    TransactionContext *context = new TwoPLContext(txn_id, isolation_level);
    TransactionMap::AddTransactionContext(context);

    return context;
}

void TwoPLManager::Commit(TransactionContext *txn_context) {
    auto context = txn_context->Cast<TwoPLContext>();
    context->SetCommitted();
    
    // perform commit action
    for (auto &action : context->commit_action_) {
        action();
    }

    // release all locks
    ReleaseAllLocks(txn_context);

    // free the txn context
    TransactionMap::RemoveTransactionContext(txn_context->GetTxnId());
    delete txn_context;
}

void TwoPLManager::Abort(TransactionContext *txn_context) {
    auto context = txn_context->Cast<TwoPLContext>();
    context->SetAborted();

    // rollback before releasing the lock
    // should we abort in reverse order?
    for (auto &action : context->abort_action_) {
        action();
    }

    // release all locks
    ReleaseAllLocks(txn_context);

    // free the txn context
    TransactionMap::RemoveTransactionContext(txn_context->GetTxnId());
    delete txn_context;
}

void TwoPLManager::ReleaseAllLocks(TransactionContext *txn_context) {
    auto context = txn_context->Cast<TwoPLContext>();
    std::unordered_set<RID> lock_set;
    for (auto rid : *context->GetExclusiveLockSet()) {
        lock_set.emplace(rid);
    }
    for (auto rid : *context->GetSharedLockSet()) {
        lock_set.emplace(rid);
    }
    for (auto rid : lock_set) {
        lock_manager_->Unlock(context, rid);
    }
}

}