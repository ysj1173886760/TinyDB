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


Result<> TwoPLManager::Read(TransactionContext *txn_context, Tuple *tuple, RID rid, TableInfo *table_info) {
    TINYDB_ASSERT(txn_context->IsAborted() == false, "Trying to executing aborted transaction");
    auto context = txn_context->Cast<TwoPLContext>();

    // if we are not read_uncommitted(don't need lock)
    // and we are not holding the lock
    // then we trying to acquire the lock
    if (context->isolation_level_ != IsolationLevel::READ_UNCOMMITTED && 
        !context->IsSharedLocked(rid) &&
        !context->IsExclusiveLocked(rid)) {
        lock_manager_->LockShared(context, rid);
    }

    // there are many reasons that might lead to reading failure
    // TODO: check the detailed reason then decide whether we need to abort txn
    auto res = table_info->table_->GetTuple(rid, tuple);

    // after reading, if we are read committed, then we need to release the lock
    // note that we only need to release shared lock
    if (context->isolation_level_ == IsolationLevel::READ_COMMITTED &&
        context->IsSharedLocked(rid)) {
        lock_manager_->Unlock(context, rid);
    }

    return res;
}

void TwoPLManager::Insert(TransactionContext *txn_context, const Tuple &tuple, RID *rid, TableInfo *table_info) {
    TINYDB_ASSERT(txn_context->IsAborted() == false, "Trying to executing aborted transaction");
    auto context = txn_context->Cast<TwoPLContext>();
    // for insertion, we will first try to acquire the exclusive lock on an empty slot
    // if succeed, then we perform insertion.
    // otherwise, we may not able to acquire the lock right after we've inserted the tuple.
    // because in current implementation, we will wait on the tuple that might be deleted by another transaction 
    // to prevent tuple loss
    // sheep: above statement is not true,
    // i decide to skip maybe deleted tuple. So we might loss some tuples when their transaction has been aborted,
    // i will regard this as phantom scenario, and let intention lock(or some table-level lock) to handle it in higher
    // isolation level.
    // this would simplify our implementation a lot.

    // capture lock manager and txn context
    // make an intermediate copy, since cpp doesn't allow us to capture member variable
    auto lock_manager = lock_manager_.get();
    std::function<void(const RID &)> callback = [lock_manager, context](const RID &rid) {
        lock_manager->LockExclusive(context, rid);
    };

    auto res = table_info->table_->InsertTuple(tuple, rid, callback);
    if (res.IsErr()) {
        // we abort the transaction
        throw TransactionAbortException(context->GetTxnId(), "Failed to insert tuple");
    }
    // we should already holding the exclusive lock
    TINYDB_ASSERT(context->GetExclusiveLockSet()->count(*rid) != 0, "we should have acquired exclusive lock on new tuple");

    // insert index directly, and remove these entries when we aborted
    auto indexes = table_info->GetIndexes();
    for (auto index_info : indexes) {
        index_info->index_->InsertEntryTupleSchema(tuple, *rid);
    }
    // register abort action
    for (auto index_info : indexes) {
        auto index = index_info->index_.get();
        context->RegisterAbortAction([=]() {
            index->DeleteEntryTupleSchema(tuple, *rid);
        });
    }

}

void TwoPLManager::Delete(TransactionContext *txn_context, RID rid, TableInfo *table_info) {
    TINYDB_ASSERT(txn_context->IsAborted() == false, "Trying to executing aborted transaction");
}

void TwoPLManager::Update(TransactionContext *txn_context, const Tuple &tuple, RID rid, TableInfo *table_info) {
    TINYDB_ASSERT(txn_context->IsAborted() == false, "Trying to executing aborted transaction");
}

TransactionContext *TwoPLManager::Begin(IsolationLevel isolation_level) {
    auto txn_id = next_txn_id_.fetch_add(1);
    TransactionContext *context = new TwoPLContext(txn_id, isolation_level);
    txn_map_->AddTransactionContext(context);

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
    txn_map_->RemoveTransactionContext(txn_context->GetTxnId());
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
    txn_map_->RemoveTransactionContext(txn_context->GetTxnId());
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