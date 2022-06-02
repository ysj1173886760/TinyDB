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

Result<> TwoPLManager::Read(TransactionContext *txn_context, 
                        Tuple *tuple, 
                        const RID &rid, 
                        TableInfo *table_info,
                        const std::function<bool(const Tuple &)> &predicate) {
    TINYDB_ASSERT(txn_context->IsAborted() == false, "Trying to executing aborted transaction");
    auto context = txn_context->Cast<TwoPLContext>();

    bool is_already_locked = context->IsSharedLocked(rid);
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

    // TODO: this might still contains bugs, try to organize the logic here.
    // i.e. figure out when should we skip tuple, and when should we unlock tuple.
    if (predicate && !predicate(*tuple)) {
        // if we have predicate, and the evaluation result is false. then we can release the lock
        // we can only unlock the tuple when we are locking it. because maybe the tuple was previously locked
        // and it didn't satisfied predicate this time. then we should unlock it.
        if (!is_already_locked && context->IsSharedLocked(rid)) {
            lock_manager_->Unlock(context, rid, true);
        }
        // and we should also skip this tuple
        return Result(ErrorCode::SKIP);
    }

    // after reading, if we are read committed, then we need to release the lock
    // note that we only need to release shared lock
    if (context->isolation_level_ == IsolationLevel::READ_COMMITTED &&
        context->IsSharedLocked(rid)) {
        lock_manager_->Unlock(context, rid, true);
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

    auto res = table_info->table_->InsertTuple(tuple, rid, context, callback);
    if (res.IsErr()) {
        // we abort the transaction
        throw TransactionAbortException(context->GetTxnId(), "Failed to insert tuple");
    }
    // we should already holding the exclusive lock
    TINYDB_ASSERT(context->IsExclusiveLocked(*rid) != 0, "we should have acquired exclusive lock on new tuple");

    auto tuple_rid = *rid;
    // insert index directly, and remove these entries when we aborted
    auto indexes = table_info->GetIndexes();
    for (auto index_info : indexes) {
        index_info->index_->InsertEntryTupleSchema(tuple, tuple_rid);
    }
    // register abort action
    for (auto index_info : indexes) {
        auto index = index_info->index_.get();
        context->RegisterAbortAction([=]() {
            index->DeleteEntryTupleSchema(tuple, tuple_rid);
        });
    }

}

void TwoPLManager::Delete(TransactionContext *txn_context, const Tuple &tuple, const RID &rid, TableInfo *table_info) {
    TINYDB_ASSERT(txn_context->IsAborted() == false, "Trying to executing aborted transaction");
    auto context = txn_context->Cast<TwoPLContext>();

    if (context->IsSharedLocked(rid)) {
        // upgrade lock
        lock_manager_->LockUpgrade(txn_context, rid);
    } else if (!context->IsExclusiveLocked(rid)) {
        // otherwise, we are not holding any lock, then try to acquire the exclusive lock
        lock_manager_->LockExclusive(txn_context, rid);
    }

    // mark the tuple
    auto res = table_info->table_->MarkDelete(rid, context);

    if (res.GetErr() == ErrorCode::SKIP) {
        // skip this tuple
        // release the lock obliviously
        lock_manager_->Unlock(txn_context, rid, true);
    } else {
        // register commit action
        auto indexes = table_info->GetIndexes();
        // we shall delete entry after we've commited txn
        for (auto index_info : indexes) {
            // careful to what we are going to capture
            auto index = index_info->index_.get();
            context->RegisterCommitAction([=]() {
                index->DeleteEntryTupleSchema(tuple, rid);
            });
        }
        // delete tuple on table
        context->RegisterCommitAction([=]() {
            table_info->table_->ApplyDelete(rid, context);
        });
        // register abort action
        context->RegisterAbortAction([=]() {
            table_info->table_->RollbackDelete(rid, context);
        });
    }
}

void TwoPLManager::Update(TransactionContext *txn_context, 
                          const Tuple &old_tuple, 
                          const Tuple &new_tuple, 
                          const RID &rid, 
                          TableInfo *table_info) {
    TINYDB_ASSERT(txn_context->IsAborted() == false, "Trying to executing aborted transaction");
    auto context = txn_context->Cast<TwoPLContext>();

    if (context->IsSharedLocked(rid)) {
        // upgrade lock
        lock_manager_->LockUpgrade(txn_context, rid);
    } else if (!context->IsExclusiveLocked(rid)) {
        // otherwise, we are not holding any lock, then try to acquire the exclusive lock
        lock_manager_->LockExclusive(txn_context, rid);
    }

    auto res = table_info->table_->UpdateTuple(new_tuple, rid, context);
    if (res.GetErr() == ErrorCode::ABORT) {
        throw TransactionAbortException(context->GetTxnId(), "Failed to update");
    }

    auto indexes = table_info->GetIndexes();
    for (auto index_info : indexes) {
        auto index = index_info->index_.get();
        // insert new entry right now
        index->InsertEntryTupleSchema(new_tuple, rid);
        // only delete entry when we commits
        context->RegisterCommitAction([=]() {
            index->DeleteEntryTupleSchema(old_tuple, rid);
        });
        // delete new entry when aborts
        context->RegisterAbortAction([=] {
            index->DeleteEntryTupleSchema(new_tuple, rid);
        });
    }
    // restore the tuple when aborts
    context->RegisterAbortAction([=] {
        TINYDB_ASSERT(table_info->table_->UpdateTuple(old_tuple, rid, context).IsOk(), "Failed to update");
    });
}

TransactionContext *TwoPLManager::Begin(IsolationLevel isolation_level) {
    auto txn_id = next_txn_id_.fetch_add(1);
    TransactionContext *context = new TwoPLContext(txn_id, isolation_level);
    txn_map_->AddTransactionContext(context);

    if (log_manager_ != nullptr) {
        auto log = LogRecord(txn_id, INVALID_LSN, LogRecordType::BEGIN);
        auto lsn = log_manager_->AppendLogRecord(log);
        context->SetPrevLSN(lsn);
    }

    return context;
}

void TwoPLManager::Commit(TransactionContext *txn_context) {
    auto context = txn_context->Cast<TwoPLContext>();
    context->SetCommitted();
    
    // perform commit action
    for (auto &action : context->commit_action_) {
        action();
    }

    if (log_manager_ != nullptr) {
        auto log = LogRecord(txn_context->GetTxnId(), txn_context->GetPrevLSN(), LogRecordType::COMMIT);
        auto lsn = log_manager_->AppendLogRecord(log);
        txn_context->SetPrevLSN(lsn);
        // we need to wait until commit record has been flushed to disk.
        // i.e. Commit has been persisted
        // amortize the cost of fsync
        log_manager_->Flush(lsn, false);
    }

    // release all locks
    // sheep: we need to write commit record before we release all locks, otherwise, 
    // we might "not able to commit" a txn that has been committed during recovery
    ReleaseAllLocks(txn_context);

    // free the txn context
    txn_map_->RemoveTransactionContext(txn_context->GetTxnId());
    delete txn_context;
}

void TwoPLManager::Abort(TransactionContext *txn_context) {
    auto context = txn_context->Cast<TwoPLContext>();
    if (!context->IsAborted()) {
        context->SetAborted();
    }

    // rollback before releasing the lock
    // should we abort in reverse order?
    for (auto &action : context->abort_action_) {
        action();
    }

    if (log_manager_ != nullptr) {
        auto log = LogRecord(txn_context->GetTxnId(), txn_context->GetPrevLSN(), LogRecordType::ABORT);
        auto lsn = log_manager_->AppendLogRecord(log);
        txn_context->SetPrevLSN(lsn);
        // don't need to wait until abort log has been flushed to disk,
        // since the default behaviour for undefined txn is to abort it.
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