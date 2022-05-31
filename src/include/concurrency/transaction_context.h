/**
 * @file transaction_context.h
 * @author sheep
 * @brief transaction context
 * @version 0.1
 * @date 2022-05-21
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TRANSACTION_CONTEXT_H
#define TRANSACTION_CONTEXT_H

#include "common/config.h"
#include "common/macros.h"
#include "common/logger.h"

#include <functional>
#include <forward_list>
#include <vector>

namespace TinyDB {

using TxnEndAction = std::function<void()>;

enum class TransactionState {
    INVALID = 0,
    RUNNING = 1,
    COMMITTED = 2,
    ABORTED = 3,
};

enum class IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SNAPSHOT,
    SERIALIZABLE,
};

/**
 * @brief 
 * TransactionContext contains all the information that we need while running transaction.
 * One transaction context correspond to one transaction.
 */
class TransactionContext {
public:
    TransactionContext(txn_id_t txn_id, IsolationLevel isolation_level)
        : state_(TransactionState::RUNNING),
          txn_id_(txn_id),
          isolation_level_(isolation_level) {}
        
    virtual ~TransactionContext() {}

    inline TransactionState GetTxnState() {
        return state_;
    }

    inline txn_id_t GetTxnId() {
        return txn_id_;
    }

    inline void SetAborted() {
        assert(state_ == TransactionState::RUNNING);
        state_ = TransactionState::ABORTED;
    }

    inline bool IsAborted() {
        return state_ == TransactionState::ABORTED;
    }

    inline void SetCommitted() {
        assert(state_ == TransactionState::RUNNING);
        state_ = TransactionState::COMMITTED;
    }

    void SetPrevLSN(lsn_t prev_lsn) {
        prev_lsn_ = prev_lsn;
    }

    lsn_t GetPrevLSN() {
        return prev_lsn_;
    }

    /**
     * @brief 
     * Register the action that to be executed when txn commits
     * @param action 
     */
    void RegisterCommitAction(const TxnEndAction &action) {
        commit_action_.push_front(action);
    }

    /**
     * @brief 
     * Register the action that to be executed when txn aborts
     * @param action 
     */
    void RegisterAbortAction(const TxnEndAction &action) {
        abort_action_.push_front(action);
    }

    /**
     * @brief 
     * syntax sugar for casting txn context to it's sub-class
     * @tparam T 
     * @return T* 
     */
    template<typename T>
    T* Cast() {
        auto context = dynamic_cast<T*> (this);
        TINYDB_ASSERT(context != nullptr, "invalid casting");
        return context;
    }

protected:
    // transaction state
    TransactionState state_{TransactionState::INVALID};
    // id of this transaction
    txn_id_t txn_id_{INVALID_TXN_ID};
    // actions to be executed when txn commits
    std::forward_list<TxnEndAction> commit_action_;
    // actions to be executed when txn aborts
    std::forward_list<TxnEndAction> abort_action_;
    // isolatin level
    IsolationLevel isolation_level_;
    // lsn of the last record written by current txn
    lsn_t prev_lsn_{INVALID_LSN};
    
};

}

#endif