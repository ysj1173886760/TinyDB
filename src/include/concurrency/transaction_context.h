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

#include <functional>
#include <forward_list>
#include <vector>

namespace TinyDB {

using TxnEndAction = std::function<void()>;

enum class TransactionState {
    INVALID,
    RUNNING,
    COMMITTED,
    ABORTED,
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
    TransactionContext() = default;

    inline TransactionState GetTxnState() {
        return state_;
    }

    inline txn_id_t GetTxnId() {
        return txn_id_;
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

protected:
    // transaction state
    TransactionState state_{TransactionState::INVALID};
    // id of this transaction
    txn_id_t txn_id_{INVALID_TXN_ID};
    // actions to be executed when txn commits
    std::forward_list<TxnEndAction> commit_action_;
    // actions to be executed when txn aborts
    std::forward_list<TxnEndAction> abort_action_;
    
};

}

#endif