/**
 * @file transaction_map.h
 * @author sheep
 * @brief transaction map
 * @version 0.1
 * @date 2022-05-23
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TRANSACTION_MAP_H
#define TRANSACTION_MAP_H

#include "concurrency/transaction_context.h"

#include <mutex>
#include <unordered_map>

namespace TinyDB {

class TransactionContext;

/**
 * @brief 
 * Global transaction map. I chooes to separate this part from transaction manager
 */
class TransactionMap {
public:
    TransactionMap() = default;
    ~TransactionMap() = default;

    /**
     * @brief Get the Transaction Context though txn id
     * 
     * @param txn_id 
     * @return TransactionContext* 
     */
    TransactionContext *GetTransactionContext(txn_id_t txn_id);

    /**
     * @brief 
     * Add a new transaction context
     * @param context 
     */
    void AddTransactionContext(TransactionContext *context);
    /**
     * @brief 
     * Remove a transaction from global map
     * @param txn_id 
     */
    void RemoveTransactionContext(txn_id_t txn_id);

    /**
     * @brief 
     * Check whether transaction still alive
     * @param txn_id 
     * @return true 
     * @return false 
     */
    bool IsTransactionAlive(txn_id_t txn_id);

private:
    std::unordered_map<txn_id_t, TransactionContext *> txn_map_;
    std::mutex latch_;
};

}

#endif