/**
 * @file transaction_map.cpp
 * @author sheep
 * @brief transaction map
 * @version 0.1
 * @date 2022-05-23
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/transaction_map.h"
#include "concurrency/transaction_context.h"

namespace TinyDB {

TransactionContext *TransactionMap::GetTransactionContext(txn_id_t txn_id) {
    std::lock_guard<std::mutex> latch(latch_);
    assert(txn_map_.count(txn_id) != 0);
    return txn_map_[txn_id];
}

void TransactionMap::AddTransactionContext(TransactionContext *context) {
    std::lock_guard<std::mutex> latch(latch_);
    assert(txn_map_.count(context->GetTxnId()) == 0);
    txn_map_[context->GetTxnId()] = context;
}

void TransactionMap::RemoveTransactionContext(txn_id_t txn_id) {
    std::lock_guard<std::mutex> latch(latch_);
    assert(txn_map_.count(txn_id) != 0);
    txn_map_.erase(txn_id);
}

}