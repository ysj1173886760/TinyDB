/**
 * @file transaction_manager.cpp
 * @author sheep
 * @brief transaction manager
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/transaction_manager.h"
#include "concurrency/transaction_context.h"

#include <unordered_map>

// this file is only used to initialize txn_map

namespace TinyDB {

std::unordered_map<txn_id_t, TransactionContext *> TransactionManager::txn_map_ = {};

}
