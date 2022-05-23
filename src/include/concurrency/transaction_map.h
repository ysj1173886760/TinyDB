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
    static TransactionContext *GetTransactionContext(txn_id_t txn_id);
    static void AddTransactionContext(TransactionContext *context);
private:
    static std::unordered_map<txn_id_t, TransactionContext *> txn_map_;
    static std::mutex latch_;
};

}