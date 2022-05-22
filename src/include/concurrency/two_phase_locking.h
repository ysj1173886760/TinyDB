/**
 * @file two_phase_locking.h
 * @author sheep
 * @brief concurrency control -- 2PL
 * @version 0.1
 * @date 2022-05-21
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager.h"

#include <memory>
#include <unordered_set>

namespace TinyDB {

/**
 * @brief 
 * transaction context for 2pl protocol
 */
class TwoPLContext : public TransactionContext {
public:
    TwoPLContext(txn_id_t txn_id, IsolationLevel isolation_level)
        : TransactionContext(txn_id, isolation_level) {}
        
private:
    // the set of shared-locked tuple held by this transaction
    std::unique_ptr<std::unordered_set<RID>> shared_lock_set_;
    // the set of exclusive-locked tuple held by this transaction
    std::unique_ptr<std::unordered_set<RID>> exclusive_lock_set_;
};

/**
 * @brief 
 * Transaction manager for 2pl protocol
 */
class TwoPLManager : public TransactionManager {
    /**
     * @brief
     * Create transaction manager for 2pl protocol
     */
    TwoPLManager()
        : TransactionManager(Protocol::TwoPL) {}
    
    ~TwoPLManager() = default;

    /**
     * @brief 
     * Perform Read
     * @param txn_context 
     * @param[out] tuple tuple that we read
     * @param[in] rid rid of tuple that we want to read
     * @param[in] table_info table metadata
     */
    void Read(TransactionContext *txn_context, Tuple *tuple, RID rid, TableInfo *table_info) override;

    /**
     * @brief 
     * Perform Insertion
     * @param txn_context 
     * @param tuple tuple that we want to insert
     * @param rid location of new tuple
     * @param[in] table_info table metadata
     */
    void Insert(TransactionContext *txn_context, const Tuple &tuple, RID *rid, TableInfo *table_info) override;

    /**
     * @brief 
     * Perform Deletion
     * @param txn_context 
     * @param rid rid of the tuple that we want to delete
     * @param[in] table_info table metadata
     */
    void Delete(TransactionContext *txn_context, RID rid, TableInfo *table_info) override;

    /**
     * @brief 
     * Perform Updation
     * @param txn_context 
     * @param tuple new tuple
     * @param rid rid of tuple that we want to update
     * @param[in] table_info table metadata
     */
    void Update(TransactionContext *txn_context, const Tuple &tuple, RID rid, TableInfo *table_info) override;

    /**
     * @brief 
     * Begin a transaction
     * @param[in] txn_context transaction context to be initialized
     * @param isolation_level isolation of this transaction
     */
    void Begin(TransactionContext *txn_context, IsolationLevel isolation_level = IsolationLevel::READ_COMMITTED) override;

    /**
     * @brief 
     * Commit a transaction
     * @param txn_context 
     */
    void Commit(TransactionContext *txn_context) override;

    /**
     * @brief 
     * Abort a transaction
     * @param txn_context 
     */
    void Abort(TransactionContext *txn_context) override;

};

}