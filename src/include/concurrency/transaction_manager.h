/**
 * @file transaction_manager.h
 * @author sheep
 * @brief transaction manager
 * @version 0.1
 * @date 2022-05-21
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include "concurrency/transaction_context.h"
#include "storage/table/tuple.h"
#include "catalog/catalog.h"

#include <atomic>

namespace TinyDB {

/**
 * @brief 
 * Concurrency control protocols.
 */
enum class Protocol {
    INVALID,
    TwoPL,
};

/**
 * @brief 
 * Abstract class for managing transactional operations.
 * transaction manager is responsible to perform the concurrency control protocols when 
 * executor is trying to execute transactional operations.
 * sheep: still unstable
 */
class TransactionManager {
public:
    /**
     * @brief Construct a new Transacion Manager object.
     * 
     * @param protocol protocol we want to use
     */
    TransactionManager(Protocol protocol)
        : protocol_(protocol) {}
    
    // i wonder should we use RID here? since RID represent the disk location and it's bound to 
    // slotted storage format.
    // maybe we should use some generic tuple location instead
    // sheep: defer the desicion when we are trying to add more storage engine. e.g. in-memory storage, kvs
    
    /**
     * @brief 
     * Perform Read
     * @param txn_context 
     * @param[out] tuple tuple that we read
     * @param[in] rid rid of tuple that we want to read
     * @param[in] table_info table metadata
     */
    virtual void Read(TransactionContext *txn_context, Tuple *tuple, RID rid, TableInfo *table_info) = 0;

    /**
     * @brief 
     * Perform Insertion
     * @param txn_context 
     * @param tuple tuple that we want to insert
     * @param rid location of new tuple
     * @param[in] table_info table metadata
     */
    virtual void Insert(TransactionContext *txn_context, const Tuple &tuple, RID *rid, TableInfo *table_info) = 0;

    /**
     * @brief 
     * Perform Deletion
     * @param txn_context 
     * @param rid rid of the tuple that we want to delete
     * @param[in] table_info table metadata
     */
    virtual void Delete(TransactionContext *txn_context, RID rid, TableInfo *table_info) = 0;

    /**
     * @brief 
     * Perform Updation
     * @param txn_context 
     * @param tuple new tuple
     * @param rid rid of tuple that we want to update
     * @param[in] table_info table metadata
     */
    virtual void Update(TransactionContext *txn_context, const Tuple &tuple, RID rid, TableInfo *table_info) = 0;

    // sheep: i wonder do we need to provide a scan method?

    /**
     * @brief 
     * Begin a transaction
     * @param[in] txn_context transaction context to be initialized
     * @param isolation_level isolation of this transaction
     */
    virtual void Begin(TransactionContext *txn_context, IsolationLevel isolation_level = IsolationLevel::READ_COMMITTED) = 0;

    /**
     * @brief 
     * Commit a transaction
     * @param txn_context 
     */
    virtual void Commit(TransactionContext *txn_context) = 0;

    /**
     * @brief 
     * Abort a transaction
     * @param txn_context 
     */
    virtual void Abort(TransactionContext *txn_context) = 0;

    inline TransactionContext *GetTransaction(txn_id_t txn_id) {
        return txn_map_[txn_id];
    }

protected:
    // protocol of this transaction manager
    Protocol protocol_;
    // transaction id to be assigned
    std::atomic<txn_id_t> next_txn_id_{0};
    // txn map
    std::unordered_map<txn_id_t, TransactionContext *> txn_map_;
};

}

#endif