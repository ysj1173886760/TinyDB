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

namespace TinyDB {

/**
 * @brief 
 * Concurrency control protocols.
 */
enum class Protocol {
    INVALID,
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
    TransacionManager(Protocol protocol):
        protocol_(protocol) {}
    
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
     * @param[in] table_heap table heap that will perform the actual operation
     */
    virtual void Read(TransactionContext *txn_context, Tuple *tuple, RID rid, TableHeap *table_heap) = 0;

    /**
     * @brief 
     * Perform Insertion
     * @param txn_context 
     * @param tuple tuple that we want to insert
     * @param rid location of new tuple
     * @param[in] table_heap table heap that will perform the actual operation
     */
    virtual void Insert(TransactionContext *txn_context, const Tuple &tuple, RID *rid, TableHeap *table_heap) = 0;

    /**
     * @brief 
     * Perform Deletion
     * @param txn_context 
     * @param rid rid of the tuple that we want to delete
     * @param table_heap table heap that will perform the actual operation
     */
    virtual void Delete(TransactionContext *txn_context, RID rid, TableHeap *table_heap) = 0;

    /**
     * @brief 
     * Perform Updation
     * @param txn_context 
     * @param tuple new tuple
     * @param rid rid of tuple that we want to update
     * @param table_heap table heap that will perform the actual operation
     */
    virtual void Update(TransactionContext *txn_context, cosnt Tuple &tuple, RID rid, TableHeap *table_heap) = 0;

    // sheep: i wonder do we need to provide a scan method?

private:
    // protocol of this transaction manager
    Protocol protocol_;
};

}

#endif