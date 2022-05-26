/**
 * @file execution_context.h
 * @author sheep
 * @brief execution context
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef EXECUTION_CONTEXT_H
#define EXECUTION_CONTEXT_H

#include "catalog/catalog.h"
#include "buffer/buffer_pool_manager.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction_context.h"

namespace TinyDB {

/**
 * @brief 
 * Execution context stores all the necessary context while executing
 * e.g. TransactionManager, Catalog, BufferPoolManager
 */
class ExecutionContext {
public:
    /**
     * @brief
     * Creates a execution context which is used in execution engine,
     * without transaction support.
     * @param catalog 
     * @param bpm 
     */
    ExecutionContext(Catalog *catalog, BufferPoolManager *bpm):
        catalog_(catalog),
        bpm_(bpm) {}

    /**
     * @brief
     * Creates a execution context which is used in execution engine, with transaction support.
     * @param catalog 
     * @param bpm 
     * @param txn_manager 
     * @param txn_context 
     */
    ExecutionContext(Catalog *catalog, BufferPoolManager *bpm, TransactionManager *txn_manager, TransactionContext *txn_context):
        catalog_(catalog),
        bpm_(bpm),
        txn_manager_(txn_manager),
        txn_context_(txn_context) {}

    // avoid implicit copy and move
    DISALLOW_COPY_AND_MOVE(ExecutionContext);

    ~ExecutionContext() = default;

    inline Catalog *GetCatalog() {
        return catalog_;
    }

    inline BufferPoolManager *GetBufferPoolManager() {
        return bpm_;
    }

    inline TransactionManager *GetTransactionManager() {
        return txn_manager_;
    }

    inline TransactionContext *GetTransactionContext() {
        return txn_context_;
    }

    void SetTransactionManager(TransactionManager *txn_manager) {
        txn_manager_ = txn_manager;
    }

    void SetTransactionContext(TransactionContext *txn_context) {
        txn_context_ = txn_context;
    }

private:
    Catalog *catalog_;
    // actually we've stored bpm in catalog
    // in order to avoid another indirection, i will store 
    // an additional pointer here
    BufferPoolManager *bpm_;
    // transaction manager
    TransactionManager *txn_manager_{nullptr};
    // transaction context for current txn
    TransactionContext *txn_context_{nullptr};
};

}

#endif