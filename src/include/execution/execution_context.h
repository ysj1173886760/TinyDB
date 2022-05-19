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
     * Creates a execution context which is used in execution engine
     * @param catalog 
     * @param bpm 
     */
    ExecutionContext(Catalog *catalog, BufferPoolManager *bpm):
        catalog_(catalog),
        bpm_(bpm) {}

    // avoid implicit copy and move
    DISALLOW_COPY_AND_MOVE(ExecutionContext);

    ~ExecutionContext() = default;

    inline Catalog *GetCatalog() {
        return catalog_;
    }

    inline BufferPoolManager *GetBufferPoolManager() {
        return bpm_;
    }

private:
    Catalog *catalog_;
    // actually we've stored bpm in catalog
    // in order to avoid another indirection, i will store 
    // an additional pointer here
    BufferPoolManager *bpm_;
};

}

#endif