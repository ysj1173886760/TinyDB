/**
 * @file lock_manager.h
 * @author sheep
 * @brief lock manager used for lock-based concurrency control protocol
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include "common/config.h"
#include "common/rid.h"
#include "common/result.h"

#include <list>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <vector>

namespace TinyDB {

enum class DeadLockResolveProtocol {
    DL_DETECT,
    WAIT_DIE,
    WOUND_WAIT,
};

class TransactionManager;
class TransactionContext;

/**
 * @brief 
 * LockManager is used to manage locks for lock-based concurrency control protocol
 */
class LockManager {
    enum class LockMode {
        SHARED,
        EXCLUSIVE,
    };

    // just a struct
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode mode)
            : txn_id_(txn_id), lock_mode_(mode) {}

        txn_id_t txn_id_;
        LockMode lock_mode_;
        bool granted_{false};
        // indicate whether current transaction should abort
        bool should_abort_{false};
    };

    // just a struct
    class LockRequestQueue {
    public:
        // request queue
        std::list<LockRequest> request_queue_;
        // for notifying blocked transaction on this rid
        std::condition_variable cv_;
        // whether request for upgrading
        bool upgrading_{false};
        // whether lock is held in execlusive mode
        bool writing_{false};
        // count for txn that hold shared lock
        uint32_t shared_count_{0};
    };

public:
    /**
     * @brief
     * Construct LockManager
     * @param resolve_protocol resolve protocol used to handle deadlock scenario
     */
    LockManager(DeadLockResolveProtocol resolve_protocol);
    
    ~LockManager();

    /**
     * @brief Get the deadlock resolve protocol
     * 
     * @return DeadLockResolveProtocol 
     */
    DeadLockResolveProtocol GetResolveProtocol() {
        return resolve_protocol_;
    }

    /**
     * @brief 
     * Acquire lock on RID in shared move
     * @param txn_context 
     * @param rid 
     * @return Result<> 
     */
    Result<> LockShared(TransactionContext *txn_context, const RID &rid);

    /**
     * @brief 
     * Acquire lock on RID in exclusive mode
     * @param txn_context 
     * @param rid 
     * @return Result<>
     */
    Result<> LockExclusive(TransactionContext *txn_context, const RID &rid);

    /**
     * @brief 
     * Update a lock from a shared lock to an exclusive lock
     * @param txn_context 
     * @param rid 
     * @return Result<>
     */
    Result<> LockUpgrade(TransactionContext *txn_context, const RID &rid);

    /**
     * @brief 
     * Release the lock held by the transaction
     * @param txn_context 
     * @param rid 
     * @return Result<>
     */
    Result<> Unlock(TransactionContext *txn_context, const RID &rid);

    /**
     * @brief 
     * try to acquire the exclusive lock. We will return immediately if it will block us.
     * @param txn_context 
     * @param rid 
     * @return Result<> 
     */
    Result<> TryLockExclusive(TransactionContext *txn_context, const RID &rid);

private:
    // global latch
    std::mutex latch_;
    // lock table
    std::unordered_map<RID, LockRequestQueue> lock_table_;
    // DL resolve protocol
    DeadLockResolveProtocol resolve_protocol_;

    // Deadlock resolve protocol specific

    // flag variable
    std::atomic<bool> enable_cycle_detection_{false};
    // background thread
    std::thread *cycle_detection_thread_{nullptr};
    // background thread for deadlock detection
    void RunCycleDetection();
    // run cycle detection
    bool HasCycle(txn_id_t *txn_id, std::unordered_map<txn_id_t, std::vector<txn_id_t>> &wait_for);
};

}

#endif