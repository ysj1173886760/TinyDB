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
#include "concurrency/lock_manager.h"
#include "common/result.h"

#include <memory>
#include <unordered_set>

namespace TinyDB {

enum class LockStage {
    GROWING,
    SHRINKING,
};

/**
 * @brief 
 * transaction context for 2pl protocol
 */
class TwoPLContext : public TransactionContext {
    friend class LockManager;
    friend class TwoPLManager;
public:
    TwoPLContext(txn_id_t txn_id, IsolationLevel isolation_level)
        : TransactionContext(txn_id, isolation_level),
          shared_lock_set_(new std::unordered_set<RID>()),
          exclusive_lock_set_(new std::unordered_set<RID>()) {}
        
    std::unordered_set<RID> *GetSharedLockSet() {
        return shared_lock_set_.get();
    }

    std::unordered_set<RID> *GetExclusiveLockSet() {
        return exclusive_lock_set_.get();
    }

    bool IsSharedLocked(const RID &rid) {
        return shared_lock_set_->count(rid) != 0;
    }

    bool IsExclusiveLocked(const RID &rid) {
        return exclusive_lock_set_->count(rid) != 0;
    }

private:
    // the set of shared-locked tuple held by this transaction
    std::unique_ptr<std::unordered_set<RID>> shared_lock_set_;
    // the set of exclusive-locked tuple held by this transaction
    std::unique_ptr<std::unordered_set<RID>> exclusive_lock_set_;
    // current locking phase
    LockStage stage_{LockStage::GROWING};
};

/**
 * @brief 
 * Transaction manager for 2pl protocol
 */
class TwoPLManager : public TransactionManager {
public:
    /**
     * @brief Create transaction manager with 2PL protocol
     * 
     * @param lock_manager lock manager
     */
    TwoPLManager(LockManager *lock_manager)
        : TransactionManager(Protocol::TwoPL),
          lock_manager_(lock_manager) {}
    
    ~TwoPLManager() = default;

    /**
     * @brief 
     * Perform Read
     * @param txn_context 
     * @param[out] tuple tuple that we read
     * @param[in] rid rid of tuple that we want to read
     * @param[in] table_info table metadata
     */
    Result<> Read(TransactionContext *txn_context, Tuple *tuple, RID rid, TableInfo *table_info) override;

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
     * @param tuple old tuple
     * @param rid rid of the tuple that we want to delete
     * @param[in] table_info table metadata
     */
    void Delete(TransactionContext *txn_context, const Tuple &tuple, RID rid, TableInfo *table_info) override;

    /**
     * @brief 
     * Perform Updation
     * @param txn_context 
     * @param tuple old tuple
     * @param new_tuple new tuple
     * @param rid rid of tuple that we want to update
     * @param[in] table_info table metadata
     */
    void Update(TransactionContext *txn_context, const Tuple &tuple, const Tuple &new_tuple, RID rid, TableInfo *table_info) override;

    /**
     * @brief 
     * Begin a transaction
     * @param isolation_level isolation of this transaction
     * @return new transaction context
     */
    virtual TransactionContext *Begin(IsolationLevel isolation_level = IsolationLevel::READ_COMMITTED) = 0;

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

private:
    // helper functions
    void ReleaseAllLocks(TransactionContext *txn_context);

private:
    // lock manager
    const std::unique_ptr<LockManager> lock_manager_;

};

}