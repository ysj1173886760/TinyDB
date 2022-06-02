/**
 * @file recovery_manager.h
 * @author sheep
 * @brief recovery manager
 * @version 0.1
 * @date 2022-06-02
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef RECOVERY_MANAGER_H
#define RECOVERY_MANAGER_H

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "recovery/log_record.h"

#include <unordered_map>

namespace TinyDB {

/**
 * @brief 
 * Just ARIES
 */
class RecoveryManager {
public:
    RecoveryManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {
        buffer = new char[LOG_BUFFER_SIZE];
    }

    ~RecoveryManager() {
        delete[] buffer;
    }
    
    /**
     * @brief 
     * Perform the recovery procedure
     */
    void ARIES();

private:
    // helper function
    void Scan();
    void Redo();
    void Undo();
    void RedoLog(LogRecord &log_record);

    // buffer used to store log data
    char *buffer{nullptr};

    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    // we need to keep track of what txn we need to undo
    // txn -> last lsn
    std::unordered_map<txn_id_t, lsn_t> active_txn_;
    // remember the offset of a log record
    // lsn -> (offset in disk, size of log)
    std::unordered_map<lsn_t, std::pair<int, int>> lsn_mapping_;
};

}

#endif