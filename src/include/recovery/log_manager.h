/**
 * @file log_manager.h
 * @author sheep
 * @brief log manager
 * @version 0.1
 * @date 2022-05-28
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef LOG_MANAGER_H
#define LOG_MANAGER_H

#include "recovery/log_record.h"
#include "storage/disk/disk_manager.h"

#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>

namespace TinyDB {

/**
 * @brief 
 * LogManager that will flush the log to disk.
 * In current implementation, even i've used two buffer to store log records, normal operation
 * would still be blocked as if they have only one buffer.
 * TODO: find a way to flush the log asynchronizly, and only block when it's necessary. i.e. both buffer is full
 */
class LogManager {
public:
    explicit LogManager(DiskManager *disk_manager)
        : next_lsn_(0), persistent_lsn_(INVALID_LSN), enable_flushing_(false), disk_manager_(disk_manager) {
        log_buffer_ = new char[LOG_BUFFER_SIZE];
        flush_buffer_ = new char[LOG_BUFFER_SIZE];
        // start running background flush thread
        RunFlushThread();
    }

    ~LogManager() {
        StopFlushThread();
        delete[] log_buffer_;
        delete[] flush_buffer_;
    }

    /**
     * @brief 
     * Append a log record to log buffer
     * @param log_record 
     * @return lsn_t 
     */
    lsn_t AppendLogRecord(LogRecord *log_record);

    void RunFlushThread();

    void StopFlushThread();

    void Flush(bool force);

private:
    // helper function
    void FlushThread();

    // next lsn to be used
    std::atomic<lsn_t> next_lsn_;
    // all log with lsn less that persistent_lsn_ has been flushed to disk
    std::atomic<lsn_t> persistent_lsn_;
    // whether background flush thread is enabled
    std::atomic<bool> enable_flushing_;

    char *log_buffer_;
    char *flush_buffer_;
    // global latch
    std::mutex latch_;
    // flush thread
    std::thread *flush_thread_;
    // disk manager
    DiskManager *disk_manager_;
    // cv used to wakeup the background thread
    std::condition_variable flush_cv_;
    // cv used to resume the execution of normal operation.
    // e.g. when log buffer is full, then AppendLogRecord will get blocked until
    // log has been flushed to disk
    std::condition_variable operation_cv_;
};
    
} // namespace TinyDB


#endif