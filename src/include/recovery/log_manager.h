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

namespace TinyDB {

class LogManager {
public:
    explicit LogManager(DiskManager *disk_manager)
        : next_lsn_(0), persistent_lsn_(INVALID_LSN), enable_logging_(false), disk_manager_(disk_manager) {
        log_buffer_ = new char[LOG_BUFFER_SIZE];
        flush_buffer_ = new char[LOG_BUFFER_SIZE];
    }

    ~LogManager() {
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
    // next lsn to be used
    std::atomic<lsn_t> next_lsn_;
    // all log with lsn less that persistent_lsn_ has been flushed to disk
    std::atomic<lsn_t> persistent_lsn_;
    // whether logging is enabled
    std::atomic<bool> enable_logging_;

    char *log_buffer_;
    char *flush_buffer_;

    std::mutex latch_;

    std::thread *flush_thread_;

    DiskManager *disk_manager_;
};
    
} // namespace TinyDB


#endif