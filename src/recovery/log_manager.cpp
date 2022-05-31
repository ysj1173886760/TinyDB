/**
 * @file log_manager.cpp
 * @author sheep
 * @brief log manager
 * @version 0.1
 * @date 2022-05-28
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "recovery/log_manager.h"

namespace TinyDB {


lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
    std::unique_lock<std::mutex> latch(latch_);
    if (log_record->GetSize() + log_size_ > LOG_BUFFER_SIZE) {
        // wait until we have some space to insert the log
        operation_cv_.wait(latch, [&]() { return log_record->GetSize() + log_size_ <= LOG_BUFFER_SIZE; });
    }
    
    // fetch new lsn
    lsn_t lsn = next_lsn_++;

}

void LogManager::FlushThread() {
    while (enable_flushing_.load()) {
        std::unique_lock<std::mutex> latch(latch_);
        // wait until log timeout or buffer is full or forcing log is needed
        flush_cv_.wait_for(latch, log_timeout);
        // swap buffer
        SwapBuffer();
        // remember the biggest lsn in buffer
        lsn_t lsn = next_lsn_ - 1;
        // save the buffer pointer and buffer size
        uint32_t flush_size = flush_size_;
        char *flush_buffer = flush_buffer_;
        // unlock the latch since we don't want to flush the disk while holding the lock
        latch.unlock();
        // resume the append log record operation 
        operation_cv_.notify_all();
        // start flushing log
        disk_manager_->WriteLog(flush_buffer, flush_size);
        // store persistent_lsn
        persistent_lsn_.store(lsn);
        // TODO: set need flush to false then wakeup the Flush call
        // i think we should use lsn to determine it
        // a good way to handle this would be to register future/promise into a map
        // currently, i chose to use busy waiting
    }
}

void LogManager::RunFlushThread() {
    enable_flushing_.store(true);
    flush_thread_ = new std::thread(&LogManager::FlushThread, this);
}

void LogManager::StopFlushThread() {
    enable_flushing_.store(false);
    flush_thread_->join();
    delete flush_thread_;
}

void LogManager::Flush(lsn_t lsn, bool force) {
    if (force) {
        // notify flush thread to start flushing the log
        flush_cv_.notify_one();
    }
    // busy waiting
    // sheep: a way to optimize busy waiting is to wait for sometime to prevent burning cpu while doing nothing,
    // we can use some exponential stragety. e.g. wait for 1 time unit, 2 time unit, 4 time unit ...
    while (persistent_lsn_.load() < lsn) {}
    // sheep: a better way is to use something like concurrent map<lsn, future>
    // then as long as flushing thread has updated persistent_lsn, we can simply notify all 
    // blocking thread that has lsn smaller than persistent_lsn
}

void LogManager::SwapBuffer() {
    // we are in the critical section, it's safe to exchange these two variable
    std::swap(log_buffer_, flush_buffer_);
    flush_size_ = log_size_;
    // clear log size
    log_size_ = 0;
}

}