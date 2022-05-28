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
}

void LogManager::FlushThread() {
    while (enable_flushing_.load()) {
        std::unique_lock<std::mutex> latch(latch_);
        // wait until log timeout or buffer is full or forcing log is needed
        flush_cv_.wait_for(latch, log_timeout, [&] { return need_flush_ == true; });
        // TODO: wait until previous log has been flushed
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
        operation_cv_.notify_one();
        // start flushing log
        disk_manager_->WriteLog(flush_buffer, flush_size);
        // store persistent_lsn
        persistent_lsn_.store(lsn);
        // TODO: set need flush to false then wakeup the Flush call
        // i think we should use lsn to determine it
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

void LogManager::Flush(bool force) {

}

void LogManager::SwapBuffer() {
    // we are in the critical section, it's safe to exchange these two variable
    std::swap(log_buffer_, flush_buffer_);
    flush_size_ = log_size_;
    // clear log size
    log_size_ = 0;
}

}