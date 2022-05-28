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
        SwapBuffer();
    }
}

void LogManager::FlushThread() {
    while (enable_flushing_.load()) {
        std::unique_lock<std::mutex> latch(latch_);
        // wait until log timeout or buffer is full or forcing log is needed
        if (!flush_cv_.wait_for(latch, log_timeout, [&] { return need_flush_ == true; })) {
            // if need flush is false, it means that we are triggered by time out.
            // thus we need to perform swapping buffer manually
            SwapBuffer();
        }
        // remember the biggest lsn in buffer
        lsn_t lsn = next_lsn_ - 1;
        // save the buffer pointer and buffer size
        uint32_t flush_size = flush_size_;
        char *flush_buffer = flush_buffer_;
        // unlock the latch since buffer has been swapped
        latch.unlock();
        // start flushing log
        disk_manager_->WriteLog(flush_buffer, flush_size);

        // store persistent_lsn
        persistent_lsn_.store(lsn);
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