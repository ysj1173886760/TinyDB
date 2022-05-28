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

}

void LogManager::FlushThread() {
    while (enable_flushing_.load()) {
        std::unique_lock<std::mutex> latch(latch_);
        // wait until log timeout or buffer is full or forcing log is needed
        flush_cv_.wait_for(latch, log_timeout, [&] { return need_flush_ == true; });
        SwapBuffer();
        // remember the biggest lsn in buffer
        // since we are in critical section, there is no need to use atomic load
        lsn_t prevlsn = persistent_lsn_.load(std::memory_order_relaxed);
        lsn_t lsn = next_lsn_.load(std::memory_order_relaxed) - 1;
        // unlock the latch since buffer has been swapped
        latch.unlock();
        // start flushing log
        disk_manager_->WriteLog(flush_buffer_, flush_size_);
        // store persistent_lsn
        // TODO: does compare_exchange_weak suffcient?
        persistent_lsn_.compare_exchange_strong(prevlsn, lsn);
        // update flush size
        flush_size_.store(0);
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
    std::swap(log_buffer_, flush_buffer_);
    // we are in the critical section, it's safe to exchange these two variable
    // std::swap(log_size_, flush_size_);
    uint32_t tmp = log_size_.load(std::memory_order_relaxed);
    log_size_.store(flush_size_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    flush_size_.store(tmp, std::memory_order_relaxed);
}

}