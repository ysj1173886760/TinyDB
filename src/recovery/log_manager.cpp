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

}