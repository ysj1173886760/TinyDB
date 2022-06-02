/**
 * @file disk_manager.cpp
 * @author sheep
 * @brief 
 * @version 0.1
 * @date 2021-10-20
 * 
 * @copyright Copyright (c) 2021
 * 
 */

#ifndef DISK_MANAGER_CPP
#define DISK_MANAGER_CPP

#include <string>
#include <fstream>
#include <exception>
#include <sys/stat.h>
#include <cstring>
#include <assert.h>

#include "common/logger.h"
#include "common/macros.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace TinyDB {

DiskManager::DiskManager(const std::string &filename)
    : db_name_(filename), next_page_id_(0) {
    // generate log name
    auto n = db_name_.rfind('.');
    if (n == std::string::npos) {
        THROW_IO_EXCEPTION("Wrong File Format");
        return;
    }

    log_name_ = db_name_.substr(0, n) + ".log";

    // open log file stream
    log_file_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
    if (!log_file_.is_open()) {
        // log is not opended, which means the file doesn't exist
        // then we create it
        log_file_.clear();
        // std::ios::in will fail us when the file is not exist 
        log_file_.open(log_name_, std::ios::binary | std::ios::trunc | std::ios::app | std::ios::out);
        log_file_.close();
        // reopen it with original mode
        log_file_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
        if (!log_file_.is_open()) {
            THROW_IO_EXCEPTION(
                std::string("failed to open log file, filename: %s", filename.c_str()));
        }
    }

    // open db file stream
    db_file_.open(db_name_, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_file_.is_open()) {
        // is not opened, then we create it
        db_file_.clear();
        // std::ios::in will fail us when the file is not exist 
        db_file_.open(db_name_, std::ios::binary | std::ios::trunc | std::ios::out);
        db_file_.close();

        // open it again
        db_file_.open(db_name_, std::ios::binary | std::ios::in | std::ios::out);
        if (!db_file_.is_open()) {
            THROW_IO_EXCEPTION(
                std::string("failed to open db file, filename: %s", filename.c_str()));
        }
    }

    // debug
    allocate_count_ = 0;
    deallocate_count_ = 0;

    buffer_used_ = nullptr;
}

DiskManager::~DiskManager() {
    if (db_file_.is_open()) {
        db_file_.close();
    }
}

page_id_t DiskManager::AllocatePage() {
    // TODO: use bitmap to manage free pages

    // flush a empty page to disk
    // to prevent reading past file
    // or we can flush it lazily until we write something really
    page_id_t new_page_id = next_page_id_++;
    char data[PAGE_SIZE] = {0};
    int offset = new_page_id * PAGE_SIZE;
    db_file_.seekp(offset);
    db_file_.write(data, PAGE_SIZE);

    // debug purpose
    allocate_count_++;

    return new_page_id;
}

void DiskManager::DeallocatePage(page_id_t page_id) {
    // same as above

    // debug purpose
    deallocate_count_++;
}

void DiskManager::ReadPage(page_id_t pageId, char *data) {
    // disable this check for now, we shall add it back 
    // once we figured out how to store the metadata
    // assert(pageId < next_page_id_);

    int offset = pageId * PAGE_SIZE;
    
    // getFileSize everytime, really?
    // TODO: we need to cache it
    if (offset > GetFileSize(db_name_)) {
        LOG_ERROR("read past end of file, page_id: %d", pageId);
        return;
    }

    // seekp and seekg works the same in file stream, so don't worry here
    db_file_.seekp(offset);
    db_file_.read(data, PAGE_SIZE);
    if (db_file_.bad()) {
        LOG_ERROR("I/O error while reading page %d", pageId);
        return;
    }

    uint32_t readCount = db_file_.gcount();
    if (readCount < PAGE_SIZE) {
        LOG_ERROR("read less than a page, page_id: %d", pageId);
        db_file_.clear();

        // set those random data to 0
        memset(data + readCount, 0, PAGE_SIZE - readCount);
    }
}

void DiskManager::ReadPageOrZero(page_id_t pageId, char *data) {
    int offset = pageId * PAGE_SIZE;
    
    if (offset > GetFileSize(db_name_)) {
        memset(data, 0, PAGE_SIZE);
        return;
    }

    db_file_.seekp(offset);
    db_file_.read(data, PAGE_SIZE);
    if (db_file_.bad()) {
        LOG_ERROR("I/O error while reading page %d", pageId);
        return;
    }

    uint32_t readCount = db_file_.gcount();
    if (readCount < PAGE_SIZE) {
        LOG_ERROR("read less than a page, page_id: %d", pageId);
        db_file_.clear();

        // set those random data to 0
        memset(data + readCount, 0, PAGE_SIZE - readCount);
    }
}

void DiskManager::WritePage(page_id_t pageId, const char *data) {
    // assert(pageId < next_page_id_);

    int offset = pageId * PAGE_SIZE;
    db_file_.seekp(offset);
    db_file_.write(data, PAGE_SIZE);

    if (db_file_.bad()) {
        LOG_ERROR("I/O error while writing page %d", pageId);
        return;
    }

    db_file_.flush();
}

int DiskManager::GetFileSize(const std::string &filename) {
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? static_cast<int> (stat_buf.st_size) : -1;
}

bool DiskManager::ReadLog(char *log_data, int size, int offset) {
    auto t1 = std::chrono::steady_clock::now();

    // this is stupid. we should cache the log size then read it till end
    // since log file won't change while performing recovery protocol.
    if (offset >= GetFileSize(log_name_)) {
        return false;
    }

    log_file_.seekp(offset);
    log_file_.read(log_data, size);

    if (log_file_.bad()) {
        LOG_ERROR("I/O error while reading log");
        return false;
    }

    int read_count = log_file_.gcount();
    if (read_count < size) {
        log_file_.clear();
        // pad with zero
        memset(log_data + read_count, 0, size - read_count);
    }

    auto t2 = std::chrono::steady_clock::now();
    log_read_time_ += std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    return true;
}

void DiskManager::WriteLog(char *log_data, int size) {
    // count time
    auto t1 = std::chrono::steady_clock::now();

    // deprecated
    // enforce swap log buffer
    // assert(log_data != buffer_used_);
    // buffer_used_ = log_data;

    if (size == 0) {
        return;
    }

    // append the log
    log_file_.write(log_data, size);

    // check for IO-error
    if (log_file_.bad()) {
        LOG_ERROR("I/O error while writing log");
        return;
    }

    // flush to disk
    log_file_.flush();

    auto t2 = std::chrono::steady_clock::now();
    log_write_time_ += std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    // another approach to calc elapsed time
    // std::chrono::duration<double, std::milli> fp_ms = t2 - t1;
    // LOG_INFO("%f", fp_ms.count());
}

}


#endif