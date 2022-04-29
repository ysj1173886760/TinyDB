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
#include "storage/disk/disk_manager.h"

namespace TinyDB {

DiskManager::DiskManager(const std::string &filename)
    : filename_(filename), next_page_id_(0) {

    db_file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_file_.is_open()) {
        // is not opened, then we create it
        db_file_.clear();
        // std::ios::in will fail us when the file is not exist 
        db_file_.open(filename_, std::ios::binary | std::ios::trunc | std::ios::out);
        db_file_.close();

        // open it again
        db_file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out);
        if (!db_file_.is_open()) {
            throw std::runtime_error("failed to open db file");
        }
    }
}

DiskManager::~DiskManager() {
    if (db_file_.is_open()) {
        db_file_.close();
    }
}

page_id_t DiskManager::allocatePage() {
    // currently, simply add the page count
    // TODO: use bitmap to manage free pages
    return next_page_id_++;
}

void DiskManager::readPage(page_id_t pageId, char *data) {
    // disable this check for now, we shall add it back 
    // once we figured out how to store the metadata
    // assert(pageId < next_page_id_);

    int offset = pageId * PAGE_SIZE;
    
    // getFileSize everytime, really?
    // TODO: we need to cache it
    if (offset > getFileSize(filename_)) {
        LOG_ERROR("read past end of file");
        return;
    }

    // seekp and seekg works the same in file stream, so don't worry here
    db_file_.seekp(offset);
    db_file_.read(data, PAGE_SIZE);
    if (db_file_.bad()) {
        LOG_ERROR("I/O error while reading");
        return;
    }

    int readCount = db_file_.gcount();
    if (readCount < PAGE_SIZE) {
        LOG_DEBUG("read less than a page");
        db_file_.clear();

        // set those random data to 0
        memset(data + readCount, 0, PAGE_SIZE - readCount);
    }
}

void DiskManager::writePage(page_id_t pageId, const char *data) {
    // assert(pageId < next_page_id_);

    int offset = pageId * PAGE_SIZE;
    db_file_.seekp(offset);
    db_file_.write(data, PAGE_SIZE);

    if (db_file_.bad()) {
        LOG_ERROR("I/O error while writing");
        return;
    }

    db_file_.flush();
}

int DiskManager::getFileSize(const std::string &filename) {
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? static_cast<int> (stat_buf.st_size) : -1;
}

}


#endif