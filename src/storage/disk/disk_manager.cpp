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
#include "common/exception.h"
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
            THROW_IO_EXCEPTION(
                std::string("failed to open db file, filename: %s", filename.c_str()));
        }
    }

    // debug
    allocate_count_ = 0;
    deallocate_count_ = 0;
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
    if (offset > GetFileSize(filename_)) {
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

}


#endif