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

#include "disk_manager.h"

namespace TinyDB {
    DiskManager::DiskManager(const std::string &filename)
        : _filename(filename), _currentPageId(0) {

        _dbFile.open(_filename, std::ios::binary | std::ios::in | std::ios::out);
        if (!_dbFile.is_open()) {
            // is not opened, then we create it
            _dbFile.clear();
            // std::ios::in will fail us when the file is not exist 
            _dbFile.open(_filename, std::ios::binary | std::ios::trunc | std::ios::out);
            _dbFile.close();

            // open it again
            _dbFile.open(_filename, std::ios::binary | std::ios::in | std::ios::out);
            if (!_dbFile.is_open()) {
                throw std::runtime_error("failed to open db file");
            }
        }
    }

    DiskManager::~DiskManager() {
        if (_dbFile.is_open()) {
            _dbFile.close();
        }
    }

    page_id_t DiskManager::allocatePage() {
        // currently, simply add the page count
        return _currentPageId++;
    }

    void DiskManager::readPage(page_id_t pageId, char *data) {
        int offset = pageId * PAGE_SIZE;
        
        // getFileSize everytime, really?
        // TODO: we need to cache it
        if (offset > getFileSize(_filename)) {
            throw std::runtime_error("read past end of file");
        }

        // seekp and seekg works the same in file stream, so don't worry here
        _dbFile.seekp(offset);
        _dbFile.read(data, PAGE_SIZE);
        if (_dbFile.bad()) {
            // TODO: log here
            throw std::runtime_error("IO error while reading");
            return;
        }

        int readCount = _dbFile.gcount();
        if (readCount < PAGE_SIZE) {
            // TODO: read less than a page. Add Log here
            _dbFile.clear();
        }
    }

    void DiskManager::writePage(page_id_t pageId, const char *data) {
        int offset = pageId * PAGE_SIZE;
        _dbFile.seekp(offset);
        _dbFile.write(data, PAGE_SIZE);

        if (_dbFile.bad()) {
            // TODO: log here
            return;
        }

        _dbFile.flush();
    }

    int DiskManager::getFileSize(const std::string &filename) {
        struct stat stat_buf;
        int rc = stat(filename.c_str(), &stat_buf);
        return rc == 0 ? static_cast<int> (stat_buf.st_size) : -1;
    }

}


#endif