/**
 * @file disk_manager.h
 * @author sheep
 * @brief disk manager
 * @version 0.1
 * @date 2021-10-20
 * 
 * @copyright Copyright (c) 2021
 * 
 */

#ifndef DISK_MANAGER_H
#define DISK_MANAGER_H

#include <string>
#include <fstream>

#include "common/config.h"

namespace TinyDB {

class DiskManager {
public:
    /**
     * @brief Construct a new Disk Manager object
     * 
     * @param filename the file name of the database
     */
    explicit DiskManager(const std::string &filename);

    /**
     * @brief Destroy the Disk Manager object, close the file resources
     * 
     */
    ~DiskManager();

    /**
     * @brief flush the data to the page
     * 
     * @param pageId id of the page you want to write
     * @param data corresponding data you want to write
     */
    void WritePage(page_id_t pageId, const char *data);

    /**
     * @brief read the page from disk
     * 
     * @param pageId id of the page you want to read
     * @param data buffer which will store the result
     */
    void ReadPage(page_id_t pageId, char *data);

    /**
     * @brief allocate a new page
     * 
     * @return page_id_t the id of allocated page
     */
    page_id_t AllocatePage();

    /**
     * @brief 
     * Deallocate a page on disk
     * @param page_id 
     */
    void DeallocatePage(page_id_t page_id);

    inline int GetAllocateCount() {
        return allocate_count_;
    }

    inline int GetDeallocateCount() {
        return deallocate_count_;
    }
    
private:
    /**
     * @brief helper function to get file size
     * 
     * @param filename 
     * @return int size of file
     */
    int GetFileSize(const std::string &filename);

private:
    std::string filename_;
    std::fstream db_file_;
    page_id_t next_page_id_;
    
    // for debug purpose
    int allocate_count_;
    int deallocate_count_;
};

}

#endif