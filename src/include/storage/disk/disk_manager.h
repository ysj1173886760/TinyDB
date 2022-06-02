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
#include <chrono>
#include <sstream>

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
     * @param outbound_is_error determine whether page out of bounds is an error. when this set to false, 
     * we will return with an zero page when page id is out of bounds.
     */
    void ReadPage(page_id_t pageId, char *data, bool outbound_is_error = true);

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

    /**
     * @brief 
     * Read log from disk. could be many log at a time
     * @param log_data 
     * @param size 
     * @param offset 
     * @return return false means we are reaching the end
     */
    bool ReadLog(char *log_data, int size, int offset);

    /**
     * @brief 
     * Flush the log buffer into disk
     * @param log_data 
     * @param size 
     */
    void WriteLog(char *log_data, int size);

    std::string GetTimeConsumption() {
        std::stringstream os;

        os << "DiskManagerTimeConsumption: "
           << "LogWrite: " << log_write_time_.count() << "ms, "
           << "LogRead: " << log_read_time_.count() << "ms, "
           << "DataWrite: " << data_write_time_.count() << "ms, "
           << "DataRead: " << data_read_time_.count() << "ms";

        return os.str();
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
    // file name for db file
    std::string db_name_;
    // file stream for db file
    std::fstream db_file_;
    // file name for log file
    std::string log_name_;
    // file stream for log file
    std::fstream log_file_;
    // id for next page
    page_id_t next_page_id_;
    // record the previous buffer we used to enforce
    // swapping buffer
    char *buffer_used_;
    // whether we are flushing the log
    // std::atomic<bool> is_flushing_;
    
    // for debug purpose
    int allocate_count_;
    int deallocate_count_;

    // for analysis
    std::chrono::milliseconds log_write_time_{0};
    std::chrono::milliseconds log_read_time_{0};
    std::chrono::milliseconds data_write_time_{0};
    std::chrono::milliseconds data_read_time_{0};

};

}

#endif