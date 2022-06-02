/**
 * @file buffer_pool_manager.h
 * @author sheep
 * @brief simple buffer pool manager that very likely to become the bottleneck
 * @version 0.1
 * @date 2022-04-30
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef BUFFER_POOL_MANAGER_H
#define BUFFER_POOL_MANAGER_H

#include "buffer/replacer.h"
#include "storage/page/page.h"
#include "storage/disk/disk_manager.h"
#include "common/config.h"

#include <unordered_map>
#include <list>
#include <mutex>

namespace TinyDB {

class LogManager;

class BufferPoolManager {
public:
    /**
     * @brief Construct a new Buffer Pool Manager object
     * 
     * @param pool_size size of buffer pool
     * @param disk_manager disk manager
     */
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);

    /**
     * @brief Destroy the Buffer Pool Manager object
     * 
     */
    ~BufferPoolManager();

    /**
     * @brief 
     * fetch a page though page id ignoring whether it's from disk or memory
     * @param page_id 
     * @param outbound_is_error used in ReadPage in disk manager, check it for more details.
     * @return pointer pointing to corresponding page, or nullptr when we don't have more slots
     */
    Page *FetchPage(page_id_t page_id, bool outbound_is_error = false);

    /**
     * @brief 
     * unpin the page. Now it can be swapped out from memory.
     * if we've modified it, then is_dirty should set to true
     * @param page_id 
     * @param is_dirty 
     * @return true when we successfully unpined the page
     * @return false when page is not in memory, or the pin count is zero
     */
    bool UnpinPage(page_id_t page_id, bool is_dirty);

    /**
     * @brief 
     * flush the page to disk
     * @param page_id 
     * @return true when we successfully flushed the page
     * @return false when the page is not in memory
     */
    bool FlushPage(page_id_t page_id);

    /**
     * @brief 
     * create a new page in the buffer pool. return the new page id
     * @param page_id 
     * @return pointer pointing to new page, or nullptr if we don't have more space
     */
    Page *NewPage(page_id_t *page_id);

    /**
     * @brief 
     * delete the page, return it back to disk
     * @param page_id 
     * @return true when deletion succeed
     * @return false when someone is still using this page
     */
    bool DeletePage(page_id_t page_id);

    /**
     * @brief 
     * flush all pages to disk
     */
    void FlushAllPages();

    /**
     * @brief
     * return the size of buffer pool
     * @return size_t 
     */
    size_t GetPoolSize() {
        return pool_size_;
    }

    /**
     * @brief 
     * for debug purposes, it will check the refcnt of in-memory pages.
     * @return true when refcnt of all pages are zero
     */
    bool CheckPinCount();

private:
    void FlushPageHelper(frame_id_t frame_id);

    // number of pages in the buffer pool
    size_t pool_size_;
    // array of in-memory pages
    Page *pages_;
    // pointer to disk manager
    DiskManager *disk_manager_;
    // mapping from page id to frame id
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    // replacer used to find victim pages
    Replacer *replacer_;
    // list of free pages
    std::list<frame_id_t> free_list_;
    // big latch, currently it will protect whole buffer pool manager
    // i.e. no fine-grained locking
    std::mutex latch_;
    // log manager
    LogManager *log_manager_;
};

}

#endif