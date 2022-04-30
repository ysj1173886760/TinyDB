/**
 * @file lru_replacer.h
 * @author sheep
 * @brief lru replacer used in buffer pool manager
 * @version 0.1
 * @date 2022-04-30
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef LRU_REPLACER_H
#define LRU_REPLACER_H

#include "buffer/replacer.h"
#include "common/config.h"

#include <list>
#include <unordered_map>
#include <mutex>

namespace TinyDB {

class LRUReplacer: public Replacer {
public:
    /**
     * @brief Construct a new LRUReplacer object
     * 
     * @param num_pages the maximum number of pages/frames the LRU replacer
     * need to store. Currently we don't care this number since the implementation
     * of LRU algorithm don't have constrains on the number of slots.
     * However, we can exploit this by implementing our own hash table
     * and use num_pages length array as slot buffer to have better cache locality
     */
    explicit LRUReplacer(size_t num_pages);

    /**
     * @brief Destroy the LRUReplacer object
     */
    ~LRUReplacer() override;

    // inherited methods from Replacer

    bool Evict(frame_id_t *victim) override;

    void Pin(frame_id_t frame_id) override;

    void Unpin(frame_id_t frame_id) override;

    size_t Size() override;

private:
    using list_t = std::list<frame_id_t>;
    list_t list_;
    std::unordered_map<frame_id_t, list_t::iterator> table_;

    // if buffer pool manager is protected by lock, then we don't need this mutex
    // however, since linux will use futex as mutex, we just need a additional atomic instruction
    // which should be cheap. because contention occurs at high-level in the lock of buffer pool manager
    std::mutex mu_;
};

}

#endif