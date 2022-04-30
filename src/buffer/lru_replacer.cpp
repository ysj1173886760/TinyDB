/**
 * @file lru_replacer.cpp
 * @author sheep
 * @brief implementation of lru replacer
 * @version 0.1
 * @date 2022-04-30
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef LRU_REPLACER_CPP
#define LRU_REPLACER_CPP

#include <assert.h>

#include "buffer/lru_replacer.h"

namespace TinyDB {

LRUReplacer::LRUReplacer(size_t num_pages) {
    // do nothing
}

LRUReplacer::~LRUReplacer() {
    // do nothing
    // just destruct those containers in std library
}

bool LRUReplacer::Evict(frame_id_t *victim) {
    std::lock_guard<std::mutex> guard(mu_);

    if (list_.empty()) {
        return false;
    }

    // we shouldn't just evict a frame without knowing who he is
    // because that way we would leak a page
    assert(victim != nullptr);

    *victim = list_.back();
    list_.pop_back();
    table_.erase(*victim);
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mu_);

    // replacer don't own this frame
    if (!table_.count(frame_id)) {
        return;
    }

    // remove it from replacer
    list_.erase(table_[frame_id]);
    table_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mu_);

    // only first time unpin is valid
    // if we already own this frame, 
    // then this unpin operation is invalid
    if (table_.count(frame_id)) {
        return;
    }

    // most recently used one. push it to front
    list_.push_front(frame_id);
    table_[frame_id] = list_.begin();
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> guard(mu_);
    return list_.size();
}

}

#endif