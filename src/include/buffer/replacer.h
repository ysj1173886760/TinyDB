/**
 * @file replacer.h
 * @author sheep
 * @brief abstract class for replacer.
 * different implementation should own it's own file
 * @version 0.1
 * @date 2022-04-30
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef REPLACER_H
#define REPLACER_H

#include "common/config.h"

namespace TinyDB {

/**
 * @brief 
 * whenever the page is not pinned, which means that page
 * can be evicited out to disk. we will put it into replacer.
 * replacer is responsible to give us a new page slot
 */
class Replacer {
public:
    Replacer() = default;
    virtual ~Replacer() = default;

    /**
     * @brief 
     * Remove the victim frame. we should use new frame to store
     * our new page.
     * I borrowed this api name from bustub and i'm thinking, is
     * "Evict" a better choice?
     * @param frame_id id of frame that was removed
     * @return true when a victim frame was found
     * @return false when failed to find a victim
     */
    virtual bool Victim(frame_id_t *frame_id) = 0;

    /**
     * @brief 
     * Pin a frame. this frame will be removed from replacer.
     * i.e. replacer can't evicit it from memory
     * @param frame_id id of frame to pin
     */
    virtual void Pin(frame_id_t frame_id) = 0;

    /**
     * @brief 
     * Unpin a frame, this frame will be added to replacer.
     * @param frame_id id of frame to unpin
     */
    virtual void Unpin(frame_id_t frame_id) = 0;

    /**
     * @brief 
     * return the number of elements in the replacer that can be victim
     * @return size_t 
     */
    virtual size_t Size() = 0;
};

}

#endif