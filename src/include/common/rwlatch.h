/**
 * @file rwlatch.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief rwlatch from bustub, thanks to andy pavlo
 * @version 0.1
 * @date 2022-04-29
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef RWLATCH_H
#define RWLATCH_H

#include <mutex>
#include <condition_variable>
#include <climits>

#include "common/macros.h"

namespace TinyDB {

class ReaderWriterLatch {
    // constants
    const uint32_t MAX_READERS = UINT_MAX;
public:
    ReaderWriterLatch() = default;
    ~ReaderWriterLatch() = default;

    DISALLOW_COPY(ReaderWriterLatch);

    /**
     * @brief 
     * acquire writer latch
     */
    void WLock() {
        std::unique_lock<std::mutex> latch(mutex_);
        if (writer_entered_) {
            // wait as a reader. i.e. race with other readers
            reader_.wait(latch, [&]() { return !writer_entered_; });
        }

        writer_entered_ = true;
        if (reader_count_ > 0) {
            // wait as writer
            writer_.wait(latch, [&]() { return reader_count_ == 0; });
        }
    }

    /**
     * @brief 
     * release writer latch
     */
    void WUnlock() {
        std::unique_lock<std::mutex> latch(mutex_);
        writer_entered_ = false;

        // noticy other readers
        reader_.notify_all();
    }

    /**
     * @brief 
     * Try to acquire the WLatch
     * @return true when we successfully acquired the lock, we will return while lock is holding
     */
    bool TryWLock() {
        std::unique_lock<std::mutex> latch(mutex_);
        if (writer_entered_ || reader_count_ > 0) {
            return false;
        }

        writer_entered_ = true;
    }

    /**
     * @brief 
     * acquire reader latch
     */
    void RLock() {
        std::unique_lock<std::mutex> latch(mutex_);
        if (writer_entered_ || reader_count_ == MAX_READERS) {
            reader_.wait(latch, [&]() {
                return writer_entered_ == false && reader_count_ < MAX_READERS; });
        }

        reader_count_ += 1;
    }

    /**
     * @brief 
     * release reader latch
     */
    void RUnlock() {
        std::unique_lock<std::mutex> latch(mutex_);
        reader_count_ -= 1;

        if (writer_entered_) {
            // if we are the last reader, then wake up writer
            if (reader_count_ == 0) {
                writer_.notify_one();
            }
        } else {
            // allow one more reader
            if (reader_count_ == MAX_READERS - 1) {
                reader_.notify_one();
            }
        }
    }

    /**
     * @brief 
     * try to acquire the RLock
     * @return true when we successfully acquired the lock
     */
    bool TryRLock() {
        std::unique_lock<std::mutex> latch(mutex_);
        if (writer_entered_ || reader_count_ == MAX_READERS) {
            return false;
        }

        reader_count_ += 1;
        return true;
    }

private:
    std::mutex mutex_;
    std::condition_variable writer_;
    std::condition_variable reader_;
    uint32_t reader_count_{0};
    bool writer_entered_{false};
};

class ReaderGuard {
public:
    ReaderGuard(ReaderWriterLatch &latch): latch_(latch) {
        latch_.RLock();
    }
    ~ReaderGuard() {
        latch_.RUnlock();
    }
private:
    ReaderWriterLatch &latch_;
};

class WriterGuard {
public:
    WriterGuard(ReaderWriterLatch &latch): latch_(latch) {
        latch_.WLock();
    }
    ~WriterGuard() {
        latch_.WUnlock();
    }
private:
    ReaderWriterLatch &latch_;
};

}

#endif