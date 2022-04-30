#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "common/logger.h"
#include "common/rwlatch.h"

namespace TinyDB {

class Counter {
public:
    Counter() = default;
    void Add(int num) {
        WriterGuard guard(rw_latch_);
        count_ += num;
    }
    int Read() {
        ReaderGuard guard(rw_latch_);
        return count_;
    }
private:
    int count_{0};
    ReaderWriterLatch rw_latch_;
};

TEST(RWLatchTest, ConcurrentTest) {
    Counter counter;
    counter.Add(5);
    
    const int num_threads = 100;
    std::vector<std::thread> thread_list;
    for (int i = 0; i < num_threads; i++) {
        if (i % 2 == 0) {
            thread_list.emplace_back(std::thread([&]() { counter.Add(1); }));
        } else {
            thread_list.emplace_back(std::thread([&]() { counter.Read(); }));
        }
    }

    for (int i = 0; i < num_threads; i++) {
        thread_list[i].join();
    }

    EXPECT_EQ(counter.Read(), num_threads / 2 + 5);
}

TEST(RWLatchTest, WaitTest) {
    using namespace std::chrono_literals;
    std::vector<std::thread> thread_list;
    ReaderWriterLatch latch;
    int counter = 0;

    latch.RLock();

    // concurrent reader can read the value
    thread_list.emplace_back(std::thread([&]() {
        latch.RLock();
        EXPECT_EQ(counter, 0);
        latch.RUnlock();
    }));

    // wait while holding the lock is very dangerous
    // unless you know what are you doing
    thread_list[0].join();

    // writer will get blocked
    thread_list.emplace_back(std::thread([&]() {
        latch.WLock();
        counter += 1;
        latch.WUnlock();
    }));
    
    // i decide not to introduce waiting to unit test
    // std::this_thread::sleep_for(200ms);

    // unless we release the latch, no writer can enter the critical section
    EXPECT_EQ(counter, 0);
    latch.RUnlock();

    // now writer can modify the value
    thread_list[1].join();
    EXPECT_EQ(counter, 1);
}

}