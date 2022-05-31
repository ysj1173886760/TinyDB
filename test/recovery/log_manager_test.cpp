/**
 * @file log_manager_test.cpp
 * @author sheep
 * @brief log manager test
 * @version 0.1
 * @date 2022-05-31
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"

#include <memory>
#include <random>
#include <gtest/gtest.h>

namespace TinyDB {

LogRecord GenerateRandomLogRecord(LogRecordType type) {
    auto rid = RID();
    auto colA = Column("colA", TypeId::BIGINT);
    auto colB = Column("colB", TypeId::VARCHAR, 20);
    auto colC = Column("colC", TypeId::DECIMAL);
    auto schema = Schema({colA, colB, colC});
    auto valueA = Value(TypeId::BIGINT, static_cast<int64_t> (20010310));
    auto valueB = Value(TypeId::VARCHAR, "hello world");
    auto valueC = Value(TypeId::DECIMAL, 3.14159);
    auto tuple = Tuple({valueA, valueB, valueC}, &schema);

    switch (type) {
    case LogRecordType::ABORT:
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
        return LogRecord(1, 1, type);
    case LogRecordType::APPLYDELETE:
    case LogRecordType::MARKDELETE:
    case LogRecordType::ROLLBACKDELETE:
    case LogRecordType::INSERT:
        return LogRecord(1, 1, type, rid, tuple);
    case LogRecordType::UPDATE:
        return LogRecord(1, 1, type, rid, tuple, tuple);
    default:
        TINYDB_ASSERT(false, "invalid type");
    }
}

TEST(LogManagerTest, BasicFlushTest) {
    remove("test.db");
    remove("test.log");
    auto dm = new DiskManager("test.db");
    auto lm = new LogManager(dm);
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dis(1, static_cast<int>(LogRecordType::ABORT));
    int log_num = 1000;
    std::vector<LogRecord> log_list;
    // shrink the time to speed up the test
    log_timeout = std::chrono::milliseconds(300);
    
    // append the log
    for (int i = 0; i < log_num; i++) {
        auto log = GenerateRandomLogRecord(static_cast<LogRecordType>(dis(mt)));
        log_list.push_back(log);
        lm->AppendLogRecord(log_list[i]);
    }
    lsn_t max_lsn = log_list.back().GetLSN();
    // wait until all log has been flushed to disk
    lm->Flush(max_lsn, false);
    // then we stop the log manager
    delete lm;
    delete dm;

    std::vector<LogRecord> new_log_list;
    // restart it
    dm = new DiskManager("test.db");
    char log_buffer[LOG_BUFFER_SIZE];
    int offset = 0;
    // then read the log
    while (dm->ReadLog(log_buffer, LOG_BUFFER_SIZE, offset)) {
        int inner_offset = 0;
        while (true) {
            // first probe the size
            uint32_t size = *reinterpret_cast<const uint32_t *>(log_buffer + inner_offset);
            // size = 0 means there is no more log records
            // we shall stop when buffer is empty or there is no more log records
            if (size == 0 || size + inner_offset > LOG_BUFFER_SIZE) {
                break;
            }
            auto log = LogRecord::DeserializeFrom(log_buffer + inner_offset);
            inner_offset += size;
            new_log_list.push_back(log);
        }
        offset += inner_offset;
    }

    EXPECT_EQ(log_list.size(), new_log_list.size());
    for (int i = 0; i < log_num; i++) {
        EXPECT_EQ(log_list[i], new_log_list[i]);
    }

    delete dm;

    remove("test.db");
    remove("test.log");
}

}