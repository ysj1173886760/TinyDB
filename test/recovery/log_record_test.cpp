/**
 * @file log_record_test.cpp
 * @author sheep
 * @brief 
 * @version 0.1
 * @date 2022-05-31
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "recovery/log_record.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(LogRecordTest, SerializationTest) {
    char page[4096];

    {
        auto log = LogRecord(1, 1, LogRecordType::COMMIT);
        log.SerializeTo(page);
        auto new_log = LogRecord::DeserializeFrom(page);
        EXPECT_EQ(new_log, log);
    }

    auto rid = RID();
    auto colA = Column("colA", TypeId::BIGINT);
    auto colB = Column("colB", TypeId::VARCHAR, 20);
    auto colC = Column("colC", TypeId::DECIMAL);
    auto schema = Schema({colA, colB, colC});

    auto valueA = Value(TypeId::BIGINT, static_cast<int64_t> (20010310));   // 8
    auto valueB = Value(TypeId::VARCHAR, "hello world");                    // 15
    auto valueC = Value(TypeId::DECIMAL, 3.14159);                          // 8
    std::vector<Value> values{valueA, valueB, valueC};
    auto tuple = Tuple({valueA, valueB, valueC}, &schema);

    {
        auto log = LogRecord(1, 1, LogRecordType::INSERT, rid, tuple);
        log.SerializeTo(page);
        auto new_log = LogRecord::DeserializeFrom(page);
        EXPECT_EQ(new_log, log);
    }

    {
        auto log = LogRecord(1, 1, LogRecordType::ROLLBACKDELETE, rid, tuple);
        log.SerializeTo(page);
        auto new_log = LogRecord::DeserializeFrom(page);
        EXPECT_EQ(new_log, log);
    }

    {
        auto log = LogRecord(1, 1, LogRecordType::UPDATE, rid, tuple, tuple);
        log.SerializeTo(page);
        auto new_log = LogRecord::DeserializeFrom(page);
        EXPECT_EQ(new_log, log);
    }
}

}