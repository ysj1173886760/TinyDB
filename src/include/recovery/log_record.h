/**
 * @file log_record.h
 * @author sheep
 * @brief definition of log record
 * @version 0.1
 * @date 2022-05-28
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef LOG_RECORD_H
#define LOG_RECORD_H

#include "common/config.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

#include <cstring>

namespace TinyDB {

enum class LogRecordType {
    INVALID = 0,
    // table heap related
    INSERT,
    MARKDELETE,
    APPLYDELETE,
    ROLLBACKDELETE,
    UPDATE,
    INITPAGE,
    // txn related
    BEGIN,
    COMMIT,
    ABORT,  // always keep abort the last one
};

/**
 * @brief 
 * Comments from bustub:
 * For every write operation on the table page, you should write ahead a corresponding log record.
 * 
 * For EACH log record, HEADER is like (5 fields in common, 20 bytes in total)
 * --------------------------------------------
 * | size | LSN | txnID | prevLSN | LogType |
 * --------------------------------------------
 * For insert type log record
 * --------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 * --------------------------------------------------------------
 * For delete type(including markdelete, rollbackdelete, applydelete)
 * --------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 * --------------------------------------------------------------
 * For update type log record
 * ----------------------------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
 * ----------------------------------------------------------------------------------
 * For init page type log record, i will not store prev page id since sooner doubly linked-list will be abandoned.
 * Above statement is not true, since we still need this information to set the link from prev page to current page.
 * -------------------------
 * | HEADER | prev_page_id |
 * -------------------------
 * 
 * sheep: i wonder do we need to store tuple size? for insert and delete type log since we can
 * simply derive it from total size
 */

class LogRecord {
    friend class LogManager;

public:
    LogRecord() = default;

    /**
     * @brief 
     * Create txn log record. i.e. create log record for begin, commit and abort
     * @param txn_id 
     * @param prev_lsn 
     * @param type 
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type)
        : size_(HEADER_SIZE), txn_id_(txn_id), prev_lsn_(prev_lsn), type_(type) {
        TINYDB_ASSERT(type == LogRecordType::BEGIN ||
                      type == LogRecordType::COMMIT ||
                      type == LogRecordType::ABORT, "Invalid Log Type");
    }


    /**
     * @brief
     * Constructor for init page log record
     * @param txn_id 
     * @param prev_lsn 
     * @param type 
     * @param prev_page_id 
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type, page_id_t prev_page_id)
        : txn_id_(txn_id), prev_lsn_(prev_lsn), type_(type), prev_page_id_(prev_page_id) {
        TINYDB_ASSERT(type == LogRecordType::INITPAGE, "Invalid Log Type");
        size_ = HEADER_SIZE + sizeof(page_id_t);
    }
    
    /**
     * @brief 
     * Constructor for insert/delete log record
     * @param txn_id 
     * @param prev_lsn 
     * @param type 
     * @param rid 
     * @param tuple 
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type, const RID &rid, const Tuple &tuple)
        : txn_id_(txn_id), prev_lsn_(prev_lsn), type_(type), rid_(rid) {
        TINYDB_ASSERT(type == LogRecordType::INSERT ||
                      type == LogRecordType::APPLYDELETE ||
                      type == LogRecordType::ROLLBACKDELETE ||
                      type == LogRecordType::MARKDELETE, "Invalid Log Type");
        if (type == LogRecordType::INSERT) {
            new_tuple_ = tuple;
        } else {
            old_tuple_ = tuple;
        }
        size_ = HEADER_SIZE + sizeof(RID) + sizeof(uint32_t) + tuple.GetSize();
    }

    /**
     * @brief 
     * Constructor for update log record
     * @param txn_id 
     * @param prev_lsn 
     * @param type 
     * @param rid 
     * @param old_tuple 
     * @param new_tuple 
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type, const RID &rid, const Tuple &old_tuple, const Tuple &new_tuple)
        : txn_id_(txn_id), prev_lsn_(prev_lsn), type_(type), old_tuple_(old_tuple), new_tuple_(new_tuple), rid_(rid) {
        TINYDB_ASSERT(type == LogRecordType::UPDATE, "Invalid Log Type");
        size_ = HEADER_SIZE + sizeof(RID) + sizeof(uint32_t) * 2 + old_tuple.GetSize() + new_tuple.GetSize();
    }

    ~LogRecord() = default;

    const Tuple &GetNewTuple() {
        return new_tuple_;
    }

    const Tuple &GetOldTuple() {
        return old_tuple_;
    }

    const RID &GetRID() {
        return rid_;
    }

    LogRecordType GetType() {
        return type_;
    }

    uint32_t GetSize() {
        return size_;
    }

    lsn_t GetLSN() {
        return lsn_;
    }

    lsn_t GetPrevLSN() {
        return prev_lsn_;
    }

    txn_id_t GetTxnId() {
        return txn_id_;
    }

    void SetLSN(lsn_t lsn) {
        lsn_ = lsn;
    }

    // for debug purpose
    bool operator==(const LogRecord &rhs) const {
        if (type_ != rhs.type_) {
            return false;
        }
        switch (type_) {
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
        case LogRecordType::BEGIN:
            return size_ == rhs.size_ &&
                   prev_lsn_ == rhs.prev_lsn_ &&
                   txn_id_ == rhs.txn_id_ &&
                   lsn_ == rhs.lsn_;
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE:
        case LogRecordType::MARKDELETE:
            return size_ == rhs.size_ &&
                   prev_lsn_ == rhs.prev_lsn_ &&
                   txn_id_ == rhs.txn_id_ &&
                   lsn_ == rhs.lsn_ &&
                   rid_ == rhs.rid_ &&
                   old_tuple_ == rhs.old_tuple_;
        case LogRecordType::INSERT:
            return size_ == rhs.size_ &&
                   prev_lsn_ == rhs.prev_lsn_ &&
                   txn_id_ == rhs.txn_id_ &&
                   lsn_ == rhs.lsn_ &&
                   rid_ == rhs.rid_ &&
                   new_tuple_ == rhs.new_tuple_;
        case LogRecordType::UPDATE:
            return size_ == rhs.size_ &&
                   prev_lsn_ == rhs.prev_lsn_ &&
                   txn_id_ == rhs.txn_id_ &&
                   lsn_ == rhs.lsn_ &&
                   rid_ == rhs.rid_ &&
                   old_tuple_ == rhs.old_tuple_ &&
                   new_tuple_ == rhs.new_tuple_;
        case LogRecordType::INITPAGE:
            return size_ == rhs.size_ &&
                   prev_lsn_ == rhs.prev_lsn_ &&
                   txn_id_ == rhs.txn_id_ &&
                   lsn_ == rhs.lsn_ &&
                   prev_page_id_ == rhs.prev_page_id_;
        case LogRecordType::INVALID:
            return true;
        default:
            TINYDB_ASSERT(false, "Invalid Log Type");
        }
    }

    std::string ToString() const {
        std::ostringstream os;
        os << "Log["
           << "size: " << size_ << ", "
           << "LSN: " << lsn_ << ", "
           << "txnID: " << txn_id_ << ", "
           << "prevLSN: " << prev_lsn_ << ", "
           << "LogType: " << static_cast<int>(type_) << "]";
        return os.str();
    }

    void SerializeTo(char *storage) {
        assert(type_ != LogRecordType::INVALID);
        auto serialize_header = [&]() {
            memcpy(storage, (char *)this, HEADER_SIZE);
            storage += HEADER_SIZE;
        };

        switch (type_) {
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
        case LogRecordType::BEGIN:
            // only serialize header
            serialize_header();
            break;
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE:
        case LogRecordType::MARKDELETE: {
            serialize_header();
            auto size = rid_.SerializeTo(storage);
            old_tuple_.SerializeToWithSize(storage + size);
            break;
        }
        case LogRecordType::INSERT: {
            serialize_header();
            auto size = rid_.SerializeTo(storage);
            new_tuple_.SerializeToWithSize(storage + size);
            break;
        }
        case LogRecordType::UPDATE: {
            serialize_header();
            storage += rid_.SerializeTo(storage);
            storage += old_tuple_.SerializeToWithSize(storage);
            new_tuple_.SerializeToWithSize(storage);
            break;
        }
        case LogRecordType::INITPAGE: {
            serialize_header();
            memcpy(storage, &prev_page_id_, sizeof(page_id_t));
            break;
        }
        default:
            TINYDB_ASSERT(false, "Invalid Log Type");
        }
    }

    static LogRecord DeserializeFrom(const char *storage) {
        // i wonder do we really need to store size?
        // first deserialize header
        uint32_t size = *reinterpret_cast<const uint32_t *>(storage);
        storage += sizeof(uint32_t);
        lsn_t lsn = *reinterpret_cast<const lsn_t *>(storage);
        storage += sizeof(lsn_t);
        txn_id_t txn_id = *reinterpret_cast<const txn_id_t *>(storage);
        storage += sizeof(txn_id_t);
        lsn_t prev_lsn = *reinterpret_cast<const lsn_t *>(storage);
        storage += sizeof(lsn_t);
        LogRecordType type = *reinterpret_cast<const LogRecordType *>(storage);
        storage += sizeof(LogRecordType);

        assert(type != LogRecordType::INVALID);
        // then deserialize data based on type
        // WARNING!!! don't forget to set lsn since constructor won't provide parameter to initialize lsn
        switch (type) {
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
        case LogRecordType::BEGIN: {
            auto res = LogRecord(txn_id, prev_lsn, type);
            res.lsn_ = lsn;
            TINYDB_ASSERT(size == res.GetSize(), "Deserialization LogRecord Failed");
            return res;
        }
        case LogRecordType::INSERT:
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE:
        case LogRecordType::MARKDELETE: {
            auto rid = RID::DeserializeFrom(storage);
            storage += rid.GetSerializationSize();
            auto tuple = Tuple::DeserializeFromWithSize(storage);
            auto res = LogRecord(txn_id, prev_lsn, type, rid, tuple);
            res.lsn_ = lsn;
            TINYDB_ASSERT(size == res.GetSize(), "Deserialization LogRecord Failed");
            return res;
        }
        case LogRecordType::UPDATE: {
            auto rid = RID::DeserializeFrom(storage);
            storage += rid.GetSerializationSize();
            auto old_tuple = Tuple::DeserializeFromWithSize(storage);
            storage += old_tuple.GetSerializationSize();
            auto new_tuple = Tuple::DeserializeFromWithSize(storage);
            auto res = LogRecord(txn_id, prev_lsn, type, rid, old_tuple, new_tuple);
            res.lsn_ = lsn;
            TINYDB_ASSERT(size == res.GetSize(), "Deserialization LogRecord Failed");
            return res;
        }
        case LogRecordType::INITPAGE: {
            auto prev_page_id = *reinterpret_cast<const page_id_t *>(storage);
            auto res = LogRecord(txn_id, prev_lsn, type, prev_page_id);
            res.lsn_ = lsn;
            TINYDB_ASSERT(size == res.GetSize(), "Deserialization LogRecord Failed");
            return res;
        }
        default:
            TINYDB_ASSERT(false, "Invalid Log Type");
        }
    }

    static constexpr uint32_t HEADER_SIZE = 
        sizeof(uint32_t) + sizeof(lsn_t) + sizeof(txn_id_t) + sizeof(lsn_t) + sizeof(LogRecordType);

private:
    // length of log record, for serialization
    uint32_t size_{0};
    // header
    lsn_t lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};
    lsn_t prev_lsn_{INVALID_LSN};
    LogRecordType type_{LogRecordType::INVALID};

    // for insert type log record
    // RID insert_rid_;
    // payload is new_tuple

    // for delete type log record
    // RID delete_rid_;
    // payload is old_tuple
    
    // for update type log record
    // RID update_rid_;
    Tuple old_tuple_;
    Tuple new_tuple_;
    
    // coallpse insert_rid_, delete_rid_ and update_rid_ to rid_;
    RID rid_;

    // for init page log record
    page_id_t prev_page_id_{INVALID_PAGE_ID};
};

}

#endif