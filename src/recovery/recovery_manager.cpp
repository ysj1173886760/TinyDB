/**
 * @file recovery_manager.cpp
 * @author sheep
 * @brief implementation of recovery manager
 * @version 0.1
 * @date 2022-06-02
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "recovery/recovery_manager.h"
#include "recovery/log_record.h"
#include "common/macros.h"
#include "common/exception.h"
#include "storage/page/table_page.h"

namespace TinyDB {

void RecoveryManager::ARIES() {
    Scan();
    Redo();
    Undo();
}

void RecoveryManager::Scan() {
    // only perform scan(analyse) phase when we need to recover from checkpoint
}

void RecoveryManager::Redo() {
    // currently, we only support recovery from empty database, so there is no dirty page
    int offset = 0;
    while (disk_manager_->ReadLog(buffer, LOG_BUFFER_SIZE, offset)) {
        int inner_offset = 0;
        while (true) {
            // first probe the size
            uint32_t size = *reinterpret_cast<const uint32_t *>(buffer + inner_offset);
            // size = 0 means there is no more log records
            // we shall stop when buffer is empty or there is no more log records
            if (size == 0 || size + inner_offset > LOG_BUFFER_SIZE) {
                break;
            }
            auto log = LogRecord::DeserializeFrom(buffer + inner_offset);
            // remember the necessary information to retrieve log based on lsn
            lsn_mapping_[log.GetLSN()] = std::make_pair(offset + inner_offset, size);
            // redo the log if necessary
            RedoLog(log);

            inner_offset += size;
        }
        offset += inner_offset;
    }
}

void RecoveryManager::RedoLog(LogRecord &log_record) {
    // should we inline this method inside LogRecord?
    switch (log_record.type_) {
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
        active_txn_.erase(log_record.GetTxnId());
        break;
    case LogRecordType::BEGIN:
        active_txn_[log_record.GetTxnId()] = log_record.GetLSN();
        break;
    case LogRecordType::INSERT: {
        auto page = buffer_pool_manager_->FetchOrAllocatePage(log_record.GetRID().GetPageId());
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        // if this log has been persisted on disk, then we don't need to redo it
        if (table_page->GetLSN() >= log_record.GetLSN()) {
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            break;
        }

        // i think rid should be same as the one in log
        RID rid;
        if (!table_page->InsertTuple(log_record.GetNewTuple(), &rid)) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Failed to insert tuple while recovering");
        }
        TINYDB_ASSERT(rid == log_record.GetRID(), "Logic Error");
        // update in-memory lsn
        table_page->SetLSN(log_record.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
    }

    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
    case LogRecordType::UPDATE:
    case LogRecordType::INITPAGE:
    default:
        TINYDB_ASSERT(false, "Invalid Log Type");
    }
}

void RecoveryManager::Undo() {

}

}