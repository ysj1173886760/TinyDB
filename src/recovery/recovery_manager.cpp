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

#include <set>

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
    int max_lsn = INVALID_LSN;
    while (disk_manager_->ReadLog(buffer_, LOG_BUFFER_SIZE, offset)) {
        int inner_offset = 0;
        while (true) {
            // first probe the size
            uint32_t size = *reinterpret_cast<const uint32_t *>(buffer_ + inner_offset);
            // size = 0 means there is no more log records
            // we shall stop when buffer is empty or there is no more log records
            if (size == 0 || size + inner_offset > LOG_BUFFER_SIZE) {
                break;
            }
            auto log = LogRecord::DeserializeFrom(buffer_ + inner_offset);
            // update max lsn
            max_lsn = std::max(max_lsn, log.GetLSN());
            // remember the necessary information to retrieve log based on lsn
            lsn_mapping_[log.GetLSN()] = std::make_pair(offset + inner_offset, size);
            // LOG_INFO("redo log %s", log.ToString().c_str());
            // redo the log if necessary
            RedoLog(log);

            inner_offset += size;
        }
        offset += inner_offset;
    }
    log_manager_->SetNextLsn(max_lsn + 1);
}

// should we inline this method inside LogRecord?
void RecoveryManager::RedoLog(LogRecord &log_record) {
    // record last lsn
    active_txn_[log_record.GetTxnId()] = log_record.GetLSN();

    switch (log_record.type_) {
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
        active_txn_.erase(log_record.GetTxnId());
        break;
    case LogRecordType::BEGIN:
        active_txn_[log_record.GetTxnId()] = log_record.GetLSN();
        break;
    case LogRecordType::INSERT: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
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
            THROW_UNKNOWN_TYPE_EXCEPTION("Unknown Failure while recovering");
        }
        TINYDB_ASSERT(rid == log_record.GetRID(), "Logic Error");
        // update in-memory lsn
        table_page->SetLSN(log_record.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
    }
    case LogRecordType::MARKDELETE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        // if this log has been persisted on disk, then we don't need to redo it
        if (table_page->GetLSN() >= log_record.GetLSN()) {
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            break;
        }
        if (!table_page->MarkDelete(log_record.GetRID())) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Unknown Failure while recovering");
        }
        // update lsn
        table_page->SetLSN(log_record.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
    }
    case LogRecordType::APPLYDELETE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        // if this log has been persisted on disk, then we don't need to redo it
        if (table_page->GetLSN() >= log_record.GetLSN()) {
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            break;
        }
        table_page->ApplyDelete(log_record.GetRID());
        // update lsn
        table_page->SetLSN(log_record.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
    }
    case LogRecordType::ROLLBACKDELETE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        // if this log has been persisted on disk, then we don't need to redo it
        if (table_page->GetLSN() >= log_record.GetLSN()) {
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            break;
        }
        table_page->RollbackDelete(log_record.GetRID());
        // update lsn
        table_page->SetLSN(log_record.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
    }
    case LogRecordType::UPDATE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        // if this log has been persisted on disk, then we don't need to redo it
        if (table_page->GetLSN() >= log_record.GetLSN()) {
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            break;
        }

        Tuple dummy_tuple;
        if (!table_page->UpdateTuple(log_record.GetNewTuple(), &dummy_tuple, log_record.GetRID())) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Unknown Failure while recovering");
        }

        // update lsn
        table_page->SetLSN(log_record.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
    }
    case LogRecordType::INITPAGE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.cur_page_id_, false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        // if this log has been persisted on disk, then we don't need to redo it
        if (table_page->GetLSN() >= log_record.GetLSN()) {
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            break;
        }

        table_page->Init(page->GetPageId(), PAGE_SIZE, log_record.prev_page_id_);
        // if current page is not the first page, then we reset the link
        if (log_record.prev_page_id_ != INVALID_PAGE_ID) {
            auto prev_page = buffer_pool_manager_->FetchPage(log_record.prev_page_id_, false);
            TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(prev_page != nullptr, "");
            auto prev_table_page = reinterpret_cast<TablePage *> (prev_page->GetData());
            // overwrite next page id to make sure the link is set
            prev_table_page->SetNextPageId(page->GetPageId());
            buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), true);
        }

        table_page->SetLSN(log_record.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
    }
    default:
        TINYDB_ASSERT(false, "Invalid Log Type");
    }
}

void RecoveryManager::Undo() {
    // abort all the active transactions
    // at every step in undo phase, we need to execute the log record with max lsn in undo-list
    LOG_INFO("Start Undo Phase, %ld txn need to be aborted", active_txn_.size());
    std::set<lsn_t> next_lsn;
    for (auto &[key, lsn]: active_txn_) {
        next_lsn.insert(lsn);
    }

    // currently, i'm just reading a log record at a time.
    // We can use batch reading for further optimization
    while (!next_lsn.empty()) {
        auto lsn = *next_lsn.rbegin();
        // first fetch the offset and size
        auto [offset, size] = lsn_mapping_[lsn];
        disk_manager_->ReadLog(buffer_, size, offset);
        // deserialize the log
        auto log = LogRecord::DeserializeFrom(buffer_);
        // undo log
        UndoLog(log);
        // erase current lsn and insert the previous lsn
        next_lsn.erase(lsn);
        if (log.prev_lsn_ != INVALID_LSN) {
            next_lsn.insert(log.prev_lsn_);
        }
    }

    // after redo phase is done, write abort record
    for (auto &[txn_id, lsn]: active_txn_) {
        auto log = LogRecord(txn_id, lsn, LogRecordType::ABORT);
        log_manager_->AppendLogRecord(log);
    }
}

void RecoveryManager::UndoLog(LogRecord &log_record) {
    // redo only, skip this log
    if (log_record.IsCLR()) {
        return;
    }

    switch (log_record.type_) {
    case LogRecordType::BEGIN:
        // do nothing
        break;
    case LogRecordType::INSERT: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        table_page->ApplyDelete(log_record.GetRID());
        // append CLR
        Tuple dummy_tuple;
        auto log = LogRecord(log_record.GetTxnId(), 
                             log_record.GetPrevLSN(), 
                             LogRecordType::APPLYDELETE, 
                             log_record.GetRID(), 
                             dummy_tuple);
        log.SetCLR();
        log_manager_->AppendLogRecord(log);
        // update in-memory lsn
        table_page->SetLSN(log.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        // update last lsn
        active_txn_[log_record.GetTxnId()] = log.GetLSN();
        break;
    }
    case LogRecordType::MARKDELETE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        table_page->RollbackDelete(log_record.GetRID());
        // append CLR
        Tuple dummy_tuple;
        auto log = LogRecord(log_record.GetTxnId(), 
                             log_record.GetPrevLSN(), 
                             LogRecordType::ROLLBACKDELETE, 
                             log_record.GetRID(), 
                             dummy_tuple);
        log.SetCLR();
        log_manager_->AppendLogRecord(log);
        // update lsn
        table_page->SetLSN(log.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        active_txn_[log_record.GetTxnId()] = log.GetLSN();
        break;
    }
    case LogRecordType::APPLYDELETE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        // this could fail. because while committing, we might delete some tuples and it will release some free space,
        // then whole database is down, and we need to rollback previous deletion, it could be the case that the free space 
        // previously released by us was used by other transactions, and they commits. So even we are holding the lock on that tuple,
        // we still might failed to insert the tuple due to the lack of space.
        // currently, as long as there is an pending "ApplyDelete" operation, we will skip this page

        // FIXME: this implementation is not robust, since i make assumption based on 2PL
        if (!table_page->InsertTupleWithRID(log_record.GetNewTuple(), log_record.GetRID())) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Unknown Failure while recovering");
        }
        // append CLR
        auto log = LogRecord(log_record.GetTxnId(), 
                             log_record.GetPrevLSN(), 
                             LogRecordType::INSERT, 
                             log_record.GetRID(), 
                             log_record.GetNewTuple());
        log.SetCLR();
        log_manager_->AppendLogRecord(log);
        // update lsn
        table_page->SetLSN(log.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        active_txn_[log_record.GetTxnId()] = log.GetLSN();
        break;
    }
    case LogRecordType::ROLLBACKDELETE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());

        if (!table_page->MarkDelete(log_record.GetRID())) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Unknown Failure while recovering");
        }
        // append CLR
        Tuple dummy_tuple;
        auto log = LogRecord(log_record.GetTxnId(), 
                             log_record.GetPrevLSN(), 
                             LogRecordType::MARKDELETE, 
                             log_record.GetRID(), 
                             dummy_tuple);
        log.SetCLR();
        log_manager_->AppendLogRecord(log);
        // update lsn
        table_page->SetLSN(log.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        active_txn_[log_record.GetTxnId()] = log.GetLSN();
        break;
    }
    case LogRecordType::UPDATE: {
        auto page = buffer_pool_manager_->FetchPage(log_record.GetRID().GetPageId(), false);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
        auto table_page = reinterpret_cast<TablePage *> (page->GetData());


        Tuple dummy_tuple;
        if (!table_page->UpdateTuple(log_record.GetOldTuple(), &dummy_tuple, log_record.GetRID())) {
            THROW_UNKNOWN_TYPE_EXCEPTION("Unknown Failure while recovering");
        }
        TINYDB_ASSERT(dummy_tuple == log_record.GetNewTuple(), "LogicError");
        // clear dummy tuple
        dummy_tuple = Tuple();
        auto log = LogRecord(log_record.GetTxnId(), 
                             log_record.GetPrevLSN(), 
                             LogRecordType::UPDATE, 
                             log_record.GetRID(), 
                             dummy_tuple, 
                             log_record.GetOldTuple());
        log.SetCLR();
        log_manager_->AppendLogRecord(log);
        // update lsn
        table_page->SetLSN(log.GetLSN());
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        active_txn_[log_record.GetTxnId()] = log.GetLSN();
        break;
    }
    case LogRecordType::INITPAGE: {
        // don't undo init page since it's metadata change
        // do nothing
        break;
    }
    default:
        TINYDB_ASSERT(false, "Invalid Log Type");
    }

}

}