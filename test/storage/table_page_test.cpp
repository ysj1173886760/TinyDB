/**
 * @file table_page_test.cpp
 * @author table page test
 * @brief 
 * @version 0.1
 * @date 2022-05-09
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/page/table_page.h"
#include "buffer/buffer_pool_manager.h"

#include <gtest/gtest.h>
#include <random>

namespace TinyDB {

// perform basic insertion, deletion, and read
TEST(TablePageTest, BasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 1;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    auto colB = Column("colB", TypeId::VARCHAR, 20);
    auto colC = Column("colC", TypeId::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);

    auto valueA = Value(TypeId::BIGINT, static_cast<int64_t> (20010310));   
    auto valueB = Value(TypeId::VARCHAR, "hello world");                    
    auto valueC = Value(TypeId::DECIMAL, 3.14159);                          
    std::vector<Value> values{valueA, valueB, valueC};

    auto valueAA = Value(TypeId::BIGINT, static_cast<int64_t> (0504));   
    auto valueBB = Value(TypeId::VARCHAR, "hello tinydb");                    
    auto valueCC = Value(TypeId::DECIMAL, 0.618);                          
    std::vector<Value> values2{valueAA, valueBB, valueCC};

    auto tuple = Tuple(values, &schema);
    auto tuple_update = Tuple(values2, &schema);

    page_id_t page0_id;
    auto raw_page0 = bpm->NewPage(&page0_id);
    auto page0 = reinterpret_cast<TablePage *> (raw_page0->GetData());
    page0->Init(page0_id, PAGE_SIZE, INVALID_PAGE_ID);
    EXPECT_EQ(page0->GetPageId(), page0_id);
    auto rid0 = RID();
    auto rid1 = RID();
    EXPECT_EQ(page0->InsertTuple(tuple, &rid0), true);
    EXPECT_EQ(page0->InsertTuple(tuple, &rid1), true);
    EXPECT_EQ(rid0.GetSlotId(), 0);
    EXPECT_EQ(rid1.GetSlotId(), 1);
    EXPECT_EQ(rid0.GetPageId(), page0_id);
    EXPECT_EQ(rid1.GetPageId(), page0_id);
    bpm->UnpinPage(page0_id, true);

    // evict the page 0 implicitly
    // insert tuple into page1
    page_id_t page1_id;
    auto raw_page1 = bpm->NewPage(&page1_id);
    auto page1 = reinterpret_cast<TablePage *> (raw_page1->GetData());
    page0->Init(page1_id, PAGE_SIZE, INVALID_PAGE_ID);
    auto rid2 = RID();
    auto rid3 = RID();
    EXPECT_EQ(page1->InsertTuple(tuple, &rid2), true);
    EXPECT_EQ(page1->InsertTuple(tuple, &rid3), true);
    EXPECT_EQ(rid2.GetSlotId(), 0);
    EXPECT_EQ(rid3.GetSlotId(), 1);
    EXPECT_EQ(rid2.GetPageId(), page1_id);
    EXPECT_EQ(rid3.GetPageId(), page1_id);
    bpm->UnpinPage(page1_id, true);

    // refetch the page 0
    raw_page0 = bpm->FetchPage(page0_id);
    page0 = reinterpret_cast<TablePage *> (raw_page0->GetData());

    auto tuple0 = Tuple();
    auto tuple1 = Tuple();
    EXPECT_EQ(page0->GetTuple(rid0, &tuple0), true);
    EXPECT_EQ(page0->GetTuple(rid1, &tuple1), true);
    EXPECT_EQ(tuple0 == tuple, true);
    EXPECT_EQ(tuple1 == tuple, true);
    EXPECT_EQ(tuple0.GetRID() == rid0, true);
    EXPECT_EQ(tuple1.GetRID() == rid1, true);
    auto first_rid = RID();
    auto second_rid = RID();
    auto tmp_rid = RID();
    EXPECT_EQ(page0->GetFirstTupleRid(&first_rid), true);
    EXPECT_EQ(page0->GetNextTupleRid(first_rid, &second_rid), true);
    EXPECT_EQ(page0->GetNextTupleRid(second_rid, &tmp_rid), false);
    EXPECT_EQ(first_rid == rid0, true);
    EXPECT_EQ(second_rid == rid1, true);

    // delete tuple in page 0
    EXPECT_EQ(page0->MarkDelete(first_rid), true);
    EXPECT_EQ(page0->MarkDelete(second_rid), true);
    // we still able see the mark-deleted tuple
    EXPECT_EQ(page0->GetFirstTupleRid(&tmp_rid), true);
    EXPECT_EQ(tmp_rid == first_rid, true);
    // We shouldn't able to update deleted tuple
    EXPECT_EQ(page0->UpdateTuple(tuple_update, &tuple0, tmp_rid), false);
    page0->RollbackDelete(tmp_rid);
    EXPECT_EQ(page0->UpdateTuple(tuple_update, &tuple0, tmp_rid), true);
    EXPECT_EQ(page0->GetTuple(tmp_rid, &tuple0), true);
    EXPECT_EQ(tuple0 == tuple_update, true);

    // delete two tuple
    page0->ApplyDelete(first_rid);
    page0->ApplyDelete(second_rid);
    // we shouldn't able to see the real deleted tuple
    EXPECT_EQ(page0->GetFirstTupleRid(&tmp_rid), false);
    bpm->UnpinPage(page0_id, true);

    raw_page1 = bpm->FetchPage(page1_id);
    page1 = reinterpret_cast<TablePage *> (raw_page1->GetData());

    auto tuple2 = Tuple();
    auto tuple3 = Tuple();
    EXPECT_EQ(page1->GetTuple(rid2, &tuple2), true);
    EXPECT_EQ(page1->GetTuple(rid3, &tuple3), true);
    EXPECT_EQ(tuple2 == tuple, true);
    EXPECT_EQ(tuple3 == tuple, true);
    EXPECT_EQ(tuple2.GetRID() == rid2, true);
    EXPECT_EQ(tuple3.GetRID() == rid3, true);

    // perform update
    EXPECT_EQ(page1->UpdateTuple(tuple_update, &tuple2, rid2), true);
    EXPECT_EQ(page1->UpdateTuple(tuple_update, &tuple3, rid3), true);
    // then we should see the updated value
    EXPECT_EQ(page1->GetTuple(rid2, &tuple2), true);
    EXPECT_EQ(page1->GetTuple(rid3, &tuple3), true);
    EXPECT_EQ(tuple2 == tuple_update, true);
    EXPECT_EQ(tuple3 == tuple_update, true);
    EXPECT_EQ(tuple2.GetRID() == rid2, true);
    EXPECT_EQ(tuple3.GetRID() == rid3, true);

    delete bpm;
    delete disk_manager;

    remove(filename.c_str());
}

}