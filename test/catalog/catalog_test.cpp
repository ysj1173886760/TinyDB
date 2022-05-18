/**
 * @file catalog_test.cpp
 * @author sheep
 * @brief 
 * @version 0.1
 * @date 2022-05-18
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "catalog/catalog.h"

#include <gtest/gtest.h>

namespace TinyDB {

TEST(CatalogTest, BasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto table_schema = Schema(cols);
    std::vector<uint32_t> key_attrs = {0};

    auto catalog = Catalog(bpm);

    auto table = catalog.CreateTable("table", table_schema);
    auto index1 = catalog.CreateIndex("index1", "table", table_schema, key_attrs, IndexType::BPlusTreeType, 8);
    auto index2 = catalog.CreateIndex("index2", "table", table_schema, key_attrs, IndexType::BPlusTreeType, 16);
    EXPECT_NE(table, nullptr);
    EXPECT_NE(index1, nullptr);
    EXPECT_NE(index2, nullptr);

    auto table_get = catalog.GetTable("table");
    EXPECT_EQ(table_get->name_, "table");

    auto index1_get = catalog.GetIndex("index1", "table");
    EXPECT_EQ(index1_get->index_->GetMetadata()->GetIndexName(), "index1");
    EXPECT_EQ(index1_get->index_->GetMetadata()->GetKeySize(), 8);
    EXPECT_EQ(index1_get->index_->GetMetadata()->GetIndexType(), IndexType::BPlusTreeType);

    auto index2_get = catalog.GetIndex("index2", "table");
    EXPECT_EQ(index2_get->index_->GetMetadata()->GetIndexName(), "index2");
    EXPECT_EQ(index2_get->index_->GetMetadata()->GetKeySize(), 16);
    EXPECT_EQ(index2_get->index_->GetMetadata()->GetIndexType(), IndexType::BPlusTreeType);

    EXPECT_EQ(catalog.GetTableIndexes("table").size(), 2);

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

}