/**
 * @file b_plus_tree_test.cpp
 * @author sheep
 * @brief B+tree test
 * @version 0.1
 * @date 2022-05-15
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/index/b_plus_tree.h"
#include "storage/index/generic_key.h"

#include <gtest/gtest.h>


namespace TinyDB {

TEST(BPlusTreeTest, BasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 50;
    remove(filename.c_str());

    auto disk_manager = new DiskManager(filename);
    auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    auto colA = Column("colA", TypeId::BIGINT);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<8> comparator(&schema);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("basic_test", bpm, comparator);
    GenericKey<8> index_key;
    RID rid;
    BPlusTreeExecutionContext context;

    int key_num = 5000;
    std::vector<int64_t> keys(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
    }
    // insert  keys
    for (auto key : keys) {
        context.Reset();
        int64_t value = key;
        rid = RID(value);
        auto tmp = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(tmp);
        EXPECT_EQ(tree.Insert(index_key, rid, &context), true);
    }
    
    // read them
    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), true);
        EXPECT_EQ(result[0], RID(key));
    }

    // delete them
    for (uint i = 0; i < keys.size(); i++) {
        context.Reset();
        auto k = Tuple({Value(TypeId::BIGINT, keys[i])}, &schema);
        index_key.SetFromKey(k);
        tree.Remove(index_key, &context);
    }

    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(tree.GetValue(index_key, &result, &context), false);
    }

    delete disk_manager;
    delete bpm;
    remove(filename.c_str());
}

}