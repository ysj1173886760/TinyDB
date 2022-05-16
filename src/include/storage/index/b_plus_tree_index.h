/**
 * @file b_plus_tree_index.h
 * @author sheep
 * @brief b+tree index wrapper
 * @version 0.1
 * @date 2022-05-14
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef B_PLUS_TREE_INDEX_H
#define B_PLUS_TREE_INDEX_H

#include "storage/index/b_plus_tree.h"
#include "storage/index/index.h"

namespace TinyDB {

#define BPLUSTREEINDEX_TYPE \
    BPlusTreeIndex<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndex : Index {
public:
    explicit BPlusTreeIndex(std::unique_ptr<IndexMetadata> metadata);

    void InsertEntry(const Tuple &key, RID rid) override;

    void DeleteEntry(const Tuple &key, RID rid) override;

    void ScanKey(const Tuple &key, std::vector<RID> *result) override;

    IndexIterator Begin() override;

    IndexIterator Begin(const Tuple &key) override;

private:
    BPlusTree<KeyType, ValueType, KeyComparator> tree_;
};

}

#endif