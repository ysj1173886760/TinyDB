/**
 * @file b_plus_tree_index.cpp
 * @author sheep
 * @brief B+tree index wrapper
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/index/b_plus_tree_index.h"
#include "storage/index/b_plus_tree.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREEINDEX_TYPE::BPlusTreeIndex(std::unique_ptr<IndexMetadata> metadata, BufferPoolManager *bpm)
    : Index(metadata),
      tree_(metadata_->GetIndexName(), bpm, KeyComparator(metadata_->GetKeySchema())) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::InsertEntry(const Tuple &key, RID rid) {
    BPlusTreeExecutionContext context;
    tree_.Insert(key, rid, &context);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::DeleteEntry(const Tuple &key, RID rid) {
    BPlusTreeExecutionContext context;
    tree_.Remove(key, &context);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result) {
    tree_.GetValue(key, result);
}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator BPLUSTREEINDEX_TYPE::Begin() {
    return IndexIterator(tree_.Begin());
}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator BPLUSTREEINDEX_TYPE::Begin(const Tuple &key) {
    return IndexIterator(tree_.Begin(key));
}

}