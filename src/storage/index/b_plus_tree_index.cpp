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
    : Index(std::move(metadata)),
      tree_(metadata_->GetIndexName(), bpm, KeyComparator(metadata_->GetKeySchema())) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::InsertEntry(const Tuple &key, RID rid) {
    BPlusTreeExecutionContext context;
    KeyType index_key;
    index_key.SetFromKey(key);
    tree_.Insert(index_key, rid, &context);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::DeleteEntry(const Tuple &key, RID rid) {
    BPlusTreeExecutionContext context;
    KeyType index_key;
    index_key.SetFromKey(key);
    tree_.Remove(index_key, &context);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result) {
    KeyType index_key;
    index_key.SetFromKey(key);
    tree_.GetValue(index_key, result);
}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator BPLUSTREEINDEX_TYPE::Begin() {
    return IndexIterator(tree_.Begin());
}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator BPLUSTREEINDEX_TYPE::Begin(const Tuple &key) {
    KeyType index_key;
    index_key.SetFromKey(key);
    return IndexIterator(tree_.Begin(index_key));
}

template class BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>;

}