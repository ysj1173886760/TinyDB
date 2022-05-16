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

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREEINDEX_TYPE::BPlusTreeIndex(std::unique_ptr<IndexMetadata> metadata) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::InsertEntry(const Tuple &key, RID rid) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::DeleteEntry(const Tuple &key, RID rid) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREEINDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result) {

}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator BPLUSTREEINDEX_TYPE::Begin() {

}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator BPLUSTREEINDEX_TYPE::Begin(const Tuple &key) {

}

}