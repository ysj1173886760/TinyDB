/**
 * @file b_plus_tree_iterator.cpp
 * @author sheep
 * @brief b+tree iterator
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/index/b_plus_tree_iterator.h"
#include "storage/index/b_plus_tree.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_ITERATOR_TYPE::BPlusTreeIterator(BufferPoolManager *bpm, 
                                           Page *page, 
                                           int index, 
                                           BPLUSTREE_TYPE *tree)
    : buffer_pool_manager_(bpm),
      index_(index),
      tree_(tree) {
    // we are holding the latch
    page_id_ = page->GetPageId();
    leaf_page_ = reinterpret_cast<LeafPage *> (page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_ITERATOR_TYPE::Advance() {

}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_ITERATOR_TYPE::IsEnd() {

}

INDEX_TEMPLATE_ARGUMENTS
RID BPLUSTREE_ITERATOR_TYPE::Get() {

}

template class BPlusTreeIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIterator<GenericKey<64>, RID, GenericComparator<64>>;

}