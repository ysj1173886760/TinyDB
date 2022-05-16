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
#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "common/macros.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_ITERATOR_TYPE::BPlusTreeIterator(BufferPoolManager *bpm, 
                                           Page *page, 
                                           int index, 
                                           BPLUSTREE_TYPE *tree)
    : buffer_pool_manager_(bpm),
      index_(index),
      page_(page),
      tree_(tree) {
    // we are holding the latch
    page_id_ = page->GetPageId();
    leaf_page_ = reinterpret_cast<LeafPage *> (page->GetData());
    // store the key
    key_ = leaf_page_->KeyAt(index);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_ITERATOR_TYPE::Advance() {
    while (!AdvanceHelper()) {}
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_ITERATOR_TYPE::AdvanceHelper() {
    index_++;
    if (index_ == leaf_page_->GetSize()) {
        if (leaf_page_->GetNextPageId() == INVALID_PAGE_ID) {
            // release current page
            page_->RUnlatch();
            buffer_pool_manager_->UnpinPage(page_id_, false);
            // then set iterator to end
            page_id_ = INVALID_PAGE_ID;
            index_ = -1;
            return true;
        }

        // first try to acquire the latch of next page
        auto next_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(next_page != nullptr, "");
        if (next_page->TryRLatch()) {
            // we managed to acquire the RLatch, update the pointer and release the previous page
            leaf_page_ = reinterpret_cast<LeafPage *> (next_page->GetData());
            page_->RUnlatch();
            buffer_pool_manager_->UnpinPage(page_id_, false);
            
            page_id_ = next_page->GetPageId();
            page_ = next_page;
            index_ = 0;
            key_ = leaf_page_->KeyAt(index_);
            return true;
        }

        // if we failed to acquire the RLatch, then we may encounter the dead lock situation
        // more smart way to do this would be check whether there is lock on parent,
        // but that would make the logic really complex. For the simplicity i will just check the 
        // lock on sibling here

        // first release the current page
        page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_id_, false);
        // re-scan based on the highest key that we've read
        // transfer the ownership from FindHelper to me, which means i'm responsible to
        // release the lock on that page and unpin that page
        auto [page, index] = tree_->FindHelper(key_);
        page_ = page;
        index_ = index;
        leaf_page_ = reinterpret_cast<LeafPage *> (page->GetData());
        page_id_ = page->GetPageId();
        // if we've read the higher key, then it means advance succeed
        if (tree_->comparator_(leaf_page_->KeyAt(index_), key_) > 0) {
            // update the key
            key_ = leaf_page_->KeyAt(index_);
            return true;
        }

        // otherwise, we need to do the advance again
        return false;
    }

    key_ = leaf_page_->KeyAt(index_);
    return true;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_ITERATOR_TYPE::IsEnd() {
    return page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
RID BPLUSTREE_ITERATOR_TYPE::Get() {
    assert(IsEnd() == false);
    return leaf_page_->ValueAt(index_);
}

template class BPlusTreeIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIterator<GenericKey<64>, RID, GenericComparator<64>>;

}