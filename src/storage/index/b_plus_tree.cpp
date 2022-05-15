/**
 * @file b_plus_tree.cpp
 * @author sheep
 * @brief B+tree implementation
 * @version 0.1
 * @date 2022-05-14
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/index/b_plus_tree.h"
#include "common/macros.h"
#include "common/exception.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string index_name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          uint32_t leaf_max_size, uint32_t internal_max_size)
    : index_name_(index_name),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, BPlusTreeExecutionContext *context) {
    root_latch_.lock();
    if (IsEmpty()) {
        StartNewTree(key, value);
        root_latch_.unlock();
        return true;
    }
    return InsertIntoLeaf(key, value, context);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Remove(const KeyType &key, BPlusTreeExecutionContext *context) {

    root_latch_.lock();
    bool rootLocked = true;
    if (IsEmpty()) {
        root_latch_.unlock();
        return false;
    }

    Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(cur_page != nullptr, "");
    cur_page->WLatch();

    context->AddIntoPageSet(cur_page);
    BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

    while (!bPlusTreePage->IsLeafPage()) {
        InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
        Page *next_page = buffer_pool_manager_->FetchPage(internalPage->Lookup(key, comparator_));
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(next_page != nullptr, "");
        next_page->WLatch();

        BPlusTreePage *next_node = reinterpret_cast<BPlusTreePage *>(next_page);
        // both internal node and leaf node will coalesce when size < minSize
        if (next_node->GetSize() > next_node->GetMinSize()) {
            // safe, release all of the previous pages
            // release pages at top-down order
            auto pageSet = context->GetPageSet();
            while (!pageSet->empty()) {
                Page *page = pageSet->front();
                pageSet->pop_front();

                if (rootLocked && page->GetPageId() == root_page_id_) {
                    root_latch_.unlock();
                    rootLocked = false;
                }
                page->WUnlatch();
                buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            }
        }
        context->AddIntoPageSet(next_page);

        // step to next page
        cur_page = next_page;
        bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    }

    LeafPage *leafPage = reinterpret_cast<LeafPage *>(bPlusTreePage);
    bool res = leafPage->RemoveAndDeleteRecord(key, comparator_);

    if (leafPage->GetSize() < leafPage->GetMinSize()) {
        CoalesceOrRedistribute<LeafPage>(leafPage, context);
    }

    auto pageSet = context->GetPageSet();
    auto deletedPageSet = context->GetDeletedPageSet();
    while (!pageSet->empty()) {
        Page *page = pageSet->front();
        pageSet->pop_front();

        page_id_t pageId = page->GetPageId();
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(pageId, res);
        if (deletedPageSet->count(pageId) != 0) {
            buffer_pool_manager_->DeletePage(pageId);
            // guarantee that we will only delete page once
            deletedPageSet->erase(pageId);
        }
    }

    if (rootLocked) {
        root_latch_.unlock();
    }

    return res;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, BPlusTreeExecutionContext *context) {
    root_latch_.lock();
    bool rootLocked = true;
    if (IsEmpty()) {
        root_latch_.unlock();
        return false;
    }

    Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(cur_page != nullptr, "");

    cur_page->RLatch();
    BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

    while (!bPlusTreePage->IsLeafPage()) {
        InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
        page_id_t next_page_id;
        next_page_id = internalPage->Lookup(key, comparator_);

        Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(next_page != nullptr, "");

        next_page->RLatch();

        // release previous page
        if (rootLocked && bPlusTreePage->IsRootPage()) {
            root_latch_.unlock();
            rootLocked = false;
        }
        cur_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

        // step to next page
        cur_page = next_page;
        bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    }

    ValueType v;
    LeafPage *leafPage = reinterpret_cast<LeafPage *>(cur_page->GetData());
    bool res = false;

    if (leafPage->Lookup(key, &v, comparator_)) {
        res = true;
        result->push_back(v);
    }

    // release current page. don't forget to check the root latch
    if (rootLocked) {
        root_latch_.unlock();
    }
    cur_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);

    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(new_page != nullptr, "");

    LeafPage *leafPage = reinterpret_cast<LeafPage *>(new_page->GetData());
    leafPage->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    leafPage->Insert(key, value, comparator_);

    root_page_id_ = new_page_id;
    UpdateRootPageId();

    buffer_pool_manager_->UnpinPage(new_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, BPlusTreeExecutionContext *context) {
    bool rootLocked = true;
    Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(cur_page != nullptr, "");
    cur_page->WLatch();

    context->AddIntoPageSet(cur_page);
    BPlusTreePage *bPlusTreePage = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

    while (!bPlusTreePage->IsLeafPage()) {
        InternalPage *internalPage = reinterpret_cast<InternalPage *>(bPlusTreePage);
        Page *next_page = buffer_pool_manager_->FetchPage(internalPage->Lookup(key, comparator_));
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(next_page != nullptr, "");
        next_page->WLatch();

        BPlusTreePage *next_node = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
        // for internal node, it will only split when size == maxSize
        // for leaf node, it will split when size == maxSize - 1
        if ((next_node->IsLeafPage() && next_node->GetSize() < next_node->GetMaxSize() - 1) ||
            (!next_node->IsLeafPage() && next_node->GetSize() < next_node->GetMaxSize())) {
            // safe, release all of the previous pages
            // release pages as top-down order
            auto pageSet = context->GetPageSet();
            while (!pageSet->empty()) {
                Page *page = pageSet->front();
                pageSet->pop_front();

                // actually, we shall use IsRoot to check whether the page is root and release the root latch.
                // for the sake of simplicity, and efficiency (and also lazy). I use root_page_id_ to check it directly.
                // sheep: i think this check is fine, if we are holding the lock, then there is no way root_page_id_ will 
                // change, so i think simply check the root id is enough.
                if (rootLocked && page->GetPageId() == root_page_id_) {
                    root_latch_.unlock();
                    rootLocked = false;
                }
                page->WUnlatch();
                buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            }
        }
        context->AddIntoPageSet(next_page);

        // step to next page
        cur_page = next_page;
        bPlusTreePage = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    }

    LeafPage *leafPage = reinterpret_cast<LeafPage *>(cur_page->GetData());
    bool res = true;

    if (leafPage->Lookup(key, nullptr, comparator_)) {
        res = false;
    } else {
        // first we need to split
        leafPage->Insert(key, value, comparator_);
        if (leafPage->GetSize() >= leafPage->GetMaxSize()) {
            LeafPage *new_node = Split<LeafPage>(leafPage);
            InsertIntoParent(leafPage, new_node->KeyAt(0), new_node, context);
            new_node->SetNextPageId(leafPage->GetNextPageId());
            leafPage->SetNextPageId(new_node->GetPageId());
            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        }
    }

    // here we don't need to check whether the root is locked or not.
    // if we are modifying root, then we shall release lock here.
    // if we are not modifying root, then root latch shall be released above
    auto pageSet = context->GetPageSet();
    while (!pageSet->empty()) {
        Page *page = pageSet->front();
        pageSet->pop_front();
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), res);
    }

    if (rootLocked) {
        root_latch_.unlock();
    }
    return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node, BPlusTreeExecutionContext *context) {
    if (old_node->IsRootPage()) {
        page_id_t new_page_id;
        Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(new_page != nullptr, "");

        root_page_id_ = new_page_id;
        UpdateRootPageId();

        InternalPage *internalPage = reinterpret_cast<InternalPage *>(new_page->GetData());
        internalPage->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
        internalPage->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

        old_node->SetParentPageId(new_page_id);
        new_node->SetParentPageId(new_page_id);
        buffer_pool_manager_->UnpinPage(new_page_id, true);
    } else {
        Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(parent_page != nullptr, "");

        InternalPage *internalPage = reinterpret_cast<InternalPage *>(parent_page->GetData());

        internalPage->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        if (internalPage->GetSize() > internalPage->GetMaxSize()) {
            InternalPage *new_node = Split<InternalPage>(internalPage);
            InsertIntoParent(internalPage, new_node->KeyAt(0), new_node, context);
            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        }
    }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(new_page != nullptr, "");

    if (node->IsLeafPage()) {
        LeafPage *leafPage = reinterpret_cast<LeafPage *>(new_page->GetData());
        leafPage->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
        reinterpret_cast<LeafPage *>(node)->MoveHalfTo(leafPage);
        return reinterpret_cast<N *>(leafPage);
    }

    InternalPage *internalPage = reinterpret_cast<InternalPage *>(new_page->GetData());
    internalPage->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    reinterpret_cast<InternalPage *>(node)->MoveHalfTo(internalPage, buffer_pool_manager_);
    return reinterpret_cast<N *>(internalPage);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, BPlusTreeExecutionContext *context) {
    if (node->IsRootPage()) {
        if (AdjustRoot(node)) {
            context->AddIntoDeletedPageSet(node->GetPageId());
            return true;
        }
        return false;
    }

    Page *p = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(p != nullptr, "");
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(p->GetData());
    // p should already been added in page set
    // but remember, we still need to call Unpin
    // for the simplicity, let's add it again so it will be Unpin twice
    context->AddIntoPageSet(p);

    int index = internalPage->ValueIndex(node->GetPageId());
    int sibling;
    if (index == 0) {
        sibling = 1;
    } else {
        sibling = index - 1;
    }

    Page *sibP = buffer_pool_manager_->FetchPage(internalPage->ValueAt(sibling));
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(sibP != nullptr, "");
    sibP->WLatch();
    context->AddIntoPageSet(sibP);
    N *siblingPage = reinterpret_cast<N *>(sibP->GetData());

    // coalesce
    // according to our split algorithm, for internalNode, we can have the node size equals to MaxSize.
    // but leafNode, we only can have maximum to MaxSize - 1
    if ((node->IsLeafPage() && siblingPage->GetSize() + node->GetSize() < node->GetMaxSize()) ||
        (!node->IsLeafPage() && siblingPage->GetSize() + node->GetSize() <= node->GetMaxSize())) {
        // since we are swapping node and sibling here. So we don't use return value to judge whether a node should be
        // deleted. instead, we add to deletedPageSet directly and delete it outside.
        if (index == 0) {
            std::swap(siblingPage, node);
            std::swap(index, sibling);
        }
        Coalesce(&siblingPage, &node, &internalPage, index, context);
        return true;
    }

    // Redistribute
    Redistribute(siblingPage, node, index);
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node, InternalPage **parent, int index, BPlusTreeExecutionContext *context) {
    if ((*node)->IsLeafPage()) {
        reinterpret_cast<LeafPage *>(*node)->MoveAllTo(reinterpret_cast<LeafPage *>(*neighbor_node));
    } else {
        reinterpret_cast<InternalPage *>(*node)->MoveAllTo(reinterpret_cast<InternalPage *>(*neighbor_node),
                                                        (*parent)->KeyAt(index), buffer_pool_manager_);
    }
    (*parent)->Remove(index);
    context->AddIntoDeletedPageSet((*node)->GetPageId());

    if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
        return CoalesceOrRedistribute<InternalPage>(*parent, context);
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");
    InternalPage *parentPage = reinterpret_cast<InternalPage *>(page->GetData());

    if (index == 0) {
        if (node->IsLeafPage()) {
            reinterpret_cast<LeafPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<LeafPage *>(node));
        } else {
            reinterpret_cast<InternalPage *>(neighbor_node)
                ->MoveFirstToEndOf(reinterpret_cast<InternalPage *>(node), parentPage->KeyAt(1), buffer_pool_manager_);
        }
        parentPage->SetKeyAt(1, neighbor_node->KeyAt(0));

    } else {
        if (node->IsLeafPage()) {
            reinterpret_cast<LeafPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<LeafPage *>(node));
        } else {
            reinterpret_cast<InternalPage *>(neighbor_node)
                ->MoveLastToFrontOf(reinterpret_cast<InternalPage *>(node), parentPage->KeyAt(index), buffer_pool_manager_);
        }
        parentPage->SetKeyAt(index, node->KeyAt(0));
    }

    // even we've already added page into the page set in context
    // we still need to Unpin this page, because every FetchPage call should 
    // have its own Unpin call
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    // one last child
    if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
        InternalPage *internalPage = reinterpret_cast<InternalPage *>(old_root_node);

        page_id_t new_root = internalPage->RemoveAndReturnOnlyChild();
        Page *page = buffer_pool_manager_->FetchPage(new_root);
        TINYDB_CHECK_OR_THROW_OUT_OF_MEMORY_EXCEPTION(page != nullptr, "");

        LeafPage *leafPage = reinterpret_cast<LeafPage *>(page->GetData());
        leafPage->SetParentPageId(INVALID_PAGE_ID);

        root_page_id_ = new_root;
        UpdateRootPageId();

        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        return true;
    }

    // empty tree
    if (old_root_node->GetSize() == 0 && old_root_node->IsLeafPage()) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();
        return true;
    }

    return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(bool insert_record) {
    // TODO: implement header page then uncomment this
    // HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
    // if (insert_record != 0) {
    //     // create a new record<index_name + root_page_id> in header_page
    //     header_page->InsertRecord(index_name_, root_page_id_);
    // } else {
    //     // update root_page_id in header_page
    //     header_page->UpdateRecord(index_name_, root_page_id_);
    // }
    // buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}