/**
 * @file b_plus_tree_internal_page.cpp
 * @author sheep
 * @brief B+tree internal page implementation
 * @version 0.1
 * @date 2022-05-13
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/page/b_plus_tree_internal_page.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, uint32_t max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetPageId(page_id);
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(uint32_t index) const {
    return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(uint32_t index, const KeyType &key) {
    array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    for (int i = 0; i < GetSize(); i++) {
        if (array_[i].second == value) {
            return i;
        }
    }
    return -1;
}

INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(uint32_t index) const {
    return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
    uint lb = 0;
    uint ub = GetSize();
    while (ub - lb > 1) {
        uint mid = (ub + lb) / 2;
        if (comparator(key, KeyAt(mid)) >= 0) {
            // key is bigger that mid
            // raise the lower bound
            lb = mid;
        } else {
            ub = mid;
        }
    }
    return ValueAt(lb);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
    IncreaseSize(1);
    array_[0].second = old_value;
    InsertNodeAfter(old_value, new_key, new_value);
}

INDEX_TEMPLATE_ARGUMENTS
uint32_t B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
    for (uint i = GetSize(); i >= 1; i--) {
        if (array_[i - 1].second == old_value) {
            array_[i] = std::make_pair(new_key, new_value);
            break;
        }
        array_[i] = array_[i - 1];
    }
    IncreaseSize(1);
    return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    for (auto i = index; i < GetSize() - 1; i++) {
        array_[i] = array_[i + 1];
    }
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    SetSize(0);
    return array_[0].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm) {
    array_[0].first = middle_key;
    recipient->CopyNFrom(array_, GetSize(), bpm);
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm) {
    uint half = (GetSize() + 1) / 2;
    recipient->CopyNFrom(&array_[half], GetSize() - half, bpm);
    SetSize(half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm) {
    recipient->CopyLastFrom(std::make_pair(middle_key, array_[0].second), bpm);
    Remove(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *bpm) {
    recipient->SetKeyAt(0, middle_key);
    recipient->CopyFirstFrom(array_[GetSize() - 1], bpm);
    Remove(GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, uint32_t size, BufferPoolManager *bpm) {
    for (uint i = 0; i < size; i++) {
        auto *page = bpm->FetchPage(items[i].second);
        auto bplustree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
        bplustree_page->SetParentPageId(GetPageId());
        bpm->UnpinPage(page->GetPageId(), true);
        array_[GetSize() + i] = items[i];
    }
    IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *bpm) {
    auto page = bpm->FetchPage(pair.second);
    auto bplustree_page = reinterpret_cast<BPlusTreePage *> (page->GetData());
    bplustree_page->SetParentPageId(GetPageId());
    bpm->UnpinPage(page->GetPageId(), true);

    array_[GetSize()] = pair;
    IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *bpm) {
    auto page = bpm->FetchPage(pair.second);
    auto bplustree_page = reinterpret_cast<BPlusTreePage *> (page->GetData());
    bplustree_page->SetParentPageId(GetPageId());
    bpm->UnpinPage(page->GetPageId(), true);

    for (uint i = GetSize(); i >= 1; i--) {
        array_[i] = array_[i - 1];
    }
    array_[0] = pair;
    IncreaseSize(1);
}

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

}