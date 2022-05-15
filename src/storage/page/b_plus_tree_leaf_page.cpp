/**
 * @file b_plus_tree_leaf_page.cpp
 * @author sheep
 * @brief B+tree leaf page implementation
 * @version 0.1
 * @date 2022-05-13
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "storage/page/b_plus_tree_leaf_page.h"
#include "common/exception.h"

namespace TinyDB {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, uint32_t max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetMaxSize(max_size);
    SetParentPageId(parent_id);
    SetPageId(page_id);
    SetSize(0);
    SetNextPageId(INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
    return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(uint32_t index) const {
    return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
    int lb = -1;
    int ub = GetSize() - 1;
    while (ub - lb > 1) {
        int mid = (ub + lb) / 2;
        if (comparator(KeyAt(mid), key) >= 0) {
            // if mid is greater than or equal to key
            // then we lower the upper bound
            ub = mid;
        } else {
            lb = mid;
        }
    }
    return static_cast<uint32_t>(ub);
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(uint32_t index) {
    return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
uint32_t B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
    if (GetSize() == 0) {
        array_[0] = std::make_pair(key, value);
    } else {
        auto ub = KeyIndex(key, comparator);

        // don't update for duplicated key
        if (ub == GetSize() || comparator(KeyAt(ub), key) != 0) {
            for (auto i = GetSize(); i >= ub; i--) {
                array_[i] = array_[i - 1];
            }
            array_[ub] = std::make_pair(key, value);
        } else {
            THROW_LOGIC_ERROR_EXCEPTION("duplicated key");
        }
    }

    IncreaseSize(1);
    return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
    auto ub = KeyIndex(key, comparator);
    if (comparator(array_[ub].first, key) == 0) {
        if (value != nullptr) {
            *value = array_[ub].second;
        }
        return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
uint32_t B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
    auto ub = KeyIndex(key, comparator);
    if (comparator(array_[ub].first, key) == 0) {
        for (auto i = ub; i < GetSize() - 1; i++) {
            array_[i] = array_[i + 1];
        }
        IncreaseSize(-1);
    }
    return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
    int half = (GetSize() + 1) / 2;
    recipient->CopyNFrom(&array_[half], GetSize() - half);
    SetSize(half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
    recipient->CopyNFrom(array_, GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
    recipient->CopyLastFrom(array_[0]);
    IncreaseSize(-1);
    for (int i = 0; i < GetSize() - 1; i++) {
        array_[i] = array_[i + 1];
    }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
    recipient->CopyFirstFrom(array_[GetSize() - 1]);
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, uint32_t size) {
    for (int i = 0; i < size; i++) {
        array_[i + GetSize()] = items[i];
    }
    IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array_[GetSize()] = item;
    IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
    for (int i = GetSize(); i >= 1; i--) {
        array_[i] = array_[i - 1];
    }
    array_[0] = item;
    IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

}