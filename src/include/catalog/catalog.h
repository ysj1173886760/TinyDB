/**
 * @file catalog.h
 * @author sheep
 * @brief catalog
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef CATALOG_H
#define CATALOG_H

#include "storage/table/table_heap.h"
#include "storage/index/index.h"

namespace TinyDB {

using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

struct TableInfo {
    TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
        : schema_(schema),
          name_(name),
          table_(table),
          oid_(oid) {}
    
    Schema schema_;
    std::string name_;
    std::unique_ptr<TableHeap> table_;
    table_oid_t oid_;
}

struct IndexInfo {
    IndexInfo(std::unique_ptr<Index> &&index, index_oid_t index_oid)
        : index_(index),
          index_oid_(index_oid) {}

    // index has the index metadata, so we don't need to store additional metadata
    std::unique_ptr<Index> index_;
    index_oid_t index_oid_;
}


}

#endif