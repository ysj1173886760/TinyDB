/**
 * @file index_builder.h
 * @author sheep
 * @brief index factory
 * @version 0.1
 * @date 2022-05-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef INDEX_BUILDER_H
#define INDEX_BUILDER_H

#include "storage/index/index.h"
#include "buffer/buffer_pool_manager.h"

#include <memory>

namespace TinyDB {

class IndexBuilder {
public:
    static std::unique_ptr<Index> Build(std::unique_ptr<IndexMetadata> metadata, BufferPoolManager *bpm);
};

}

#endif