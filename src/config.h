/**
 * @file config.h
 * @author sheep
 * @brief defintions of types and constants
 * @version 0.1
 * @date 2021-10-20
 * 
 * @copyright Copyright (c) 2021
 * 
 */

#ifndef CONFIG_H
#define CONFIG_H

#include <cstdint>

namespace TinyDB {

    // size of a page, which is the basic unit of our database
    static constexpr int PAGE_SIZE = 4096;

    using page_id_t = int32_t;
    using frame_id_t = int32_t;
};

#endif