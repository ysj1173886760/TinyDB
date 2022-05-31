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
#include <stddef.h>
#include <chrono>

namespace TinyDB {

// size of a page, which is the basic unit of our database
static constexpr uint32_t PAGE_SIZE = 4096;

// size of buffer pool, should be configured based on your memory
static constexpr uint32_t BUFFER_POOL_SIZE = 10;

// size of log buffer
static constexpr int LOG_BUFFER_SIZE = ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE);

// special values
static constexpr int INVALID_PAGE_ID = -1;
static constexpr int INVALID_TXN_ID = -1;
static constexpr int INVALID_LSN = -1;

// type definitions
using page_id_t = int32_t;
using frame_id_t = int32_t;
using lsn_t = int32_t;
using txn_id_t = int32_t;

// configurations

// interval for running cycle detection
// we will store it in cpp file, to prevent multiple definition error
extern std::chrono::milliseconds CYCLE_DETECTION_INTERVAL;

// interval for flushing the log
extern std::chrono::milliseconds log_timeout;

};

#endif