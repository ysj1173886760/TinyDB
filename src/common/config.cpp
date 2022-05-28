/**
 * @file config.cpp
 * @author sheep
 * @brief configuration file
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "common/config.h"

namespace TinyDB {

std::chrono::milliseconds CYCLE_DETECTION_INTERVAL = std::chrono::milliseconds(50);

std::chrono::duration<int64_t> log_timeout = std::chrono::seconds(1);

}