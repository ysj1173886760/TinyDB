/**
 * @file logger.h
 * @author sheep
 * @brief logger
 * @version 0.1
 * @date 2021-10-20
 * 
 * @copyright Copyright (c) 2021
 * 
 */

#ifndef LOGGER_H
#define LOGGER_H

#include <ctime>
#include <string>

namespace TinyDB {

using cstr = const char *;

static constexpr cstr pastLastSlash(cstr str, cstr lastSlash) {
    return *str == '\0' ? lastSlash : 
           *str == '/'  ? pastLastSlash(str + 1, str + 1) : 
                          pastLastSlash(str + 1, lastSlash);
}

static constexpr cstr pastLastSlash(cstr str) { return pastLastSlash(str, str); }

#define __SHORT_FILE__                            \
  ({                                              \
    constexpr cstr sf__{pastLastSlash(__FILE__)}; \
    sf__;                                         \
  })

}

#endif