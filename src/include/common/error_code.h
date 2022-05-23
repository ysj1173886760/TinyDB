/**
 * @file error_code.h
 * @author sheep
 * @brief errors in tinydb
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef ERROR_CODE_H
#define ERROR_CODE_H

namespace TinyDB {

enum class ErrorCode {
    INVALID,
    // out of memory, buffer pool manager is full
    OUT_OF_MEMORY,
    // encountered a deadlock situation
    DEADLOCK,
    // we should skip this tuple
    SKIP,
    // out of space when updating tuple
    OUT_OF_SPACE,
    // indicate that we should abort the txn
    ABORT,
};

}

#endif