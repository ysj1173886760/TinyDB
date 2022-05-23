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
    OUT_OF_MEMORY,
    DEADLOCK,
};

}

#endif