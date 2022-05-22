/**
 * @file result.h
 * @author sheep
 * @brief Result type, trying to mimic result type in rust
 * @version 0.1
 * @date 2022-05-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "common/error_code.h"

#include <utility>
#include <cassert>

namespace TinyDB {

/**
 * @brief 
 * type that i was trying to mimic "Result" in rust. 
 * Current implementation is naive. Since we will only use error code to represent error,
 * i've simplified the implementation and combined the type flag with the error code.
 * I really want to introduce the real "Result" in this project, so i might try to dig it a bit deeper, 
 * or maybe just introduce C++20 and introduce std::expect which i think it's similar to "Result"
 * @tparam T 
 */
template <typename T>
class Result {
public:
    Result(T &&data)
        : data_(data), code_(ErrorCode::INVALID) {}
    
    Result(ErrorCode code)
        : code_(code) {
    }

    ~Result() {}
    
    bool IsErr() {
        return code_ != ErrorCode::INVALID;
    }
    
    bool IsOk() {
        return code_ == ErrorCode::INVALID;
    }

    // hope i'm not introducing additional copy
    T &&GetOk() {
        return std::move(data_);
    }

    ErrorCode GetErr() {
        return code_;
    }

private:
    T data_;
    ErrorCode code_;
};

}