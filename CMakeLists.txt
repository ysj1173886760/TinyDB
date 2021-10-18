cmake_minimum_required(VERSION 3.14)
project(TinyDB)

set(CMAKE_CXX_STANDARD 17)              # Compile as C++17
set(CMAKE_CXX_STANDARD_REQUIRED ON)     # Require C++17 support

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

include_directories(src)

add_subdirectory(src)
add_subdirectory(test)