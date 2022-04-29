set(binary ${CMAKE_PROJECT_NAME})

file(GLOB_RECURSE sources ${PROJECT_SOURCE_DIR}/src/*/*.cpp ${PROJECT_SOURCE_DIR}/src/*/*/*.cpp ${PROJECT_SOURCE_DIR}/src/*.cpp)

set(sources ${sources})

# add_executable(${binary} ${sources})

add_library(${binary}_lib STATIC ${sources})