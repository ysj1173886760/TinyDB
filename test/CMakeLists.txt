file(GLOB TINY_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*.cpp")

include(GoogleTest)

find_program(VALGRIND_BIN valgrind)
if ("${VALGRIND_BIN}" STREQUAL "VALGRIND_BIN-NOTFOUND")
    message(WARNING " tinydb/test couldn't find valgrind.")
else()
    message(STATUS " tinydb/test found valgrind at ${VALGRIND_BIN}")
endif()

##########################################
# "make XYZ_test"
##########################################
foreach (tiny_test_source ${TINY_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(tiny_test_filename ${tiny_test_source} NAME)
    string(REPLACE ".cpp" "" tiny_test_name ${tiny_test_filename})

    add_executable(${tiny_test_name} ${tiny_test_source})

    target_link_libraries(${tiny_test_name} gtest_main ${CMAKE_PROJECT_NAME}_lib)

    # Set test target properties and dependencies.
    # set_target_properties(${tiny_test_name}
    #     PROPERTIES
    #     RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
    #     COMMAND ${tiny_test_name}
    # )

    # Add the test under CTest.
    # add_test(${tiny_test_name} ${VALGRIND_BIN} ${VALGRIND_OPTIONS}
    # --suppressions=${VALGRIND_SUPPRESSIONS_FILE} ${CMAKE_BINARY_DIR}/test/${tiny_test_name}
    # --gtest_color=yes --gtest_output=xml:${CMAKE_BINARY_DIR}/test/unit_${test_name}.xml)
    # add_test(${tiny_test_name} ${CMAKE_BINARY_DIR}/test/${tiny_test_name} --gtest_color=yes
    #         --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${tiny_test_name}.xml)
    gtest_discover_tests(${tiny_test_name})

endforeach(tiny_test_source ${tiny_TEST_SOURCES})