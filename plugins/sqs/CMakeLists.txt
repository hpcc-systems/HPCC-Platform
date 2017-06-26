# minimal CMakeLists.txt for the AWS SDK for C++
cmake_minimum_required(VERSION 2.8)

# "my-example" is just an example value.
project(sqs)


if(SQS)
    ADD_PLUGIN(sqs)
    set(AWS_SDK_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/aws-sdk-cpp)
    include(ExternalProject)
    ExternalProject_Add(
        aws-sdk-cpp
        SOURCE_DIR ${AWS_SDK_CPP_SOURCE_DIR}
        BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp
        CMAKE_ARGS -DCUSTOM_MEMORY_MANAGEMENT:string=0
        BUILD_COMMAND $(MAKE) LDFLAGS=-Wl,-rpath-link,${LIB_PATH} aws-cpp-sdk-sqs
        INSTALL_COMMAND "")
    add_library(aws-cpp-sdk-core SHARED IMPORTED)
    add_library(aws-cpp-sdk-sqs SHARED IMPORTED)
    set_property(TARGET aws-cpp-sdk-core
        PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/aws-cpp-sdk-core/libaws-cpp-sdk-core.so)
    set_property(TARGET aws-cpp-sdk-sqs
        PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/aws-cpp-sdk-sqs/libaws-cpp-sdk-sqs.so)
    add_dependencies(aws-cpp-sdk-core aws-sdk-cpp)
    add_dependencies(aws-cpp-sdk-sqs aws-sdk-cpp)

    add_definitions(-DUSE_IMPORT_EXPORT)

    set(SRCS
        sqs.h
        sqs.cpp)

    include_directories(
        ./../../system/include
        ./../../rtl/eclrtl
        ./../../rtl/include
        ./../../common/deftype
        ./../../system/jlib
        ${AWS_SDK_CPP_SOURCE_DIR}/aws-cpp-sdk-core/include
        ${AWS_SDK_CPP_SOURCE_DIR}/aws-cpp-sdk-sqs/include
        ${CMAKE_BINARY_DIR})

    add_definitions(-D_USRDLL -DECL_SQS_EXPORTS)
    HPCC_ADD_LIBRARY(sqs SHARED ${SRCS})

    if(NOT APPLE)
        set_target_properties(sqs PROPERTIES NO_SONAME 1)
    endif()

    install(
        TARGETS sqs
        DESTINATION plugins)

    install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/aws-sdk-cpp/aws-cpp-sdk-core:${PROJECT_BINARY_DIR}/aws-sdk-cpp/aws-cpp-sdk-sqs\")")
    install(FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/aws-sdk-cpp/LICENSE.txt
        DESTINATION "."
        RENAME aws-sdk-cpp-LICENSE.txt)
    install(PROGRAMS
        ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/aws-cpp-sdk-core/libaws-cpp-sdk-core.so
        ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/aws-cpp-sdk-sqs/libaws-cpp-sdk-sqs.so
        DESTINATION lib)

    target_link_libraries(
        sqs
        aws-cpp-sdk-sqs
        aws-cpp-sdk-core
        eclrtl
        jlib
        ${ZLIB_LIBRARIES})
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/sqs.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
