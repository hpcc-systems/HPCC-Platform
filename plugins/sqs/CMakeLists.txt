# minimal CMakeLists.txt for the AWS SDK for C++
cmake_minimum_required(VERSION 2.8)

# "my-example" is just an example value.
project(sqs)


if(SQS)
   ADD_PLUGIN(sqs)
   if(MAKE_SQS)
    # Locate the AWS SDK for C++ package.
    # Requires that you build with:
    # -Daws-sdk-cpp_DIR=/path/to/sdk_build
    # or export/set:
    # CMAKE_PREFIX_PATH=/path/to/sdk_build
    find_package(aws-sdk-cpp)
    if(!aws-sdk-cpp_FOUND) 
         message(FATAL_ERROR
          "AWS SDK not found and required that you build with: 
		-Daws-sdk-cpp_DIR=/path/to/sdk_build 
           or export/set:
		CMAKE_PREFIX_PATH=/path/to/sdk_buid")
    endif()	

    # Link to the SDK shared libraries.
    add_definitions(-DUSE_IMPORT_EXPORT)
   # add_subdirectory(doc)
    include_directories (BEFORE ${INCLUDE_DIR})
    set(
            SRCS
            sqs.h
            sqs.cpp)

   include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            ${PROJECT_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR} )

       add_definitions(-D_USRDLL -DECL_SQS_EXPORTS)
        HPCC_ADD_LIBRARY(sqs SHARED ${SRCS})

        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(sqs PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS sqs
            DESTINATION plugins)


        target_link_libraries(
            sqs 
            aws-cpp-sdk-sqs
            eclrtl
            jlib
            ${ZLIB_LIBRARIES})

   endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/sqs.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
