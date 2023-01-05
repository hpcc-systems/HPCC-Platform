PROJECT(sqs
  VERSION ${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}.${HPCC_SEQUENCE}
)

if(SQS AND USE_AWS)
    ADD_PLUGIN(sqs)
    if(MAKE_SQS)
        set(CRT_BUILD_SHARED_LIBS ON)
        find_package(aws-cpp-sdk-core REQUIRED)
        find_package(aws-cpp-sdk-sqs REQUIRED)
        
        set(AWS_SDK_CPP_SOURCE_DIR ${HPCC_SOURCE_DIR}/system/aws/aws-sdk-cpp)

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
            aws-cpp-sdk-core
            aws-cpp-sdk-sqs
            ${CMAKE_BINARY_DIR})

        add_definitions(-D_USRDLL -DECL_SQS_EXPORTS)
        HPCC_ADD_LIBRARY(sqs SHARED ${SRCS})

        if(NOT APPLE)
            set_target_properties(sqs PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS sqs
            DESTINATION plugins
            CALC_DEPS
        )

        target_link_libraries(
            sqs
            aws-cpp-sdk-sqs
            aws-cpp-sdk-core
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
