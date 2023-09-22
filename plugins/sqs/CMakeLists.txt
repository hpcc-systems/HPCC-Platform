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

        if(WIN32 OR APPLE)
            install(
                TARGETS sqs
                DESTINATION plugins
                CALC_DEPS
            )
        else()
            install(
                RUNTIME_DEPENDENCY_SET sqs_deps
                DIRECTORIES "${VCPKG_FILES_DIR}/vcpkg_installed"
                PRE_EXCLUDE_REGEXES "libaws-c-auth|libaws-c-cal|libaws-c-common|libaws-c-compression|libaws-c-event-stream|libaws-c-http|libaws-c-io|libaws-c-mqtt|libaws-c-s3|libaws-c-sdkutils|libaws-checksums|libaws-cpp-sdk-core|libaws-cpp-sdk-core|libaws-crt-cpp"
                POST_INCLUDE_REGEXES "^${VCPKG_FILES_DIR}\/vcpkg_installed\/.*"
                POST_EXCLUDE_REGEXES ".*"
            )
            install(
                TARGETS sqs
                RUNTIME_DEPENDENCY_SET sqs_deps
                DESTINATION plugins
            )
        endif()


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
