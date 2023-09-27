project(wasmembed)

if(WASMEMBED)
    ADD_PLUGIN(wasmembed)
    if(MAKE_WASMEMBED)

        add_subdirectory(secure-enclave)

        include_directories(
            ./../../common/thorhelper
            ./../../dali/base
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../rtl/nbcd
            ./../../system/include
            ./../../system/jlib
            ./../../system/mp
        )

        add_definitions(-D_USRDLL -DWASMEMBED_EXPORTS)

        add_library(wasmembed SHARED 
            wasmembed.cpp
        )

        target_link_libraries(wasmembed
            roxiemem
            eclrtl
            jlib
            secure-enclave
        )

        install(
            TARGETS wasmembed 
            DESTINATION plugins
        )

    else()
        message(WARNING "Cannot build wasmembed plugin")
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/wasm.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
