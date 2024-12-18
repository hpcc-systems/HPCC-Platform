project(component-model-cpp)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SRC
    boolean.cpp
    boolean.hpp
    context.hpp
    context.cpp
    float.hpp
    float.cpp
    integer.hpp
    integer.cpp
    list.hpp
    list.cpp
    load.hpp
    load.cpp
    string.hpp
    string.cpp
    traits.hpp
    traits.cpp
    util.hpp
    util.cpp
)

add_library(${PROJECT_NAME} STATIC
    ${SRC}
)

target_link_libraries(${PROJECT_NAME} 
)

target_compile_definitions(${PROJECT_NAME} 
    PRIVATE CMCPP_EXPORTS
)
target_compile_options(${PROJECT_NAME} PUBLIC "$<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/permissive->")

target_include_directories(
    ${PROJECT_NAME} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                         $<INSTALL_INTERFACE:include/${PROJECT_NAME}-${PROJECT_VERSION}>
)
