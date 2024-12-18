project(component-model-test)

find_package(doctest CONFIG REQUIRED)

include_directories(
    ../src
)

add_executable(${PROJECT_NAME} 
    main.cpp
    host-util.hpp
    host-util.cpp
)

target_link_libraries(${PROJECT_NAME} 
    PRIVATE doctest::doctest
    PRIVATE component-model-cpp
)

add_test(
    NAME ${PROJECT_NAME}
    COMMAND $<TARGET_FILE:${PROJECT_NAME}>
)
