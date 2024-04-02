cmake_minimum_required(VERSION 3.15)

# set the project name and version
project(nlp-engine)

set(CMAKE_CXX_STANDARD 17)

find_package(ICU REQUIRED COMPONENTS data i18n io tu uc)

include_directories(cs/include)
include_directories(include)
include_directories(include/Api)
include_directories(include/Api/lite)
include_directories(${ICU_INCLUDE_DIRS})

add_subdirectory(cs)
add_subdirectory(src)

install(DIRECTORY "${CMAKE_SOURCE_DIR}/include" DESTINATION "." FILES_MATCHING PATTERN "*.h")

add_definitions( -DLITE_EXPORTS )
if(WIN32)
  add_definitions( -DMSVC_VERSION=${MSVC_VERSION} -D_CRT_SECURE_NO_WARNINGS )
else()
  add_definitions( -DLINUX -Wno-write-strings -Wno-deprecated )
endif()

file(GLOB lite_src "lite/*.h" "lite/*.cpp")
add_library(nlp-engine SHARED ${lite_src})
target_link_libraries(nlp-engine PRIVATE 
  prim
  kbm
  consh
  words
  ${ICU_LIBRARIES}
)

target_include_directories(nlp-engine
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:include/Api/lite>
  $<INSTALL_INTERFACE:include/Api>
)

install(TARGETS nlp-engine 
  EXPORT nlp-engine-config
  RUNTIME DESTINATION bin
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
)

install(EXPORT nlp-engine-config
  FILE nlp-engine-config.cmake
  DESTINATION share/cmake/nlp-engine
)

install(
  DIRECTORY ./analyzers
  DESTINATION share/nlp-engine
  COMPONENT Runtime
)

install(
  DIRECTORY ./data
  DESTINATION share/nlp-engine
  COMPONENT Runtime
)
