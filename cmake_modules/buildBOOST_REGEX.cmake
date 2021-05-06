
include(ExternalProject)
ExternalProject_Add(
  generate-boost-regex
  URL https://boostorg.jfrog.io/artifactory/main/release/1.71.0/source/boost_1_71_0.tar.gz
  URL_HASH SHA256=96b34f7468f26a141f6020efb813f1a2f3dfb9797ecf76a7d7cbd843cc95f5bd
  TIMEOUT 60
  DOWNLOAD_DIR ${CMAKE_BINARY_DIR}/downloads
  SOURCE_DIR ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0
  CONFIGURE_COMMAND ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0/bootstrap.sh
  BUILD_COMMAND ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0/b2 --prefix=${INSTALL_DIR} --exec-prefix=${INSTALL_DIR} --with-regex --with-headers
  BUILD_IN_SOURCE TRUE
  INSTALL_COMMAND ""
  )

add_library(boost-regex SHARED IMPORTED GLOBAL)
set_property(TARGET boost-regex
  PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0/stage/lib/libboost_regex.so.1.71.0)
add_dependencies(boost-regex generate-boost-regex)

if(PLATFORM OR CLIENTTOOLS_ONLY)
  install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${CMAKE_BINARY_DIR}:${CMAKE_BINARY_DIR}/downloads/boost_1_71_0/stage/lib\")")
  install(PROGRAMS
    ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0/stage/lib/libboost_regex.so.1.71.0
    ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0/stage/lib/libboost_regex.so
    DESTINATION ${LIB_DIR})
endif()

set(BOOST_REGEX_INCLUDE_DIR ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0)
set(BOOST_REGEX_LIBRARIES ${CMAKE_BINARY_DIR}/downloads/boost_1_71_0/stage/lib/libboost_regex.so.1.17.0)
message(STATUS "----------------------- ${BOOST_REGEX_LIBRARIES}")
mark_as_advanced(BOOST_REGEX_LIBRARIES BOOST_REGEX_INCLUDE_DIR)
