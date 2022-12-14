cmake_minimum_required(VERSION 3.22.1)

project(HelloWorld)

# Add an executable target
add_executable(HelloWorld main.cpp)

# Use CPack to create installers for Windows, macOS, and Ubuntu
include(InstallRequiredSystemLibraries)
set(CPACK_PACKAGE_NAME "HelloWorld")
set(CPACK_PACKAGE_VENDOR "MyCompany")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A simple Hello World program")
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "HelloWorld")

if (APPLE)
    set(CPACK_GENERATOR "DragNDrop;productbuild;Bundle")
elseif (LINUX)
    set(CPACK_GENERATOR "DEB")
elseif (WIN32)
    set(CPACK_GENERATOR "NSIS")
endif ()

include(CPack)
