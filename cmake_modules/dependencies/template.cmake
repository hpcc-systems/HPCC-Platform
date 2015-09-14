# This is a template of what is needed to create a dependency file for your distribution.

# Steps to follow.
# 1. Copy template.cmake to <distribution name>.cmake
#       ex. Ubuntu 11.04      natty.cmake
#       ex. Centos 5.x            el5.cmake
#
# 2. Add a DEPENDS line based on the package system of your distribution.
#       ex. SET_DEPENDENCIES ( CPACK_DEBIAN_PACKAGE_DEPENDS libicu libboost-regex1.42.0 )
#       ex. SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES libicu boost )
#
# 3. Save your changes and create a build from your build directory with `make package`
