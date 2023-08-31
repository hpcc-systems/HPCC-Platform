################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.
#
#    Licensed under the GPL, Version 2.0 or later (the "License");
#    You may not use this file except in compliance with the License.
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

# Component: Rembed

#####################################################
# Description:
# ------------
#    Cmake Input File for Rembed
#    Note that as RInside and RCpp are GPL licensed,
#    Rembed must not be packaged with any Apache licenced
#    software such as the HPCC platform or client tools
#####################################################

project(Rembed)

if(REMBED)
    ADD_PLUGIN(Rembed PACKAGES R MINVERSION 0.12.1)
    if(MAKE_REMBED)
        set(
            SRCS
            Rembed.cpp)

        include_directories(
            ${R_INCLUDE_DIRS}
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../rtl/nbcd
            ./../../common/deftype
            ./../../common/thorhelper
            ./../../dali/base
            ./../../system/mp
            ./../../system/jlib)

        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-error=format-nonliteral")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=format-nonliteral")
        set_property (SOURCE Rembed.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-format-nonliteral")
        add_definitions(-D_USRDLL -DREMBED_EXPORTS)
        if(RCPP_LIBRARY STREQUAL "")
            add_definitions(-DRCPP_HEADER_ONLY)
        endif()

        HPCC_ADD_LIBRARY(Rembed SHARED ${SRCS})
        SET_DEPENDENCIES(CPACK_DEBIAN_PACKAGE_DEPENDS r-base-core r-cran-rcpp r-cran-inline)
        SET_DEPENDENCIES(CPACK_RPM_PACKAGE_REQUIRES R-Rcpp)
        target_link_libraries(
            Rembed
            ${R_LIBRARIES}
            eclrtl
            jlib)

        install(
            TARGETS Rembed
            DESTINATION plugins
            CALC_DEPS)
        install(
            FILES ${RINSIDE_LIBRARY}
            DESTINATION ${LIB_DIR}
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/R.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
