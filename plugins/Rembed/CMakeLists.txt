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

project( Rembed )

if (USE_RINSIDE AND MAKE_R_ONLY)
  ADD_PLUGIN(Rembed PACKAGES R OPTION MAKE_REMBED)
  if ( MAKE_REMBED )
    set (    SRCS
             Rembed.cpp
        )

    include_directories (
             ${R_INCLUDE_DIRS}
             ./../../system/include
             ./../../rtl/eclrtl
             ./../../rtl/include
             ./../../rtl/nbcd
             ./../../common/deftype
             ./../../system/jlib
        )

    ADD_DEFINITIONS( -D_USRDLL -DREMBED_EXPORTS )
    if (RCPP_LIBRARY STREQUAL "")
      ADD_DEFINITIONS( -DRCPP_HEADER_ONLY )
    endif()

    HPCC_ADD_LIBRARY( Rembed SHARED ${SRCS} )
    install ( TARGETS Rembed DESTINATION plugins )

    target_link_libraries ( Rembed
        ${R_LIBRARIES}
        eclrtl
        jlib
        )
  endif()
endif()

# Even if not making the R plugin, we want to install the header
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/R.ecllib DESTINATION plugins COMPONENT Runtime)
