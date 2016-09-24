################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################


# Component: eclblas

#####################################################
# Description:
# ------------
#    Cmake Input File for eclblas
#####################################################


project( eclblas )

set (    SRCS
         dasum.cpp
         daxpy.cpp
         dgemm.cpp
         dgetf2.cpp
         dpotf2.cpp
         dscal.cpp
         dsyrk.cpp
         dttrsm.cpp
         eclblas.cpp
         Extract_Tri.cpp
         make_diag.cpp
    )

include_directories (
         ./../../rtl/include
         ./../../rtl/eclrtl
    )

ADD_DEFINITIONS( -D_USRDLL -DECLBLAS_EXPORTS )

HPCC_ADD_LIBRARY( eclblas SHARED ${SRCS} )
install ( TARGETS eclblas DESTINATION plugins )
target_link_libraries ( eclblas
         eclrtl
         cblas
    )