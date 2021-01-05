################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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


# Component: hql 

#####################################################
# Description:
# ------------
#    Cmake Input File for hql
#####################################################


project( hql ) 

set(extrasources)
if (WIN32)
  if (ARCH64BIT)
    # 64bit windows builds do not support inline assembler - so the folding function needs to be defined in an asm file
    enable_language(ASM_MASM)
    set(extrasources hqlfoldasm.asm)
  endif()
endif()

set (   SRCS 
        hqlatoms.cpp
        hqlattr.cpp
        hqlcache.cpp
        hqlcollect.cpp
        hqldesc.cpp
        hqldsparam.cpp
        hqlerror.cpp
        hqlesp.cpp
        hqlexpr.cpp
        hqlfield.cpp
        hqlfilter.cpp
        hqlfold.cpp
        hqlgram2.cpp
        hqlmanifest.cpp
        hqlmeta.cpp
        hqlopt.cpp
        hqlparse.cpp
        hqlpmap.cpp
        hqlplugininfo.cpp
        hqlpregex.cpp
        hqlrepository.cpp
        hqlscope.cpp
        hqlstack.cpp
        hqlthql.cpp
        hqltrans.cpp
        hqlusage.cpp
        hqlutil.cpp
        hqlir.cpp
        hqlvalid.cpp
        hqlwuerr.cpp
        hqlxmldb.cpp
        reservedwords.cpp

        ${extrasources}

        hqlgram.y
        hqllex.l
         
        hqlerror.hpp
        hqlerrors.hpp
        hql.hpp
        hqlatoms.hpp
        hqlattr.hpp
        hqlcache.hpp
        hqlcollect.hpp
        hqldesc.hpp
        hqlesp.hpp
        hqlexpr.hpp
        hqlfield.hpp
        hqlfilter.hpp
        hqlfold.hpp
        hqlgram.hpp
        hqlmeta.hpp
        hqlopt.hpp
        hqlopt.ipp
        hqlplugininfo.hpp
        hqlpmap.hpp
        hqlpregex.hpp
        hqlrepository.hpp
        hqlscope.hpp
        hqlstack.hpp
        hqlthor.hpp
        hqlthql.hpp
        hqlusage.hpp
        hqlutil.hpp
        hqlir.hpp
        hqlvalid.hpp
        hqlwuerr.hpp
        hqlxmldb.hpp
        reservedwords.hpp
    )

include_directories ( 
         ${CMAKE_CURRENT_BINARY_DIR}  # for generated header files
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../common/deftype 
         ./../../system/hrpc 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../rtl/include
         ./../eclagent 
         ./../../system/include 
         ./../../common/workunit
         ./../../system/jlib 
         ./../../ecl/hql 
         ./../../testing/unittests
         ./../../system/security/zcrypt
         ${HPCC_SOURCE_DIR}/system/codesigner
    )

if (WIN32)
    add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h
        #${bisonopt} is used to force s/r and r/r errors to abort compiling
        COMMAND ${bisoncmdprefix} ${bisoncmd} ${bisonopt} --report=state --defines=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h --output=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hqlgram.y 2>&1
        DEPENDS hqlgram.y
    )
else()
    add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h
        #delete output files before building to force rebuilds with errors to fail rather than continue unnoticed, using the existing output files.
        COMMAND rm -f ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp
        #${bisonopt} is used to force s/r and r/r errors to abort compiling
        COMMAND ${bisoncmdprefix} ${bisoncmd} ${bisonopt} --report=state --defines=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h --output=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hqlgram.y 2>&1
        DEPENDS hqlgram.y
    )
endif()

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hqllex.cpp 
    COMMAND ${flexcmdprefix} ${flexcmd} --outfile=${CMAKE_CURRENT_BINARY_DIR}/hqllex.cpp --header-file=${CMAKE_CURRENT_BINARY_DIR}/hqllex.hpp ${CMAKE_CURRENT_SOURCE_DIR}/hqllex.l
    DEPENDS hqllex.l
)

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-switch -Wno-unused-parameter -Werror -Wno-error=delete-non-virtual-dtor")

  if (CMAKE_COMPILER_IS_GNUCC AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "7.0.0")
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wimplicit-fallthrough=3")
    #gcc incorrectly thinks that a function that never returns can fall through
    set_source_files_properties (hqlfilter.cpp PROPERTIES COMPILE_FLAGS " -Wimplicit-fallthrough=0")
  endif()

  #Compiler complains about a memcpy of a structure that contains a union, and copy if uninitialised enum
  if (CMAKE_COMPILER_IS_GNUCC AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "8.0.0")
    set_source_files_properties (hqlgram.cpp PROPERTIES COMPILE_FLAGS " -Wno-class-memaccess -fno-sanitize=undefined")
    set_source_files_properties (hqllex.cpp PROPERTIES COMPILE_FLAGS " -Wno-class-memaccess -Wno-sign-compare")
  endif()

  #Compiler complains about an unused function yyunput, and make sure that any warnings are not reported as errors
  set_source_files_properties (hqllex.cpp PROPERTIES COMPILE_FLAGS " -Wno-unused-function -Wno-error")
endif()

if (WIN32)
    set_source_files_properties (hqlgram.cpp PROPERTIES COMPILE_FLAGS "/Od")
endif()

ADD_DEFINITIONS( -D_USRDLL -DHQL_EXPORTS -DHQLFOLD_EXPORTS -DHQLTRANS_EXPORTS )

HPCC_ADD_LIBRARY( hql SHARED ${SRCS} ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hqllex.cpp  )
install ( TARGETS hql RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( hql
         jlib
         nbcd
         eclrtl
         deftype
         ${CPPUNIT_LIBRARIES}
    )

IF (USE_ZLIB)
    target_link_libraries ( hql
        ${ZLIB_LIBRARIES}
        zcrypt
    )
ENDIF()
