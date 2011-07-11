################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

# Component: jlib 
#####################################################
# Description:
# ------------
#    Cmake Input File for jlib
#####################################################

project( jlib ) 

INCLUDE(CheckLibraryExists) 

if (NOT WIN32 AND NOT WIN64)
CHECK_LIBRARY_EXISTS(dl dlopen "" HAVE_LIBDL) 
CHECK_LIBRARY_EXISTS(crypt crypt "" HAVE_LIBCRYPT) 
endif ()

set (    SRCS 
    ${SCM_GENERATED_INCLUDES}
         jargv.cpp 
         jarray.cpp 
         javahash.cpp 
         jbroadcast.cpp 
         jbsocket.cpp 
         jbuff.cpp 
         jcomp.cpp 
         jcrc.cpp 
         jdebug.cpp 
         jencrypt.cpp 
         jexcept.cpp 
         jfile.cpp 
         jflz.cpp 
         jhash.cpp 
         jiface.cpp 
         jio.cpp 
         jiter.cpp 
         jkeyboard.cpp 
         jlib.cpp 
         jlog.cpp 
         jlzma.cpp 
         jlzw.cpp 
         jmalloc.cpp 
         jmd5.cpp 
         jmemleak.cpp 
         jmisc.cpp 
         jmutex.cpp 
         jobserve.cpp 
         jprop.cpp 
         jptree.cpp 
         jregexp.cpp 
         jsem.cpp 
         jset.cpp 
         jsmartsock.cpp 
         jsocket.cpp 
         jsort.cpp 
         jstats.cpp 
         jstream.cpp 
         jstring.cpp 
         jsuperhash.cpp 
         jthread.cpp 
         jtime.cpp 
         junicode.cpp 
         jutil.cpp 
         jvmem.cpp 
         sourcedoc.xml
    )

set (    INCLUDES
        jaio.hpp
        jarray.hpp
        jarray.tpp
        jatomic.hpp
        javahash.hpp
        javahash.tpp
        jbroadcast.hpp
        jbroadcast.ipp
        jbuff.hpp
        jcomp.hpp
        jcomp.ipp
        jcrc.hpp
        jdebug.hpp
        jelogtype.hpp
        jencrypt.hpp
        jerror.hpp
        jerrorrange.hpp
        jexcept.hpp
        jexpdef.hpp
        jfile.hpp
        jfile.ipp
        jflz.hpp
        jhash.hpp
        jhash.ipp
        jheap.hpp
        jheap.ipp
        jiface.hpp
        jio.hpp
        jio.ipp
        jisem.hpp
        jiter.hpp
        jiter.ipp
        jiter.tpp
        jkeyboard.hpp
        jlib.hpp
        jliball.hpp
        jlog.hpp
        jlog.ipp
        jlzma.hpp
        jlzw.hpp
        jlzw.ipp
        jmalloc.hpp
        jmd5.hpp
        jmisc.hpp
        jmutex.hpp
        jobserve.hpp
        jobserve.ipp
        jpqueue.hpp
        jprop.hpp
        jptree.hpp
        jptree.ipp
        jqueue.tpp
        jregexp.hpp
        jrespool.tpp
        jscm.hpp
        jsem.hpp
        jset.hpp
        jsmartsock.hpp
        jsmartsock.ipp
        jsocket.hpp
        jsort.hpp
        jsorta.hpp
        jstream.hpp
        jstream.ipp
        jstring.hpp
        jsuperhash.hpp
        jthread.hpp
        jtime.hpp
        jtime.ipp
        junicode.hpp
        jutil.hpp
        jvmem.hpp
        )

set_source_files_properties (jmalloc.cpp PROPERTIES COMPILE_FLAGS -fno-strict-aliasing)


include_directories ( 
         ../../system/win32 
         ../../system/include 
         ../../system/lzma
         ${CMAKE_CURRENT_BINARY_DIR}  # for generated jelog.h file 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -DLOGMSGCOMPONENT=1 -D_USRDLL -DJLIB_EXPORTS )
HPCC_ADD_LIBRARY( jlib SHARED ${SRCS} ${INCLUDES} )

install ( TARGETS jlib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( jlib
        lzma
       )

if ( ${HAVE_LIBDL} )
target_link_libraries ( jlib dl)
endif ( ${HAVE_LIBDL} )

if ( ${HAVE_LIBCRYPT} )
target_link_libraries ( jlib crypt)
endif ( ${HAVE_LIBCRYPT} )

if (WIN32)
 target_link_libraries ( jlib ws2_32 mpr.lib Winmm.lib)
else ()
 target_link_libraries ( jlib rt)
endif ()

if (WIN32)
    FILE(TO_NATIVE_PATH "${CMAKE_CURRENT_BINARY_DIR}" WINDOWS_CMAKE_CURRENT_BINARY_DIR)
    FILE(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}\\jelog.mc" WINDOWS_CMAKE_CURRENT_SOURCE_DIR)
    add_custom_command ( DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/jelog.mc
                         OUTPUT ${EXECUTABLE_OUTPUT_PATH}/${CMAKE_CFG_INTDIR}/jelog.dll ${CMAKE_CURRENT_BINARY_DIR}/jelog.h
                         COMMAND echo mc -r ${WINDOWS_CMAKE_CURRENT_BINARY_DIR} -h ${WINDOWS_CMAKE_CURRENT_BINARY_DIR} ${WINDOWS_CMAKE_CURRENT_SOURCE_DIR}
                         COMMAND echo rc -r -fo ${CMAKE_CURRENT_BINARY_DIR}/jelog.res ${CMAKE_CURRENT_BINARY_DIR}/jelog.rc 
                         COMMAND echo link -dll -noentry -machine:IX86 -out:${EXECUTABLE_OUTPUT_PATH}/${CMAKE_CFG_INTDIR}/jelog.dll ${CMAKE_CURRENT_BINARY_DIR}/jelog.res
                         COMMAND mc -r ${WINDOWS_CMAKE_CURRENT_BINARY_DIR} -h ${WINDOWS_CMAKE_CURRENT_BINARY_DIR} ${WINDOWS_CMAKE_CURRENT_SOURCE_DIR}
                         COMMAND rc -r -fo ${CMAKE_CURRENT_BINARY_DIR}/jelog.res ${CMAKE_CURRENT_BINARY_DIR}/jelog.rc 
                         COMMAND link -dll -noentry -machine:IX86 -out:${EXECUTABLE_OUTPUT_PATH}/${CMAKE_CFG_INTDIR}/jelog.dll ${CMAKE_CURRENT_BINARY_DIR}/jelog.res
                        )
    add_custom_target ( jelog ALL DEPENDS ${EXECUTABLE_OUTPUT_PATH}/${CMAKE_CFG_INTDIR}/jelog.dll )
    add_dependencies( jlib jelog )
endif ()
