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


# Component: hql 

#####################################################
# Description:
# ------------
#    Cmake Input File for hql
#####################################################


project( hql ) 

set (   SRCS 
        hqlatoms.cpp 
        hqlattr.cpp 
        hqlcollect.cpp
        hqldesc.cpp
        hqldsparam.cpp 
        hqlerror.cpp 
        hqlesp.cpp 
        hqlexpr.cpp 
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
        hqlvalid.cpp 
        hqlwuerr.cpp 
        hqlxmldb.cpp 

        hqlgram.y
        hqllex.l
         
        hqlerror.hpp
        hqlerrors.hpp
        hql.hpp
        hqlatoms.hpp
        hqlattr.hpp
        hqlcollect.hpp
        hqldesc.hpp
        hqlesp.hpp
        hqlexpr.hpp
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
        hqlvalid.hpp
        hqlwuerr.hpp
        hqlxmldb.hpp
    )

include_directories ( 
         ${CMAKE_CURRENT_BINARY_DIR}  # for generated header files
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../common/deftype 
         ./../../system/hrpc 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../eclagent 
         ./../../system/include 
         ./../../common/workunit
         ./../../system/jlib 
         ./../../ecl/hql 
    )

if (WIN32)
    add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h
        #pipe result through findstr to remove warnings that are hard to suppress, and pipe through more to prevent error code from no matches aborting the compile
        COMMAND ${bisoncmdprefix} ${bisoncmd} --report=state --defines=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h --output=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hqlgram.y 2>&1 | findstr /V "unused value" | more
        DEPENDS hqlgram.y
    )
else()
    add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h
        #pipe result through grep to remove warnings that are hard to suppress, and pipe through cat to prevent error code from no matches aborting the compile
        COMMAND ${bisoncmdprefix} ${bisoncmd} --report=state --defines=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.h --output=${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hqlgram.y 2>&1 | grep -v "unused value" | cat
        DEPENDS hqlgram.y
    )
endif()

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hqllex.cpp 
    COMMAND ${flexcmdprefix} ${flexcmd} --outfile=${CMAKE_CURRENT_BINARY_DIR}/hqllex.cpp --header-file=${CMAKE_CURRENT_BINARY_DIR}/hqllex.hpp ${CMAKE_CURRENT_SOURCE_DIR}/hqllex.l
    DEPENDS hqllex.l
)

if (WIN32)
    set_source_files_properties (hqlgram.cpp PROPERTIES COMPILE_FLAGS "/Od")
endif()

ADD_DEFINITIONS( -D_USRDLL -DHQL_EXPORTS -DHQLFOLD_EXPORTS -DHQLTRANS_EXPORTS )

HPCC_ADD_LIBRARY( hql SHARED ${SRCS} ${CMAKE_CURRENT_BINARY_DIR}/hqlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hqllex.cpp  )
install ( TARGETS hql DESTINATION ${OSSDIR}/lib )
target_link_libraries ( hql  
         jlib
         nbcd 
         eclrtl 
         deftype 
    )


