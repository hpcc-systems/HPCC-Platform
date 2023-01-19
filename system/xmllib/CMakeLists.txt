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

# Component: xmllib 
#####################################################
# Description:
# ------------
#    Cmake Input File for xmllib
#####################################################

project( xmllib )

if ( USE_XALAN )
    set(LIB_INCLUDE_DIR ${XALAN_INCLUDE_DIR} ${XERCES_INCLUDE_DIR})
    set(LIB_LIBRARIES ${XALAN_LIBRARIES} ${XERCES_LIBRARIES})
    set (SRCS
        xsdparser.cpp
        xslcache.cpp
        xalan_processor.cpp
        xerces_validator.cpp
    )
elseif (USE_LIBXSLT)
    set(LIB_INCLUDE_DIR ${LIBXSLT_INCLUDE_DIR} ${LIBXSLT_EXSLT_INCLUDE_DIR} ${LIBXML2_INCLUDE_DIR})
    set(LIB_LIBRARIES ${LIBXSLT_LIBRARIES} ${LIBEXSLT_LIBRARIES} ${LIBXSLT_EXSLT_LIBRARIES} ${LIBXML2_LIBRARIES})
    set (SRCS
        xsdparser.cpp
        xslcache.cpp
        libxslt_processor.cpp
        libxml_validator.cpp
        libxml_xpathprocessor.cpp
    )
else()
    set (SRCS
        xsdparser.cpp
        xslcache.cpp
        xmllib_unsupported.cpp
    )
endif()


include_directories (
         ./../../common/deftype
         ./../../system/include
         ./../../system/jlib
         ./../../ecl/hql
         ./../../rtl/include
         ./../../rtl/eclrtl
         ${LIB_INCLUDE_DIR}
         ${HPCC_SOURCE_DIR}/testing/unittests
    )

ADD_DEFINITIONS( -D_USRDLL -DXMLLIB_EXPORTS )

HPCC_ADD_LIBRARY( xmllib SHARED ${SRCS} )
install ( TARGETS xmllib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} CALC_DEPS)
target_link_libraries ( xmllib
         ${LIB_LIBRARIES}
         eclrtl
         jlib
    )

IF (USE_CPPUNIT)
  target_link_libraries (xmllib ${CPPUNIT_LIBRARIES})
ENDIF()
