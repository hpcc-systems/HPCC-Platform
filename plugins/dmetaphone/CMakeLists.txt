# Component: dmetaphone

#####################################################
# Description:
# ------------
#    Cmake Input File for dmetaphone
#####################################################

set ( toolsdir "${HPCC_SOURCE_DIR}/tools" )


project( dmetaphone )

set (    SRCS
         dmetaphone.cpp
         metaphone.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/plugins/common
         ${HPCC_SOURCE_DIR}/system/include
    )

ADD_DEFINITIONS( -D_USRDLL -DDMETAPHONE_EXPORTS )

HPCC_ADD_LIBRARY( dmetaphone SHARED ${SRCS} )
install ( 
     TARGETS dmetaphone 
     DESTINATION plugins 
     COMPONENT Runtime
)
target_link_libraries ( dmetaphone
         jlib
    )
