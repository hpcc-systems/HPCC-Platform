################################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
################################################################################


# Component: soapplus 

#####################################################
# Description:
# ------------
#    Cmake Input File for soapplus
#####################################################


project( soapplus ) 

set (    SRCS 
         EspLogDeserializer.cpp 
         http.cpp 
         httpproxy.cpp 
         main.cpp 
         msggenerator.cpp 
         xmldiff.cpp 
    )

include_directories ( 
         ./../../../system/include 
         ./../../../system/security/securesocket 
         ./../../bindings/http/platform 
         ./../../../system/xmllib 
         ./../../../system/jlib 
    )

ADD_DEFINITIONS( -D_CONSOLE )

add_executable ( soapplus ${SRCS} )
install ( TARGETS soapplus DESTINATION ${OSSDIR}/bin )

target_link_libraries ( soapplus 
         jlib
         xmllib 
         securesocket 
    )


