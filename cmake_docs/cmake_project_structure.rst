project( myproj )

set (    SRCS
         test.cpp
    )

include_directories (
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../system/include
         ./../../system/jlib
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( myproj ${SRCS} )
install ( TARGETS myproj RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( myproj
         jlib
    )
