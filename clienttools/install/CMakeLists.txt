if (UNIX)
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/postinst.in" "postinst" )
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/prerm.in" "prerm" )
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/postrm.in" "postrm" )
endif(UNIX)

install ( PROGRAMS post_install.sh DESTINATION etc/init.d/install COMPONENT Runtime )
