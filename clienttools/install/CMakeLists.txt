if (APPLE)
   # If APPLE requires the same scripts  if/elseif  will be removed
elseif (UNIX)
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/postinst.in" "postinst" )
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/prerm.in" "prerm" )
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/postrm.in" "postrm" )
endif(APPLE)

install ( PROGRAMS post_install.sh DESTINATION etc/init.d/install COMPONENT Runtime )
