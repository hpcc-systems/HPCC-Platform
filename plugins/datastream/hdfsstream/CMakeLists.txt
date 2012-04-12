project(hdfsstream)

option(USE_HDFSSTREAM "Configure use of hdstream plugin" OFF)
if ( USE_HDFSSTREAM )
add_subdirectory (ecl)
	option(HADOOP_PATH "Set the Hadoop path.")
	if( NOT HADOOP_PATH )
		set(HADOOP_PATH "/usr/local/hadoop")
	endif()

	#set other paths
	#generate config for script.
	#add script processor for vars.

	configure_file("hdfsstream.conf.in" "hdfsstream.conf")

	set(HDFSCONFIG "${CMAKE_INSTALL_PREFIX}/${OSSDIR}${CONFIG_DIR}")
	configure_file("hdfspipe.in" "hdfspipe" @ONLY )

	find_package(JNI REQUIRED)
	find_package(LIBHDFS REQUIRED)

	set( SRC hdfsstream.cpp )

	include_directories (
					${CMAKE_BINARY_DIR}
					${CMAKE_BINARY_DIR}/oss
					${JNI_INCLUDE_DIRS}
					${JAVA_INCLUDE_PATH}
					${JAVA_INCLUDE_PATH2}
					${LIBHDFS_INCLUDE_DIR}	)

	add_executable( hdfsstream ${SRC} )

	set ( INSTALLDIR "${OSSDIR}/bin")
	Install ( TARGETS hdfsstream DESTINATION ${INSTALLDIR} COMPONENT Runtime)
	Install ( PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/hdfspipe DESTINATION ${INSTALLDIR} COMPONENT Runtime )
	Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/hdfsstream.conf DESTINATION ${HDFSCONFIG} COMPONENT Runtime )
	target_link_libraries ( hdfsstream
					${JAVA_JVM_LIBRARY}
					${LIBHDFS_LIBRARIES})
endif()
