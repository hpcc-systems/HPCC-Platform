project(hdfsconnector)

option(USE_HDFSCONNECTOR "Configure use of hdfs data connector" OFF)
if ( USE_HDFSCONNECTOR )
add_subdirectory (ecl)
	option(HADOOP_PATH "Set the Hadoop path.")
	if( NOT HADOOP_PATH )
		set(HADOOP_PATH "/usr/local/hadoop")
	endif()

	#set other paths
	#generate config for script.
	#add script processor for vars.

	set(HPCC_ETC_DIR "${CMAKE_INSTALL_PREFIX}/${OSSDIR}/etc")
	set(HPCC_CONF_DIR "${CMAKE_INSTALL_PREFIX}/${OSSDIR}${CONFIG_DIR}")
	set(HDFSCONN_CONF_FILE "hdfsconnector.conf")

	configure_file("hdfsconnector.conf.in" "hdfsconnector.conf")
	configure_file("hdfspipe.in" "hdfspipe" @ONLY )

	find_package(JNI REQUIRED)
	find_package(LIBHDFS REQUIRED)

	set( SRC hdfsconnector.cpp )

	include_directories (
					${CMAKE_BINARY_DIR}
					${CMAKE_BINARY_DIR}/oss
					${JNI_INCLUDE_DIRS}
					${JAVA_INCLUDE_PATH}
					${JAVA_INCLUDE_PATH2}
					${LIBHDFS_INCLUDE_DIR}	)

	add_executable( hdfsconnector ${SRC} )

	set ( INSTALLDIR "${OSSDIR}/bin")
	Install ( TARGETS hdfsconnector DESTINATION ${INSTALLDIR} COMPONENT Runtime)
	Install ( PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/hdfspipe DESTINATION ${INSTALLDIR} COMPONENT Runtime )
	Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/hdfsconnector.conf DESTINATION ${HPCC_CONF_DIR} COMPONENT Runtime )
	target_link_libraries ( hdfsconnector
					${JAVA_JVM_LIBRARY}
					${LIBHDFS_LIBRARIES})
endif()
