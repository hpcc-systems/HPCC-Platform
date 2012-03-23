project(hdfsstream)

option(USE_HDFSSTREAM "Configure use of hdstream plugin" OFF)
if ( USE_HDFSSTREAM )
	option(HADOOP_PATH "Set the Hadoop path.")
	if( NOT HADOOP_PATH )
		set(HADOOP_PATH "/usr/local/hadoop")
	endif()

	#set other paths
	#generate config for script.
	#add script processor for vars.

	configure_file("hdfsstreamconfig.in" "hdfsstreamconfig")

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

#	Install ( TARGETS hdfsstream DESTINATION ${DIR_NAME}/bin COMPONENT Runtime)
#	Install ( PROGRAMS hdfspipe DESTINATION ${DIR_NAME}/bin COMPONENT Runtime )
#	Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/hdfsstreamconfig DESTINATION ${CONFIG_DIR}/rpmnew COMPONENT Runtime )
#	Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/hdfsstreamconfig DESTINATION ${DIR_NAME}/bin COMPONENT Runtime )
	target_link_libraries ( hdfsstream
					${JAVA_JVM_LIBRARY}
					${LIBHDFS_LIBRARIES})
endif()
