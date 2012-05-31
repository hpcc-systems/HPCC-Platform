cmake_minimum_required(VERSION 2.8)
project(jdbcdriver Java)
option(USE_HPCCJDBC "Configure use of JDBC DB Connector")
if ( USE_HPCCJDBC )

	set ( HPCC_JDBC_PAC_PATH com/hpccsystems/jdbcdriver)
	set ( HPCC_JDBC_REL_PATH src/${HPCC_JDBC_PAC_PATH})
	set ( HPCC_ECLJDBC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/${HPCC_JDBC_REL_PATH})
	set ( HPCC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../..)
	include(${HPCC_SOURCE_DIR}/version.cmake)

	add_subdirectory (${HPCC_JDBC_REL_PATH})

        find_package(JNI REQUIRED)
        find_package(Java REQUIRED)

        include_directories (
                                        ${CMAKE_BINARY_DIR}
                                        ${CMAKE_BINARY_DIR}/oss
                                        ${JNI_INCLUDE_DIRS}
                                        ${JAVA_INCLUDE_PATH}
                                        ${JAVA_INCLUDE_PATH2}
                                        )

        set ( INSTALLDIR "${OSSDIR}/bin")

endif( USE_HPCCJDBC)
