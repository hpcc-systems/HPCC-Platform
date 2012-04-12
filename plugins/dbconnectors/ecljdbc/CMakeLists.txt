cmake_minimum_required(VERSION 2.8)
project(ecljdbc Java)
option(USE_ECLJDBC "Configure use of DB Connectors")
if ( USE_ECLJDBC )
	add_subdirectory (src/main/java/com/hpccsystems/ecljdbc)

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

endif( USE_ECLJDBC)
