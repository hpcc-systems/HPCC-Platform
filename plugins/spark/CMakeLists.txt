################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################


#########################################################
# Description:
# ------------
#    Spark with HPCC
#
#
#########################################################
cmake_minimum_required(VERSION 3.3)

project(spark-integration)

option(DOWNLOAD_SPARK_JARS "Download spark jars if jars aren't supplied on command line" OFF)

if(SPARK)
    ADD_PLUGIN(spark PACKAGES Java MINVERSION 1.8.0)

    if(SPARK_URL)
        string( REPLACE "\/" ";" SPARK_URL_LIST ${SPARK_URL} )
        list( GET SPARK_URL_LIST "-1"  spark_file_name_w_ext )
        string( REPLACE "\.tgz" "" spark_file_name_only ${spark_file_name_w_ext} )
        string( REPLACE "-" ";" spark_file_name_parts ${spark_file_name_only} )
        list( GET spark_file_name_parts 1 SPARK_VERSION )
        list( GET spark_file_name_parts 3 hadoop_version_w_head )
        string( REPLACE "hadoop" "" HADOOP_VERSION ${hadoop_version_w_head} )

    else(SPARK_URL)
        if (NOT HADOOP_VERSION)
            set(HADOOP_VERSION 2.7)
        endif (NOT HADOOP_VERSION)

        if (NOT SPARK_VERSION)
            set(SPARK_VERSION 2.3.1)
        endif (NOT SPARK_VERSION)

        if (NOT SPARK_URL_BASE)
            set(SPARK_URL_BASE  "http://apache.mirrors.pair.com")
        endif (NOT SPARK_URL_BASE)

        set(SPARK_PACKAGE_NAME "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}")
        set(SPARK_URL  "${SPARK_URL_BASE}/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE_NAME}.tgz")
    endif(SPARK_URL)

    set(CENTRAL_REPO "https://repo1.maven.org/maven2")

    if(NOT SPARK_HPCC_JAR AND DOWNLOAD_SPARK_JARS)
        if(NOT SPARK_HPCC_VERSION)
            set(SPARK_HPCC_VERSION "7.0.0")
        endif()
        file(DOWNLOAD
            ${CENTRAL_REPO}/org/hpccsystems/spark-hpcc/${SPARK_HPCC_VERSION}/spark-hpcc-${SPARK_HPCC_VERSION}.jar
            ${CMAKE_CURRENT_BINARY_DIR}/spark-hpcc-${SPARK_HPCC_VERSION}.jar
            INACTIVITY_TIMEOUT 30
            TIMEOUT 90)
        set(SPARK_HPCC_JAR ${CMAKE_CURRENT_BINARY_DIR}/spark-hpcc-${SPARK_HPCC_VERSION}.jar)
    else()
        message(FATAL_ERROR "No spark jar supplied and DOWNLOAD_SPARK_JARS=OFF")
    endif()

    if(NOT WSCLIENT_JAR)    
        if(NOT WSCLIENT_VERSION)
            set(WSCLIENT_VERSION "7.0.0")
        endif()
        file(DOWNLOAD
            ${CENTRAL_REPO}/org/hpccsystems/wsclient/${WSCLIENT_VERSION}/wsclient-${WSCLIENT_VERSION}-jar-with-dependencies.jar
            ${CMAKE_CURRENT_BINARY_DIR}/wsclient-${WSCLIENT_VERSION}-jar-with-dependencies.jar
            INACTIVITY_TIMEOUT 30
            TIMEOUT 90)
        set(WSCLIENT_JAR ${CMAKE_CURRENT_BINARY_DIR}/wsclient-${WSCLIENT_VERSION}-jar-with-dependencies.jar)
    else()
        message(FATAL_ERROR "No wsclient jar supplied and DOWNLOAD_SPARK_JARS=OFF")
    endif(NOT WSCLIENT_JAR)

    message(STATUS "SPARK VERSION: ${SPARK_VERSION}")
    message(STATUS "HADOOP VERSION: ${HADOOP_VERSION}")

    install(
        DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/spark-hadoop"
        USE_SOURCE_PERMISSIONS
        COMPONENT runtime
        DESTINATION "externals"
        )

    install(
        FILES 
            ${SPARK_HPCC_JAR}
            ${WSCLIENT_JAR}
        COMPONENT runtime
        DESTINATION "jars/spark/"
        )

    configure_file(spark-defaults.conf.in spark-defaults.conf @ONLY)
    configure_file(spark-env.sh.in spark-env.sh @ONLY)
    install(
        FILES
            ${CMAKE_CURRENT_BINARY_DIR}/spark-defaults.conf
            ${CMAKE_CURRENT_BINARY_DIR}/spark-env.sh
        COMPONENT runtime
        DESTINATION "externals/spark-hadoop/conf"
        )

    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/sparkthor.sh.in" "${CMAKE_CURRENT_BINARY_DIR}/sparkthor.sh" @ONLY)
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/sparkthor-worker.sh.in" "${CMAKE_CURRENT_BINARY_DIR}/sparkthor-worker.sh" @ONLY)
    install(PROGRAMS 
        ${CMAKE_CURRENT_BINARY_DIR}/sparkthor.sh
        ${CMAKE_CURRENT_BINARY_DIR}/sparkthor-worker.sh
        DESTINATION sbin COMPONENT Runtime)

    configure_file(init_sparkthor.in init_sparkthor @ONLY)
    install(
        PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init_sparkthor
        DESTINATION ${EXEC_DIR}
        COMPONENT Runtime)
    
    configure_file(sparkthor@instance.service.in sparkthor@.service @ONLY)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/sparkthor@.service DESTINATION etc/systemd/system COMPONENT Systemd)

    if(PLATFORM)
        install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/sparkthor-service.install DESTINATION etc/init.d/install COMPONENT Systemd)
        install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/sparkthor-service.uninstall DESTINATION etc/init.d/uninstall COMPONENT Systemd)
    endif(PLATFORM)
endif(SPARK)
