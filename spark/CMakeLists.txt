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

if(USE_SPARK)

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

    if(NOT SPARK_HPCC_JAR)
        if(NOT SPARK_HPCC_VERSION)
            set(SPARK_HPCC_VERSION "7.0.0")
        endif()
        file(DOWNLOAD
            ${CENTRAL_REPO}/org/hpccsystems/spark-hpcc/${SPARK_HPCC_VERSION}/spark-hpcc-${SPARK_HPCC_VERSION}.jar
            ${CMAKE_CURRENT_BINARY_DIR}/spark-hpcc-${SPARK_HPCC_VERSION}.jar
            INACTIVITY_TIMEOUT 30
            TIMEOUT 90)
        set(SPARK_HPCC_JAR ${CMAKE_CURRENT_BINARY_DIR}/spark-hpcc-${SPARK_HPCC_VERSION}.jar)
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
    endif(NOT WSCLIENT_JAR)


    message("")
    message("SPARK VERSION: ${SPARK_VERSION}")
    message("HADOOP VERSION: ${HADOOP_VERSION}")
    message("SPARK Package URI: ${SPARK_URL}")
    message("")

    if(SHA512)
        set( SHA512STRING "${SHA512}" )
    else(SHA512)
        set( SHA512STRING "DC3A97F3D99791D363E4F70A622B84D6E313BD852F6FDBC777D31EAB44CBC112CEEAA20F7BF835492FB654F48AE57E9969F93D3B0E6EC92076D1C5E1B40B4696" )
    endif(SHA512)

    string(TOLOWER "${SHA512STRING}" SHA512STRING)
    set(URL_HASHSTRING "SHA512=${SHA512STRING}")

    message("${URL_HASHSTRING}")

    include(ExternalProject)
    ExternalProject_Add(
        fetch-spark-source
        URL ${SPARK_URL}
        URL_HASH ${URL_HASHSTRING}
        TIMEOUT 300
        DOWNLOAD_DIR ${CMAKE_BINARY_DIR}/downloads
        SOURCE_DIR ${CMAKE_BINARY_DIR}/downloads/spark-hadoop
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND "")

    add_custom_target(
        spark-source ALL
        DEPENDS fetch-spark-source
        COMMAND ${CMAKE_COMMAND} -E remove -f ${SPARK_PACKAGE_NAME}.tgz
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/downloads)

    install(
        DIRECTORY "${CMAKE_BINARY_DIR}/downloads/spark-hadoop"
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

endif(USE_SPARK)
