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
            set(SPARK_VERSION 2.4.3)
        endif (NOT SPARK_VERSION)

        if (NOT SPARK_URL_BASE)
            set(SPARK_URL_BASE  "https://archive.apache.org/dist")
        endif (NOT SPARK_URL_BASE)

        set(SPARK_PACKAGE_NAME "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}")
        set(SPARK_URL  "${SPARK_URL_BASE}/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE_NAME}.tgz")
    endif(SPARK_URL)

    set(CENTRAL_REPO "https://repo1.maven.org/maven2")

    set(JAR_VERSION "${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}-${HPCC_SEQUENCE}")
    if(NOT HPCC_MATURITY STREQUAL "release")
        set(JAR_VERSION "${JAR_VERSION}-SNAPSHOT")
    endif()


    if(NOT SPARK_HPCC_JAR)
        if(HPCC_MATURITY STREQUAL "release")
            file(DOWNLOAD
                ${CENTRAL_REPO}/org/hpccsystems/spark-hpcc/${JAR_VERSION}/spark-hpcc-${JAR_VERSION}.jar
                ${CMAKE_CURRENT_BINARY_DIR}/spark-hpcc-${JAR_VERSION}.jar
                INACTIVITY_TIMEOUT 30
            TIMEOUT 90)
        else()
            execute_process(
                COMMAND wget "https://oss.sonatype.org/service/local/artifact/maven/content?r=snapshots&g=org.hpccsystems&a=spark-hpcc&v=${JAR_VERSION}" -O spark-hpcc-${JAR_VERSION}.jar -q
                WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                RESULT_VARIABLE dl_spark_hpcc
                OUTPUT_QUIET
                )
            if(dl_spark_hpcc)
                message(FATAL_ERROR "Download of spark-hpcc-${JAR_VERSION}.jar failed")
            else()
                message(STATUS "Successfully downloaded spark-hpcc-${JAR_VERSION}.jar")
            endif()
        endif()
        set(SPARK_HPCC_JAR ${CMAKE_CURRENT_BINARY_DIR}/spark-hpcc-${JAR_VERSION}.jar)
    endif()

    if(NOT DFSCLIENT_JAR)
        if(HPCC_MATURITY STREQUAL "release") 
            file(DOWNLOAD
                ${CENTRAL_REPO}/org/hpccsystems/dfsclient/${JAR_VERSION}/dfsclient-${JAR_VERSION}-jar-with-dependencies.jar
                ${CMAKE_CURRENT_BINARY_DIR}/dfsclient-${JAR_VERSION}-jar-with-dependencies.jar
                INACTIVITY_TIMEOUT 30
                TIMEOUT 90)
        else()
            execute_process(
                COMMAND wget "https://oss.sonatype.org/service/local/artifact/maven/content?r=snapshots&g=org.hpccsystems&a=dfsclient&v=${JAR_VERSION}&c=jar-with-dependencies" -O dfsclient-${JAR_VERSION}-jar-with-dependencies.jar -q
                WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                RESULT_VARIABLE dl_dfsclient
                OUTPUT_QUIET
                )
            if(dl_dfsclient)
                message(FATAL_ERROR "Download of dfsclient-${JAR_VERSION}-jar-with-dependencies.jar failed")
            else()
                message(STATUS "Successfully downloaded dfsclient-${JAR_VERSION}-jar-with-dependencies.jar")
            endif()
        endif()
        set(DFSCLIENT_JAR ${CMAKE_CURRENT_BINARY_DIR}/dfsclient-${JAR_VERSION}-jar-with-dependencies.jar)
    endif(NOT DFSCLIENT_JAR)


    message("")
    message("SPARK VERSION: ${SPARK_VERSION}")
    message("HADOOP VERSION: ${HADOOP_VERSION}")
    message("SPARK Package URI: ${SPARK_URL}")
    message("")

    if(SHA512)
        set( SHA512STRING "${SHA512}" )
    else(SHA512)
        set( SHA512STRING "e8b7f9e1dec868282cadcad81599038a22f48fb597d44af1b13fcc76b7dacd2a1caf431f95e394e1227066087e3ce6c2137c4abaf60c60076b78f959074ff2ad" )
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
            ${DFSCLIENT_JAR}
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
    install(
        FILES
            ${CMAKE_CURRENT_SOURCE_DIR}/spark-env.install
        COMPONENT runtime
        DESTINATION "etc/init.d/install"
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
