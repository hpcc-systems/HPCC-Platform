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
    if(MAKE_SPARK)

        set(CENTRAL_REPO "https://repo1.maven.org/maven2")

        option(DOWNLOAD_SPARK_JARS OFF "Download most recent spark plugin java packages at build time")

        #check submodules
        message(STATUS "Checking for submodule files:")
        message(STATUS "${CMAKE_CURRENT_SOURCE_DIR}/spark-hadoop/LICENSE")
        if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/spark-hadoop/LICENSE")
        message(STATUS "Spark-hadoop submodule not populated on disk")
        message(STATUS "Run: git submodule update --init --recursive")
        message(FATAL_ERROR "spark-hadoop submodules error")
        endif()

        message(STATUS "${CMAKE_CURRENT_SOURCE_DIR}/spark-plugin-java-packages/LICENSE.txt")
        if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/spark-plugin-java-packages/LICENSE.txt")
        message(STATUS "Spark-plugin-java-packages not populated on disk")
        message(STATUS "Run: git submodule update --init --recursive")
        message(FATAL_ERROR "spark-plugin-java-packages submodule error")
        endif()
        #end submodule check

        set(JAR_VERSION "${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}-${HPCC_SEQUENCE}")
        if(NOT HPCC_MATURITY STREQUAL "release")
            set(JAR_VERSION "${JAR_VERSION}-SNAPSHOT")
        endif()

        if(NOT SPARK_HPCC_JAR AND DOWNLOAD_SPARK_JARS)
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
                    message(STATUS "\tdownloaded spark-hpcc-${JAR_VERSION}.jar")
                endif()
            endif()
            set(SPARK_HPCC_JAR ${CMAKE_CURRENT_BINARY_DIR}/spark-hpcc-${JAR_VERSION}.jar)
        endif(NOT SPARK_HPCC_JAR AND DOWNLOAD_SPARK_JARS)

        if(NOT DFSCLIENT_JAR AND DOWNLOAD_SPARK_JARS)
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
                    message(STATUS "\tdownloaded dfsclient-${JAR_VERSION}-jar-with-dependencies.jar")
                endif()
            endif()
            set(DFSCLIENT_JAR ${CMAKE_CURRENT_BINARY_DIR}/dfsclient-${JAR_VERSION}-jar-with-dependencies.jar)
        endif(NOT DFSCLIENT_JAR AND DOWNLOAD_SPARK_JARS)

        if(NOT SPARK_HPCC_JAR)
            file(GLOB SPARK_HPCC_JAR 
                RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
                ${CMAKE_CURRENT_SOURCE_DIR}/spark-plugin-java-packages/spark-hpcc-*.jar)
        endif()
        if(NOT DFSCLIENT_JAR)
            file(GLOB DFSCLIENT_JAR
                RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
                ${CMAKE_CURRENT_SOURCE_DIR}/spark-plugin-java-packages/dfsclient-*-jar-with-dependencies.jar)
        endif()

        message(STATUS "\tincluding ${DFSCLIENT_JAR} in the package")
        message(STATUS "\tincluding ${SPARK_HPCC_JAR} in the package")

        install(
            DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/spark-hadoop"
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
    endif()
endif()
