################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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


# Component: javaembed

#####################################################
# Description:
# ------------
#    Cmake Input File for javaembed
#####################################################

project(javaembed)

if(JAVAEMBED AND USE_JAVA)
    ADD_PLUGIN(javaembed)
    if(MAKE_JAVAEMBED)
        find_package(Java 1.8 COMPONENTS Development)
        find_package(JNI)
        set(
            SRCS
            javaembed.cpp)

        include_directories(
            ${JNI_INCLUDE_DIRS}
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../rtl/nbcd
            ./../../common/deftype
            ./../../system/jlib
             ./../../dali/base
            ./../../system/mp
            ./../../system/security/shared
            ./../../roxie/roxiemem
            ${HPCC_SOURCE_DIR}/esp/esdllib
            ${HPCC_SOURCE_DIR}/common/thorhelper
            ${CMAKE_BINARY_DIR}
            ${CMAKE_BINARY_DIR}/oss)

        set(java_sources HpccClassLoader.java
                         HpccUtils.java
                         ActivityContext.java)
        set(java_classes ${CMAKE_CURRENT_BINARY_DIR}/com/HPCCSystems/HpccClassLoader.class
                         ${CMAKE_CURRENT_BINARY_DIR}/com/HPCCSystems/HpccUtils.class
                         ${CMAKE_CURRENT_BINARY_DIR}/com/HPCCSystems/ActivityContext.class)
        add_custom_command ( OUTPUT ${java_classes}
            COMMAND ${Java_JAVAC_EXECUTABLE} ${java_sources} -d ${CMAKE_CURRENT_BINARY_DIR} -source 1.8 -target 1.8
            DEPENDS ${java_sources}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
        add_custom_target(build-java-classes ALL
          DEPENDS ${java_classes}
        )
            
        add_definitions(-D_USRDLL -DJAVAEMBED_EXPORTS)

        HPCC_ADD_LIBRARY(javaembed SHARED ${SRCS})
        add_dependencies(javaembed build-java-classes)

        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(javaembed PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS javaembed
            DESTINATION plugins)
        install(
            FILES ${java_classes}
            DESTINATION classes/com/HPCCSystems
            COMPONENT Runtime
            CALC_DEPS)

        # We link against jsig so that signals are chained from the jvm

        get_filename_component(JAVA_LIBRARY_PATH ${JAVA_AWT_LIBRARY} PATH)
        if(APPLE)
            set(JSIG_LIBRARY ${JAVA_LIBRARY_PATH}/libjsig.dylib)
        elseif(WIN32)
            set(JSIG_LIBRARY ${JAVA_LIBRARY_PATH}/jsig.dll)
        else()
            set(JSIG_LIBRARY ${JAVA_LIBRARY_PATH}/libjsig.so)
        endif()

        target_link_libraries(
            javaembed
            #${JSIG_LIBRARY}
            ${JAVA_JVM_LIBRARY}
            eclrtl
            roxiemem
            jlib)
    endif()
endif()
if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/java.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
