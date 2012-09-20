################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

file(GLOB hFiles RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} cmake_modules/Hpcc*)
file(GLOB dFiles RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} cmake_modules/dependencies)
file(GLOB rFiles RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} cmake_modules/*)
LIST(REMOVE_ITEM rFiles ${hFiles} ${dFiles})
FOREACH( iFiles ${rFiles} )
    GET_FILENAME_COMPONENT(iPath "${iFiles}" PATH)
    install ( FILES ${HPCC_SOURCE_DIR}/${iFiles} DESTINATION ${OSSDIR}/lib/${iPath} COMPONENT Include )
ENDFOREACH()

file(GLOB_RECURSE headerFiles RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.h*)
FOREACH( iFiles ${headerFiles} )
    GET_FILENAME_COMPONENT(iPath "${iFiles}" PATH)
    install ( FILES ${HPCC_SOURCE_DIR}/${iFiles} DESTINATION ${OSSDIR}/include/${iPath} COMPONENT Include )
ENDFOREACH()

file(GLOB scmFiles RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} esp/scm/*)
FOREACH( iFiles ${scmFiles} )
    GET_FILENAME_COMPONENT(iPath "${iFiles}" PATH)
    install ( FILES ${HPCC_SOURCE_DIR}/${iFiles} DESTINATION ${OSSDIR}/include/${iPath} COMPONENT Include )
ENDFOREACH()

install ( FILES ${CMAKE_BINARY_DIR}/build-config.h DESTINATION ${OSSDIR}/include COMPONENT Include )

file(GLOB docCommon RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} docs/common/*)
file(GLOB docBuildTools RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} docs/BuildTools/*)
file(GLOB docResources RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} docs/resources/*)

list(APPEND docFiles ${docCommon} ${docBuildTools} ${docResources} )
FOREACH( iFiles ${docFiles} )
    GET_FILENAME_COMPONENT(iPath "${iFiles}" PATH)
    install ( FILES ${HPCC_SOURCE_DIR}/${iFiles} DESTINATION ${OSSDIR}/lib/${iPath} COMPONENT Include )
ENDFOREACH()
