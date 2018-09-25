################################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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

# Component: envxml
#####################################################
# Description:
# ------------
#    Implements IConfigEnv
#####################################################

project( envxml )

set (    INCLUDES
         IConfigEnv.hpp
         GenEnvRules.hpp
         EnvHelper.hpp
         ConfigEnv.hpp
         ComponentBase.hpp
         SWComponentBase.hpp
         Hardware.hpp
         Programs.hpp
         EnvSettings.hpp
         Software.hpp
         SWDirectories.hpp
         SWDropZone.hpp
         SWProcess.hpp
         SWDaliProcess.hpp
         SWRoxieCluster.hpp
         SWThorCluster.hpp
         SWEspProcess.hpp
         SWEspService.hpp
         SWTopology.hpp
         ConfigEnvFactory.hpp
         ComponentFromXSD.hpp
         environment.hpp
         SWBackupNode.hpp

    )
set (    SRCS
         ConfigEnv.cpp
         ../common/GenEnvRules.cpp
         EnvHelper.cpp
         ComponentBase.cpp
         SWComponentBase.cpp
         Hardware.cpp
         Programs.cpp
         EnvSettings.cpp
         Software.cpp
         SWProcess.cpp
         SWDaliProcess.cpp
         SWDropZone.cpp
         SWDirectories.cpp
         SWEspProcess.cpp
         SWEspService.cpp
         SWRoxieCluster.cpp
         SWThorCluster.cpp
         SWTopology.cpp
         ComponentFromXSD.cpp
         SWBackupNode.cpp
    )

include_directories (
         ../
         ../common
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/esp/esplib
         ${HPCC_SOURCE_DIR}/deployment/configgen
         ${HPCC_SOURCE_DIR}/deployment/deploy
         ${HPCC_SOURCE_DIR}/deployment/configutils
         ${HPCC_SOURCE_DIR}/deployment/deployutils
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/security/shared
    )

ADD_DEFINITIONS ( -D_USRDLL -DCONFIGENV_EXPORTS )

HPCC_ADD_LIBRARY( configenv SHARED ${SRCS} )
install ( TARGETS configenv RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( configenv
         jlib
#         environment
#         xmllib
#         deploy
#         configutils
          deployutils
    )
