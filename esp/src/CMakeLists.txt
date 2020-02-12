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

project ( eclwatch ) 

set ( SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/css/ecl.css
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/css/hpcc.css
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/nls/hpcc.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/ActivityPageWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/CurrentUserDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DataPatternsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DFUQueryWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DFUSearchWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DFUWUDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DiskUsageDetails.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DiskUsageWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DynamicESDLDefinitionDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/DynamicESDLDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/ECLPlaygroundResultsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/ECLPlaygroundWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/ECLSourceWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/EventScheduleWorkunitWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/FilePartsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/FilterDropDownWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/FullResultWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/GetDFUWorkunitsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/Graph7Widget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/GraphPageWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/GraphTree7Widget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/GraphTreeWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/GraphWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/GridDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/GroupDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HexViewWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HPCCPlatformECLWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HPCCPlatformFilesWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HPCCPlatformMainWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HPCCPlatformOpsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HPCCPlatformRoxieWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HPCCPlatformServicesPluginWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/HPCCPlatformWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/IFrameWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/InfoGridWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/LFDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/LockDialogWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/LogVisualizationWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/LogWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/LZBrowseWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/MachineInformationWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/PackageMapDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/PackageMapQueryWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/PackageMapValidateContentWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/PackageMapValidateWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/PackageSourceWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/QuerySetDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/QuerySetQueryWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/QueryTestWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/RequestInformationWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/ResultWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/SelectionGridWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/SFDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/TimingPageWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/TimingTreeMapWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/TopologyDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/TpThorStatusWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/UserDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/UserQueryWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/VizWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/WUDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/WUQueryWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/templates/XrefDetailsWidget.html
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/viz/DojoD3.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/viz/DojoD3Choropleth.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/viz/DojoD3NDChart.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/viz/DojoD32DChart.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/viz/Mapping.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/_TabContainerWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/_Widget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ActivityWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ClusterProcessesQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/CurrentUserDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DelayLoadWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DFUQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DFUSearchWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DFUWUDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DiskUsageDetails.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DiskUsageWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/dojoConfig.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DynamicESDLDefinitionDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DynamicESDLDefinitionQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DynamicESDLDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DynamicESDLMethodWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/DynamicESDLQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ECLPlaygroundResultsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ECLPlaygroundWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ECLSourceWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/EventScheduleWorkunitWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/FileBelongsToWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/FileBloomsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/FileHistoryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/FilePartsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/FilterDropDownWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/FullResultWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GetDFUWorkunitsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GetNumberOfFilesToCopyWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GraphPageWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GraphsLFWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GraphsQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GraphsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GraphsWUWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GraphWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GridDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/GroupDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HelpersWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HexViewWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HPCCPlatformECLWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HPCCPlatformFilesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HPCCPlatformMainWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HPCCPlatformOpsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HPCCPlatformRoxieWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HPCCPlatformServicesPluginWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/HPCCPlatformWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/IFrameWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/InfoGridWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/JSGraphWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/LFDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/LibrariesUsedWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/LockDialogWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/LogVisualizationWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/LogWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/LZBrowseWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/MachineInformationWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/MemberOfWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/MembersWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/MonitoringWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PackageMapDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PackageMapPartsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PackageMapQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PackageMapValidateContentWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PackageMapValidateWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PackageSourceWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PermissionsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/PreflightDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/QuerySetDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/QuerySetErrorsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/QuerySetLogicalFilesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/QuerySetQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/QuerySetSuperFilesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/QueryTestWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/RequestInformationWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ResourcesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ResultsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ResultWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/SearchResultsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/SelectionGridWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/SFDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ShowAccountPermissionsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ShowIndividualPermissionsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ShowInheritedPermissionsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/ShowPermissionsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/SourceFilesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/stub.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/SystemServersQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TableContainer.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TargetClustersQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TargetComboBoxWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TargetSelectClass.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TargetSelectWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TimingPageWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TimingTreeMapWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TopologyDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TopologyWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TpClusterInfoWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/TpThorStatusWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/UserDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/UserQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/VariablesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/VizWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/WorkflowsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/WUDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/WUQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/XrefDetailsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/XrefDirectoriesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/XrefErrorsWarningsWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/XrefFoundFilesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/XrefLostFilesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/XrefOrphanFilesWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch/XrefQueryWidget.js
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/AttributeDesc.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/Cardinality.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/config.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/DGridHeaderHook.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/DPWorkunit.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/NAWidget.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/PopularPatterns.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/Report.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatterns/StatChart.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/react/index.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/react/render.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/react/wuStatus.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src/UserPreferences/EnvironmentTheme.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Clippy.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/CodeMirror.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DataPatternsWidget.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DeclareDecorator.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/DiskUsage.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dojoLib.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPActivity.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPBase.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPDFUWorkunit.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPGraph.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPLogicalFile.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPPackageProcess.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPPreflight.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPQuery.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPQueue.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPRequest.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPResource.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPResult.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPTopology.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPTree.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPUtil.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ESPWorkunit.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/FileSpray.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Graph7Widget.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/GraphStore.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/GraphTree7Widget.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/GraphTreeWidget.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/KeyValStore.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Pagination.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Session.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Timings.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Utility.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ws_access.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ws_account.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ws_elk.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ws_machine.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsDfu.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsDFUXref.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsEcl.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsESDLConfig.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsPackageMaps.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsSMC.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsTopology.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WsWorkunits.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WUGraphLegend.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WUScopeController.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src/WUStatus.ts
)

if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Debug" )
    set ( ECLWATCH_BUILD "build-dev" )
else ()
    set ( ECLWATCH_BUILD "build" )
endif ()

set (NPM_ERROR "")
execute_process(COMMAND npm --version OUTPUT_VARIABLE _VERSION RESULT_VARIABLE _NPM_VERSION_RESULT)
if (NOT _NPM_VERSION_RESULT)
    string (REPLACE "v" "" NPM_VERSION_STRING "${_VERSION}")
    string (REPLACE "." ";" _VERSION_LIST "${NPM_VERSION_STRING}")
    list (GET _VERSION_LIST 0 NPM_VERSION_MAJOR)
    if (NPM_VERSION_MAJOR LESS 6)
        set (NPM_ERROR "NPM version ${NPM_VERSION_MAJOR} is too old (expected >= 6), please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites" )
    endif ()
else ()
    set (NPM_ERROR "Unable to locate node/npm, please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites" )
endif ()

if ( NOT "${NPM_ERROR}" STREQUAL "")
    message ( FATAL_ERROR "${NPM_ERROR}" )
endif ()

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/no.file
    COMMAND npm run clean
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target( eclwatch_clean
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/no.file
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/lib/package-lock.json
    COMMAND npm ci && ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/package-lock.json ${CMAKE_CURRENT_SOURCE_DIR}/lib/package-lock.json
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/package.json
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target( eclwatch_fetchdeps
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/lib/package-lock.json
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/build/dist/stub.eclwatch.js
    COMMAND npm run ${ECLWATCH_BUILD}
    DEPENDS ${SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target ( eclwatch ALL 
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/build/dist/stub.eclwatch.js
)

add_dependencies ( eclwatch eclwatch_fetchdeps )

install ( DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/." DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS )
