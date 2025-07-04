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

file(REMOVE_RECURSE
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch
    ${CMAKE_CURRENT_BINARY_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/src-react
)

file(COPY
    ${CMAKE_CURRENT_SOURCE_DIR}/dgrid
    ${CMAKE_CURRENT_SOURCE_DIR}/eclwatch
    ${CMAKE_CURRENT_SOURCE_DIR}/eslint
    ${CMAKE_CURRENT_SOURCE_DIR}/ganglia
    ${CMAKE_CURRENT_SOURCE_DIR}/loader
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react
    ${CMAKE_CURRENT_SOURCE_DIR}/eslint.config.mjs
    ${CMAKE_CURRENT_SOURCE_DIR}/GetUserName.html
    ${CMAKE_CURRENT_SOURCE_DIR}/index.html
    ${CMAKE_CURRENT_SOURCE_DIR}/Login.html
    ${CMAKE_CURRENT_SOURCE_DIR}/lws.config.js
    ${CMAKE_CURRENT_SOURCE_DIR}/package-lock.json
    ${CMAKE_CURRENT_SOURCE_DIR}/package.json
    ${CMAKE_CURRENT_SOURCE_DIR}/playwright.config.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/stub.htm
    ${CMAKE_CURRENT_SOURCE_DIR}/tsconfig.json
    ${CMAKE_CURRENT_SOURCE_DIR}/webpack.config.js
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/BuildInfo.ts ${CMAKE_CURRENT_BINARY_DIR}/src/BuildInfo.ts @ONLY)

set ( SRCS
    # eclwatch files
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/css/cmDarcula.css
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/css/ecl.css
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/css/hpcc.css
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/ActivityPageWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/ActivityPageContainerWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/CurrentUserDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DataPatternsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DFUQueryWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DFUSearchWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DFUWUDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DiskUsageDetails.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DiskUsageWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DynamicESDLDefinitionDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/DynamicESDLDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/ECLPlaygroundResultsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/ECLPlaygroundWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/ECLSourceWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/EventScheduleWorkunitWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/FilePartsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/FilterDropDownWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/FullResultWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/GetDFUWorkunitsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/Graph7Widget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/GraphPageWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/GraphTree7Widget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/GraphTreeWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/GraphWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/GridDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/GroupDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HexViewWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HPCCPlatformECLWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HPCCPlatformFilesWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HPCCPlatformMainWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HPCCPlatformOpsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HPCCPlatformRoxieWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HPCCPlatformServicesPluginWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/HPCCPlatformWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/IFrameWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/InfoGridWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/LFDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/LockDialogWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/LogVisualizationWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/LogWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/LZBrowseWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/MachineInformationWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/PackageMapDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/PackageMapQueryWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/PackageMapValidateContentWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/PackageMapValidateWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/PackageSourceWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/QuerySetDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/QuerySetQueryWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/QueryTestWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/RequestInformationWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/ResultWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/SelectionGridWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/SFDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/SourceFilesWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/TimingPageWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/TimingTreeMapWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/TopologyDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/TpThorStatusWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/UserDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/UserQueryWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/VizWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/WUDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/WUQueryWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/templates/XrefDetailsWidget.html
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/viz/DojoD3.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/viz/DojoD3Choropleth.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/viz/DojoD32DChart.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/viz/DojoD3NDChart.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/viz/Mapping.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/_TabContainerWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/_Widget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ActivityWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ClusterProcessesQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ComponentUsageDetails.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/CurrentUserDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DelayLoadWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DFUQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DFUSearchWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DFUWUDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DiskUsageDetails.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DiskUsageWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/dojoConfig.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DynamicESDLDefinitionDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DynamicESDLDefinitionQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DynamicESDLDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DynamicESDLMethodWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/DynamicESDLQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ECLPlaygroundResultsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ECLPlaygroundWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ECLSourceWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/EventScheduleWorkunitWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/FileBelongsToWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/FileBloomsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/FileHistoryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/FilePartsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/FileProtectListWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/FilterDropDownWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/FullResultWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GetDFUWorkunitsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GetNumberOfFilesToCopyWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GraphPageWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GraphsLFWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GraphsQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GraphsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GraphsWUWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GraphWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GridDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/GroupDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HelpersWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HexViewWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HPCCPlatformECLWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HPCCPlatformFilesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HPCCPlatformMainWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HPCCPlatformOpsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HPCCPlatformRoxieWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HPCCPlatformServicesPluginWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/HPCCPlatformWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/IFrameWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/InfoGridWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/JSGraphWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/LFDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/LibrariesUsedWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/LockDialogWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/LogVisualizationWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/LogWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/LZBrowseWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/MachineInformationWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/MemberOfWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/MembersWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/MonitoringWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PackageMapDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PackageMapPartsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PackageMapQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PackageMapValidateContentWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PackageMapValidateWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PackageSourceWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PermissionsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/PreflightDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ProcessesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/QuerySetDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/QuerySetErrorsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/QuerySetLogicalFilesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/QuerySetQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/QuerySetSuperFilesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/QueryTestWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/RequestInformationWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ResourcesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ResultsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ResultWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/SearchResultsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/SelectionGridWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/SFDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ShowAccountPermissionsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ShowIndividualPermissionsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ShowInheritedPermissionsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/ShowPermissionsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/SourceFilesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/stub.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/SummaryStatsQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/SystemServersQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TableContainer.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TargetClustersQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TargetComboBoxWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TargetSelectClass.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TargetSelectWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TimingPageWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TimingTreeMapWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TopologyDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TopologyWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TpClusterInfoWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/TpThorStatusWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/UserDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/UserQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/VariablesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/VizWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/WorkflowsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/WUDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/WUQueryWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/XrefDetailsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/XrefDirectoriesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/XrefErrorsWarningsWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/XrefFoundFilesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/XrefLostFilesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/XrefOrphanFilesWidget.js
    ${CMAKE_CURRENT_BINARY_DIR}/eclwatch/XrefQueryWidget.js

    # src files
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/AttributeDesc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/Cardinality.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/config.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/DGridHeaderHook.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/DPWorkunit.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/NAWidget.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/PopularPatterns.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/Report.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatterns/StatChart.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/bs/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/es/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/fr/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/hr/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/hu/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/pt-br/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/sr/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nls/zh/hpcc.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/react/hooks/useWsStore.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/react/index.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/react/render.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/react/wuStatus.tsx
    ${CMAKE_CURRENT_BINARY_DIR}/src/store/Deferred.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/store/Memory.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/store/Paged.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/store/Store.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/store/Tree.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/store/util/SimpleQueryEngine.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/UserPreferences/EnvironmentTheme.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/UserPreferences/Recent.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/Clippy.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DataPatternsWidget.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/DiskUsage.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/dojoLib.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ECLArchiveWidget.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPActivity.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPBase.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPDFUWorkunit.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPLog.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPLogicalFile.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPPackageProcess.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPPreflight.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPQuery.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPQueue.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPRequest.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPResource.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPResult.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPSearch.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPTopology.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPTree.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPUtil.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ESPWorkunit.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/FileSpray.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/Graph7Widget.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/GraphStore.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/GraphTree7Widget.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/GraphTreeWidget.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/KeyValStore.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/nlsHPCC.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/Pagination.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/Session.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/Timings.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/Utility.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ws_access.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ws_account.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsDfu.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsDFUXref.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsEcl.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ws_elk.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsESDLConfig.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/ws_machine.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsPackageMaps.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsSMC.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsTopology.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WsWorkunits.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WUGraphLegend.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WUScopeController.ts
    ${CMAKE_CURRENT_BINARY_DIR}/src/WUStatus.ts

    # src-react files
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/comms/workunit.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/controls/TabbedPanes/TabInfo.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/activity.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/cloud.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/deepHooks.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/duckdb.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/favorite.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/file.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/index.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/logging.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/metrics.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/platform.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/query.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/resources.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/store.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/theme.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/user.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/util.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/workunit.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/index.css
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/index.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/layouts/pivot.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/layouts/react-reflex.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/routes.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/themes.css
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/themes.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/util/hashUrl.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/util/history.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/util/metricArchive.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/util/metricGraph.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/util/throttle.ts

    # Key src-react tsx components
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/Workunits.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/DFUWorkunits.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/Files.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/Activities.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/Queries.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/QueryDetails.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/WorkunitDetails.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/DFUWorkunitDetails.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/FileDetails.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/ECLPlayground.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/Security.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/UserDetails.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/GroupDetails.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/Monitoring.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/Xrefs.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/components/PackageMaps.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/banner.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/confirm.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/hooks/grid.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/layouts/DojoAdapter.tsx
    ${CMAKE_CURRENT_SOURCE_DIR}/src-react/layouts/HpccJSAdapter.tsx

    # Root configuration files
    ${CMAKE_CURRENT_SOURCE_DIR}/eslint.config.mjs
    ${CMAKE_CURRENT_SOURCE_DIR}/GetUserName.html
    ${CMAKE_CURRENT_SOURCE_DIR}/index.html
    ${CMAKE_CURRENT_SOURCE_DIR}/Login.html
    ${CMAKE_CURRENT_SOURCE_DIR}/lws.config.js
    ${CMAKE_CURRENT_SOURCE_DIR}/package-lock.json
    ${CMAKE_CURRENT_SOURCE_DIR}/package.json
    ${CMAKE_CURRENT_SOURCE_DIR}/stub.htm
    ${CMAKE_CURRENT_SOURCE_DIR}/tsconfig.json
    ${CMAKE_CURRENT_SOURCE_DIR}/webpack.config.js
)

if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Debug" )
    set ( ECLWATCH_BUILD "build-dev" )
else ()
    set ( ECLWATCH_BUILD "build" )
endif ()

set (NPM_ERROR "")
if (WIN32)
    execute_process(COMMAND cmd /c "npm --version" OUTPUT_VARIABLE _VERSION RESULT_VARIABLE _NPM_VERSION_RESULT)
else ()
    execute_process(COMMAND npm --version OUTPUT_VARIABLE _VERSION RESULT_VARIABLE _NPM_VERSION_RESULT)
endif ()
if (NOT _NPM_VERSION_RESULT)
    string (REPLACE "v" "" NPM_VERSION_STRING "${_VERSION}")
    string (REPLACE "." ";" _VERSION_LIST "${NPM_VERSION_STRING}")
    list (GET _VERSION_LIST 0 NPM_VERSION_MAJOR)
    if (NPM_VERSION_MAJOR LESS 8)
        set (NPM_ERROR "NPM version ${NPM_VERSION_MAJOR} is too old (expected >= 8), please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites" )
    endif ()
else ()
    set (NPM_ERROR "Unable to locate node/npm, please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites" )
endif ()

if ( NOT "${NPM_ERROR}" STREQUAL "")
    message ( FATAL_ERROR "${NPM_ERROR}" )
endif ()

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/no.file
    COMMAND npm run --silent clean
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target( eclwatch_clean
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/no.file
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
    COMMAND npm ci --silent && ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/package-lock.json ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/package-lock.json
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target( eclwatch_fetchdeps
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/build/dist/stub.eclwatch.js
    COMMAND npm run --silent ${ECLWATCH_BUILD}
    DEPENDS ${SRCS}
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target ( eclwatch ALL 
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/build/dist/stub.eclwatch.js
)

add_dependencies ( eclwatch eclwatch_fetchdeps )

install ( DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/." DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS )
