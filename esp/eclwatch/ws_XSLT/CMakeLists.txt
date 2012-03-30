################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    This program is free software: you can redistribute it and/or All rights
#    reserved. This program is NOT PRESENTLY free software: you can NOT
#    redistribute
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/exceptions.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/BoolResponse.xslt
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/xslt COMPONENT Runtime )
ENDFOREACH ( iFILES )

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/BoolResponse.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/GvcGraph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/RoxieGVCGraph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/RoxieGraph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/action.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/addto_superfile.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/atts.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/batchjobdispatch.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/batchworkunits.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/batchwuid.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/batchwuid_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/bdefault.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster_info.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/date-time.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/def_def_file.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/default.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_file.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_file_space.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_filelist.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_fileview.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_getdatacolumns.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_progress.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_searchdata.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_superedit.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_superresult.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_viewdata.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_workunits.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfu_wuid.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfusearchresult.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuwu_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuwuaction_results.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dim_graph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/exceptions.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/exceptions_svg.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/fs_desprayCopyForm.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/fs_sprayForm.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/fs_renameForm.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/graph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/graphStats.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/graph_gvc.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/graph_gvc_common.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/graphupdate_gvc.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/html.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/index.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/indexdisplayinfo.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/jobs_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/lib.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/listroxiequery.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/machines.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/oldgraph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/queryfiledetails.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/queryfilelist.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/queryfilelistdone.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/result.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/result_lib.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/result_lib1.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/result_lib2.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/resultxls.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxieclusters.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiefile_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxieindexes.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxieoriginalfiles.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery_stat_detail.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery_stat_exception.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery_stat_exception_detail.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery_stat_query.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery_stat_report.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequery_stat_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequerydetails.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequerygraph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiequerygvcgraph.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/scheduledwus.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/services.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/clusterprocesses.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/targetclusters.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/showresult.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/table.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/thor_status.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/topology.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/tplog.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/tplogdisplay.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/workunits.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wuaction_results.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wuid.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wuid_jobs.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wuid_jobs_sasha.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wuid_search.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wuidcommon.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/wuiddetails.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xml_def_file.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xref_build.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xref_directories.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xref_errwarn.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xref_found.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xref_lost.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xref_main.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/xref_orphan.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/opensave.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/filerelationsearch.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/filerelationlist.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/WUQuerysets.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/WUQuerysetQueries.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dropzonefile.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/hpccresourcelist.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/dropzonefilelist.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/WUCopyLogicalFiles.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/WUDeployWorkunit.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_accountpermissions.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_adduser.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_basedns.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_filepermission.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_groupadd.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_groupdelete.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_groupedit.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_groupmemberedit.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_groupmembereditinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_groups.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_permissionaddinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_permissionchange.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_permissionresetinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_permissions.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_permissionsreset.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_posix.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_posixinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_resetpass.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_resetpassinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_resourceadd.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_resourceaddinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_resourcedelete.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_resources.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_sudoers.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_sudoersinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_useraction.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_useredit.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_usergroupedit.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_usergroupeditinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_userinfoedit.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_userinfoeditinput.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/access_users.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/account.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/account_input.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/account_whoami.xslt
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/smc_xslt COMPONENT Runtime )
ENDFOREACH ( iFILES )

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/atts.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/result_lib.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/lib.xslt
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/wsecl/wsecl COMPONENT Runtime )
ENDFOREACH ( iFILES )
