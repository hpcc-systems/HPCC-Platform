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


# Component: esp_compile_scm 

#####################################################
# Description:
# ------------
#    Cmake Input File for esp_compile_scm
#####################################################


project( esp_compile_scm ) 

set (    SRCS 
         ../../scm/wsm_accident.ecm 
         ../../scm/wsm_addresscount.ecm 
         ../../scm/wsm_addresshistory.ecm 
         ../../scm/wsm_addressprefill.ecm 
         ../../scm/wsm_addressreport.ecm 
         ../../scm/wsm_alsofoundindicators.ecm 
         ../../scm/wsm_alsofoundreport.ecm 
         ../../scm/wsm_assetreport.ecm 
         ../../scm/wsm_bankruptcy.ecm 
         ../../scm/wsm_bizcredit.ecm 
         ../../scm/wsm_bizreport.ecm 
         ../../scm/wsm_bpsreport.ecm 
         ../../scm/wsm_bpssearch.ecm 
         ../../scm/wsm_business.ecm 
         ../../scm/wsm_businessinview.ecm 
         ../../scm/wsm_businessprofile.ecm 
         ../../scm/wsm_caltaxpermitholder.ecm 
         ../../scm/wsm_canadaphone.ecm 
         ../../scm/wsm_carrierdiscovery.ecm 
         ../../scm/wsm_changeofaddress.ecm 
         ../../scm/wsm_civilcourt.ecm 
         ../../scm/wsm_compreport.ecm 
         ../../scm/wsm_concealedweapon.ecm 
         ../../scm/wsm_conscredit_req.ecm 
         ../../scm/wsm_conscredit_resp.ecm 
         ../../scm/wsm_contactcardreport.ecm 
         ../../scm/wsm_controlledsubstance.ecm 
         ../../scm/wsm_corporate.ecm 
         ../../scm/wsm_cp_internal.ecm 
         ../../scm/wsm_criminal.ecm 
         ../../scm/wsm_deacontrolledsubstance.ecm 
         ../../scm/wsm_deadcompany.ecm 
         ../../scm/wsm_death.ecm 
         ../../scm/wsm_directoryassist.ecm 
         ../../scm/wsm_directoryassistreverse.ecm 
         ../../scm/wsm_dirassistwireless.ecm 
         ../../scm/wsm_driverlicense.ecm 
         ../../scm/wsm_driverlicense2.ecm 
         ../../scm/wsm_dunandbradstreet.ecm 
         ../../scm/wsm_eAuth.ecm 
         ../../scm/wsm_echotest.ecm 
         ../../scm/wsm_emailsearch.ecm 
         ../../scm/wsm_enhancedbizreport.ecm 
         ../../scm/wsm_enhancedbizsearch.ecm 
         ../../scm/wsm_enhancedperson.ecm 
         ../../scm/wsm_faaaircraft.ecm 
         ../../scm/wsm_faapilot.ecm 
         ../../scm/wsm_factsquery.ecm 
         ../../scm/wsm_fcclicense.ecm 
         ../../scm/wsm_fein.ecm 
         ../../scm/wsm_fictitiousbusinesssearch.ecm 
         ../../scm/wsm_firearm.ecm 
         ../../scm/wsm_focuscorp.ecm 
         ../../scm/wsm_foreclosure.ecm 
         ../../scm/wsm_gateway.ecm 
         ../../scm/wsm_gateway_apriss_req.ecm 
         ../../scm/wsm_gateway_apriss_resp.ecm 
         ../../scm/wsm_gateway_avm_req.ecm 
         ../../scm/wsm_gateway_avm_resp.ecm 
         ../../scm/wsm_gateway_avm_resp2.ecm 
         ../../scm/wsm_gateway_decorp.ecm 
         ../../scm/wsm_gateway_equifax.ecm 
         ../../scm/wsm_gateway_GBGroup.ecm 
         ../../scm/wsm_gateway_realtimeperson.ecm 
         ../../scm/wsm_gateway_news.ecm 
         ../../scm/wsm_gateway_polk.ecm 
         ../../scm/wsm_gateway_verid.ecm 
         ../../scm/wsm_getorderresults.ecm 
         ../../scm/wsm_globalwatchlist.ecm 
         ../../scm/wsm_healthcare.ecm 
         ../../scm/wsm_huntingfishing.ecm 
         ../../scm/wsm_idverify.ecm 
         ../../scm/wsm_instantid.ecm 
         ../../scm/wsm_internetdomain.ecm 
         ../../scm/wsm_lienjudgement.ecm 
         ../../scm/wsm_marriagedivorce.ecm 
         ../../scm/wsm_matrix.ecm 
         ../../scm/wsm_merchantvessel.ecm 
         ../../scm/wsm_militaryrecord.ecm 
         ../../scm/wsm_motorvehicle.ecm 
         ../../scm/wsm_moxiefind.ecm 
         ../../scm/wsm_nationalsanction.ecm 
         ../../scm/wsm_officialrecord.ecm 
         ../../scm/wsm_osha.ecm 
         ../../scm/wsm_patriotsearch.ecm 
         ../../scm/wsm_peopleatwork.ecm 
         ../../scm/wsm_peoplereport.ecm 
         ../../scm/wsm_phonesfeedback.ecm 
         ../../scm/wsm_proflicense.ecm 
         ../../scm/wsm_propassess.ecm 
         ../../scm/wsm_propdeed.ecm 
         ../../scm/wsm_property.ecm 
         ../../scm/wsm_propertyavm.ecm 
         ../../scm/wsm_propertychronology.ecm 
         ../../scm/wsm_rateevasion.ecm 
         ../../scm/wsm_rightaddress.ecm 
         ../../scm/wsm_riskview.ecm 
         ../../scm/wsm_rollupbpssearch.ecm 
         ../../scm/wsm_rollupbizreport.ecm 
         ../../scm/wsm_rolluppersonsearch.ecm 
         ../../scm/wsm_rnareport.ecm 
         ../../scm/wsm_securint.ecm 
         ../../scm/wsm_sexualoffender.ecm 
         ../../scm/wsm_sexualoffenderimage.ecm 
         ../../scm/wsm_share.ecm 
         ../../scm/wsm_sourcedoc.ecm 
         ../../scm/wsm_statewidecounts.ecm 
         ../../scm/wsm_statewidefab.ecm 
         ../../scm/wsm_statewidefap.ecm 
         ../../scm/wsm_submitorderactivity.ecm 
         ../../scm/wsm_submitorderitem.ecm 
         ../../scm/wsm_summaryreport.ecm 
         ../../scm/wsm_taxpayerinfo.ecm 
         ../../scm/wsm_taxprofessional.ecm 
         ../../scm/wsm_thindex.ecm 
         ../../scm/wsm_tsunami.ecm 
         ../../scm/wsm_ucc.ecm 
         ../../scm/wsm_usbusinessdirectory.ecm 
         ../../scm/wsm_voter.ecm 
         ../../scm/wsm_watercraft.ecm 
    )

# OBJS = 
HPCC_ADD_EXECUTABLE ( esp_compile_scm ${SRCS} )
install ( TARGETS esp_compile_scm RUNTIME DESTINATION ${EXEC_DIR} )


