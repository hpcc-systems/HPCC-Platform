################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
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
add_executable ( esp_compile_scm ${SRCS} )
install ( TARGETS esp_compile_scm DESTINATION ${OSSDIR}/bin )


