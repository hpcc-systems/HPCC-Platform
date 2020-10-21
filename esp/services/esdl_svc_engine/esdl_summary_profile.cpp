/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "esdl_summary_profile.hpp"
#include "espcontext.hpp"



bool CTxSummaryProfileEsdl::tailorSummary(IEspContext* ctx)
{
    if(!ctx)
        return false;

    CTxSummary* txsummary = ctx->queryTxSummary();

    if(!txsummary)
        return false;

    // setup name mappings
    configure();

    // root
    txsummary->set("sys", "esp", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    SCMStringBuffer utcTime;
    Owned<IJlibDateTime> now = createDateTimeNow();
    now->getGmtString(utcTime);
    txsummary->set("creation_timestamp", utcTime.str(), LogMin, TXSUMMARY_GRP_ENTERPRISE);

    txsummary->set("log_type", "INFO", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("event_code", "SUMMARY", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("format_ver", "1.2", LogMin, TXSUMMARY_GRP_ENTERPRISE);

    // id
    txsummary->set("id.global", ctx->getGlobalId(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("id.caller", ctx->getCallerId(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("id.local", ctx->getLocalId(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("id.ref", ctx->queryTransactionID(), LogMin, TXSUMMARY_GRP_ENTERPRISE);

    // app
    txsummary->set("app.resp_time_ms", ctx->queryProcessingTime(), LogMin, TXSUMMARY_GRP_ENTERPRISE);

    // caller
    // entry formerly named 'user' now broken into separate authid and ip fields
    if (ctx->queryUserId())
    {
        txsummary->set("caller.authid", ctx->queryUserId(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }
    StringBuffer peer;
    ctx->getPeer(peer);
    if (peer.length())
    {
        txsummary->set("caller.ip", peer.str(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }

    // local
    StringBuffer srvAddress;
    short port;
    ctx->getServAddress(srvAddress, port);
    if (srvAddress.length())
    {
        txsummary->set("local.ip", srvAddress.str(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }

    // custom_fields
    txsummary->set("custom_fields.port", port, LogMin, TXSUMMARY_GRP_ENTERPRISE);

    StringBuffer sysinfo;
    getSystemTraceInfo(sysinfo);
    unsigned int cpu = parseSystemInfo("PU=", sysinfo);
    unsigned int mal = parseSystemInfo("MAL=", sysinfo);
    unsigned int mu = parseSystemInfo("MU=", sysinfo);

    txsummary->set("custom_fields.cpusage_pct", cpu, LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("custom_fields.musage_pct", mu, LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("custom_fields.mal", mal, LogMin, TXSUMMARY_GRP_ENTERPRISE);

    // Update once retry is implemented
    txsummary->set("custom_fields.qRetryStatus", 0, LogMin, TXSUMMARY_GRP_ENTERPRISE);
    short isInternal = 0;
    ISecUser* secUser = ctx->queryUser();
    if (secUser)
    {
        if (secUser->getStatus()==SecUserStatus_Inhouse)
            isInternal = 1;
        txsummary->set("custom_fields.companyid", secUser->getRealm(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }
    txsummary->set("custom_fields.internal", isInternal, LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("custom_fields.wsdlver", ctx->getClientVersion(), LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->set("custom_fields.esp_build", getBuildVersion(), LogMin, TXSUMMARY_GRP_ENTERPRISE);


    // Review the different possible entries recording failure
    // and set the final status_code and status entries accordingly
    if (streq(ctx->queryAuthStatus(), AUTH_STATUS_FAIL))
    {
        txsummary->set("app.status_code", "Failed to authenticate user", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        txsummary->set("msg", "Failed to authenticate user", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }
    else if (txsummary->contains("soapFaultCode") || txsummary->contains("custom_fields.msg"))
    {
        txsummary->setCopyValueOf("app.status_code", "msg", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        txsummary->set("app.status", "FAILED", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }
    else if (txsummary->contains("custom_fields._soap_call_error_msg"))
    {
        txsummary->setCopyValueOf("app.status_code", "custom_fields._soap_call_error_msg", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        txsummary->set("app.status", "FAILED", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }
    else
    {
        txsummary->set("app.status_code", "S", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        txsummary->set("app.status", "SUCCESS", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }

    // Use the _soap_call_error_msg as our final message if it exists
    // Otherwise, if no other message has been added, then add one for success
    if (txsummary->contains("custom_fields._soap_call_error_msg"))
    {
        txsummary->setCopyValueOf("msg", "custom_fields._soap_call_error_msg", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }
    if (!txsummary->contains("msg"))
    {
        txsummary->set("msg", "Success", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    }
    // msg is set appropriately, copy it's value to custom_fields.msg
    txsummary->setCopyValueOf("custom_fields.msg", "msg", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->setCopyValueOf("custom_fields.httpmethod", "app.protocol", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->setCopyValueOf("custom_fields.method", "method", LogMin, TXSUMMARY_GRP_ENTERPRISE);
    txsummary->setCopyValueOf("custom_fields.txid", "id.ref", LogMin, TXSUMMARY_GRP_ENTERPRISE);

    return true;
}

unsigned int CTxSummaryProfileEsdl::parseSystemInfo(const char* name, StringBuffer& sysinfo)
{
    const char* finger = sysinfo.str();

    while(finger && *finger)
    {
        if(hasPrefix(finger, name, true))
        {
            // found our info field, skip over the name
            finger+=strlen(name);

            // skip any whitespace
            while(finger && ' ' == *finger)
                finger++;

            return strtoul(finger, NULL, 10);
        }
        finger++;
    }
    return 0;
}

void CTxSummaryProfileEsdl::configure()
{
    unsigned int ALL_GROUPS = TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE;

    addMap("activeReqs", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.activerecs", true});
    addMap("auth", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.auth_status", true});
    addMap("contLen", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.contLen", true});
    addMap("endcall", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endCallOut", true});
    addMap("end-HFReq", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endHandleFinalReq", true});
    addMap("end-procres", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endEsdlRespBld", true});
    addMap("end-reqproc", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endEsdlReqBld", true});
    addMap("end-resLogging", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endSendLogInfo", true});

    addMap("method", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "app.method", true});
    addMap("rcv", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.rcvdReq", true});
    addMap("reqRecvd", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.rcvdReq", true});
    addMap("respSent", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.rspSndEnd", true});
    addMap("srt-procres", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.startEsdlRespBld", true});
    addMap("srt-reqproc", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.startEsdlReqBld", true});
    addMap("srt-resLogging", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.startSendLogInfo", true});

    addMap("startcall", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.startCallOut", true});
    addMap("total", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.comp", true});

    // For plugins

    addMap("dbAuthenticate", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.dbAuthenticate", true});
    addMap("dbGetEffectiveAccess", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.dbGetEffectiveAccess", true});
    addMap("dbGetSettingAccess", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.dbGetSettingAccess", true});
    addMap("dbValidateSettings", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.dbValidateSettings", true});

    addMap("endLnaaAuthSendRequest", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endLnaaAuthSendRequest", true});
    addMap("endLnaaGetSessionIdSendRequest", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endLnaaGetSessionIdSendRequest", true});
    addMap("endLnaaGetUerDataSendRequest", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.endLnaaGetUerDataSendRequest", true});

    addMap("lnaaWsCallTime", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.lnaaWsCallTime", true});
    addMap("MysqlTime", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.MySQLTime", true});
    addMap("ResourcePool_Mysql", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.ResourcePool_MySql", true});
    addMap("ResourcePool_Sybase", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.ResourcePool_Sybase", true});

    addMap("startLnaaAuthSendRequest", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.startLnaaAuthSendRequest", true});
    addMap("startLnaaGetSessionIdSendRequest", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.startLnaaGetSessionIdSendRequest", true});
    addMap("startLnaaGetUerDataSendRequest", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.startLnaaGetUerDataSendRequest", true});

    addMap("SybaseTime", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.SybaseTime", true});
    addMap("ValidateSourceIP", {ALL_GROUPS, TXSUMMARY_OUT_JSON, "custom_fields.ValidateSourceIP", true});
}
