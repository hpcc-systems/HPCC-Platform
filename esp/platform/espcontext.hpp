/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _ESPCONTEXT_HPP__
#define _ESPCONTEXT_HPP__

/*#ifndef ESP_BUILTIN
#ifdef _WIN32
#define ESP_CONTEXT_API DECL_EXPORT
#endif
#endif

#ifndef ESP_CONTEXT_API
#define ESP_CONTEXT_API
#endif*/

#include "esp.hpp"
#include "esphttp.hpp"

#define SESSION_SDS_LOCK_TIMEOUT (30*1000) // 30 seconds
#define ESP_SESSION_TIMEOUT (120) // 120 Mins
#define ESP_SESSION_NEVER_TIMEOUT   -1
#define ESP_CHECK_SESSION_TIMEOUT (30) // 30 seconds

static const char* const USER_NAME_COOKIE = "ESPUserName";
static const char* const SESSION_ID_COOKIE = "ESPSessionID";
static const char* const SESSION_START_URL_COOKIE = "ESPAuthURL";
static const char* const SESSION_TIMEOUT_COOKIE = "ESPSessionTimeoutSeconds";
static const char* const SESSION_ID_TEMP_COOKIE = "ESPAuthIDTemp";
static const char* const SESSION_AUTH_OK_COOKIE = "ESPAuthenticated";
static const char* const SESSION_AUTH_MSG_COOKIE = "ESPAuthenticationMSG";
static const char* const USER_ACCT_ERROR_COOKIE = "ESPUserAcctError";
static const char* const DEFAULT_LOGIN_URL = "/esp/files/Login.html";
static const char* const DEFAULT_LOGIN_LOGO_URL = "/esp/files/eclwatch/img/Loginlogo.png";
static const char* const DEFAULT_GET_USER_NAME_URL = "/esp/files/GetUserName.html";
static const char* const ECLWATCH_STUB_REQ = "/esp/files/stub.htm";
static const char* const DEFAULT_UNRESTRICTED_RESOURCES = "/favicon.ico,/esp/files/*,/esp/xslt/*";
static const char* const AUTH_STATUS_NA = "NA";
static const char* const AUTH_STATUS_FAIL = "Fail";
static const char* const AUTH_STATUS_OK = "Ok";
static const char* const AUTH_STATUS_NOACCESS = "NoAccess"; //failed for feature level authorization
static const char* const AUTH_TYPE_NONE = "None";
static const char* const AUTH_TYPE_USERNAMEONLY = "UserNameOnly";
static const char* const AUTH_TYPE_PERREQUESTONLY = "PerRequestOnly";
static const char* const AUTH_TYPE_PERSESSIONONLY = "PerSessionOnly";
static const char* const AUTH_TYPE_MIXED = "Mixed";

//xpath in dali
static const char* const PathSessionRoot="Sessions";
static const char* const PathSessionProcess="Process";
static const char* const PathSessionApplication="Application";
static const char* const PathSessionSession="Session_";
static const char* const PropSessionID = "@id";
static const char* const PropSessionExternalID = "@externalid";
static const char* const PropSessionUserID = "@userid";
static const char* const PropSessionNetworkAddress = "@netaddr";
static const char* const PropSessionState = "@state";
static const char* const PropSessionCreateTime = "@createtime";
static const char* const PropSessionLastAccessed = "@lastaccessed";
static const char* const PropSessionTimeoutAt = "@timeoutAt";
static const char* const PropSessionTimeoutByAdmin = "@timeoutByAdmin";
static const char* const PropSessionLoginURL = "@loginurl";
/* The following is an example of session data stored in Dali.
<Sessions>
 <Process name="myesp">
   <Application port="8010">
    <Session_3831947145 createtime="1497376914"
             id="3831947145"
             lastaccessed="1497377015"
             timeoutAt="1497477015"
             loginurl="/"
             netaddr="10.176.152.200"
             userid="user1"/>
    <Session_4106750941 createtime="1497377427"
             id="4106750941"
             lastaccessed="1497377427"
             timeoutAt="1497477427"
             loginurl="/"
             netaddr="10.176.152.200"
             userid="user2"/>
   </Application>
   <Application port="8002">
    <Session_3680948651 createtime="1497376989"
             id="3680948651"
             lastaccessed="1497377003"
             timeoutAt="1497477003"
             loginurl="/"
             netaddr="10.176.152.200"
             userid="user1"/>
   </Application>
 </Process>
</Sessions>
 */

interface IEspSecureContext;

esp_http_decl IEspContext* createEspContext(IEspSecureContext* secureContext = nullptr);

// Get URL parameters (include these from Content)
// Return: a=b&c=d format. 
esp_http_decl bool getUrlParams(IProperties *props, StringBuffer& params);

// Only the original URL (not these from Content: URL form encoded)
// Also remove these params that start with dot (.).
// Return: a=b&c=d format. 
esp_http_decl void getEspUrlParams(IEspContext& ctx, StringBuffer& params, const char* excludeParams[]);

esp_http_decl void addEspNativeArray(StringBuffer& schema, const char* xsdType, const char* arrayType);
esp_http_decl void checkRequest(IEspContext& ctx);

esp_http_decl LogRequest readLogRequest(char const* req);
esp_http_decl StringBuffer& getLogRequestString(LogRequest req, StringBuffer& out);
esp_http_decl LogLevel getEspLogLevel(IEspContext* );
esp_http_decl LogLevel getEspLogLevel();
esp_http_decl LogRequest getEspLogRequests();
esp_http_decl bool getEspLogResponses();
esp_http_decl LogLevel getTxSummaryLevel();
esp_http_decl bool getTxSummaryResourceReq();
esp_http_decl unsigned getSlowProcessingTime();

esp_http_decl void ESPLOG(IEspContext* ctx, LogLevel level, const char* fmt, ...) __attribute__((format(printf, 3, 4)));
esp_http_decl void ESPLOG(LogLevel level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
esp_http_decl void setEspContainer(IEspContainer* container);

esp_http_decl IEspContainer* getESPContainer();

esp_http_decl void setCFD(const char* cfd);
esp_http_decl const char* getCFD();

esp_http_decl void setBuildVersion(const char* buildVersion);
esp_http_decl const char* getBuildVersion();
esp_http_decl void setBuildLevel(const char* buildLevel);
esp_http_decl const char* getBuildLevel();
esp_http_decl IEspServer* queryEspServer();

#define SDSSESSION_CONNECT_TIMEOUTMS (180*1000)
interface IRemoteConnection;
esp_http_decl IRemoteConnection* getSDSConnectionWithRetry(const char* xpath, unsigned mode, unsigned timeoutMs);
#endif

