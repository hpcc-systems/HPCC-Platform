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
#define ESP_SESSION_TIMEOUT (600) // 10 Mins
#define ESP_CHECK_SESSION_TIMEOUT (30) // 30 seconds

static const char* const SESSION_ID_COOKIE = "ESPSessionID";
static const char* const SESSION_START_URL_COOKIE = "ESPAuthURL";
static const char* const SESSION_ID_TEMP_COOKIE = "ESPAuthIDTemp";
static const char* const DEFAULT_LOGIN_URL = "/esp/files/eclwatch/templates/Login.html";
static const char* const DEFAULT_UNRESTRICTED_RESOURCES = "/favicon.ico,/esp/files/*,/esp/xslt/*";

//xpath in dali
static const char* const PathSessionRoot="Sessions";
static const char* const PathSessionProcess="Process";
static const char* const PathSessionApplication="Application";
static const char* const PathSessionSession="Session";
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
    <Session createtime="1497376914"
             id="3831947145"
             lastaccessed="1497377015"
             loginurl="/"
             netaddr="10.176.152.200"
             state="1"
             userid="TheAdmin"/>
    <Session createtime="1497377427"
             id="4106750941"
             lastaccessed="1497377427"
             loginurl="/"
             netaddr="10.176.152.200"
             state="0"/>
   </Application>
   <Application port="8002">
    <Session createtime="1497376989"
             id="3680948651"
             lastaccessed="1497377003"
             loginurl="/"
             netaddr="10.176.152.200"
             state="1"
             userid="TheAdmin"/>
   </Application>
 </Process>
</Sessions>
 */

interface IEspSecureContext;

ESPHTTP_API IEspContext* createEspContext(IEspSecureContext* secureContext = NULL);

// Get URL parameters (include these from Content)
// Return: a=b&c=d format. 
ESPHTTP_API bool getUrlParams(IProperties *props, StringBuffer& params);

// Only the original URL (not these from Content: URL form encoded)
// Also remove these params that start with dot (.).
// Return: a=b&c=d format. 
ESPHTTP_API void getEspUrlParams(IEspContext& ctx, StringBuffer& params, const char* excludeParams[]);

ESPHTTP_API void addEspNativeArray(StringBuffer& schema, const char* xsdType, const char* arrayType);
ESPHTTP_API void checkRequest(IEspContext& ctx);

ESPHTTP_API LogLevel getEspLogLevel(IEspContext* );
ESPHTTP_API LogLevel getEspLogLevel();
ESPHTTP_API bool getEspLogRequests();
ESPHTTP_API bool getEspLogResponses();
ESPHTTP_API LogLevel getTxSummaryLevel();
ESPHTTP_API bool getTxSummaryResourceReq();
ESPHTTP_API unsigned getSlowProcessingTime();

ESPHTTP_API void ESPLOG(IEspContext* ctx, LogLevel level, const char* fmt, ...) __attribute__((format(printf, 3, 4)));
ESPHTTP_API void ESPLOG(LogLevel level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
ESPHTTP_API void setEspContainer(IEspContainer* container);

ESPHTTP_API IEspContainer* getESPContainer();

ESPHTTP_API void setCFD(const char* cfd);
ESPHTTP_API const char* getCFD();

ESPHTTP_API void setBuildVersion(const char* buildVersion);
ESPHTTP_API const char* getBuildVersion();
ESPHTTP_API void setBuildLevel(const char* buildLevel);
ESPHTTP_API const char* getBuildLevel();
#endif

