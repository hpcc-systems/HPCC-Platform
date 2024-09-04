/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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
#include "espservice.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include <cstring>
#include <iostream>
#include "httpclient.hpp"

using namespace std;

int EspService::sendRequest()
{
    int responseCode = -1;
    StringBuffer req,res,status;
    req.append(reqString);

    Owned<IHttpClientContext> httpctx = getHttpClientContext();
    Owned <IHttpClient> httpclient = httpctx->createHttpClient(NULL, url);

    if(!isEmptyString(username))
    {
        httpclient->setUserID(username);
        if(!isEmptyString(password))
        {
            httpclient->setPassword(password);
        }
    }

    if(streq(reqType, "json"))
    {
       responseCode = httpclient->sendRequest("POST", "application/json", req, res, status, true);
    }
    else if(streq(reqType, "form"))
    {
       responseCode = httpclient->sendRequest("POST", "application/x-www-form-urlencoded", req, res, status, true);
    }
    else if(streq(reqType, "xml"))
    {
       responseCode = httpclient->sendRequest("POST", "text/xml", req, res, status, true);
    }

    if(responseCode != 0 )
    {
        cerr << "Response code " << responseCode << ": Enter a valid esp target" << endl;
        return 1;
    }

    if(streq("401 Unauthorized", status))
    {
        cerr << status << " : Ensure Valid Credentials" << endl;
        return 1;
    }
    if(*status.str() != '2')
    {
        cerr << status << endl;
        return 1;
    }

    if(streq(resType, ".json"))
    {
        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(res);
        StringBuffer jsonRet;
        toJSON(jsonTree, jsonRet);
        cout << jsonRet << endl;
    }
    else if (streq(resType, ".xml")) {
        Owned<IPropertyTree> xmlTree = createPTreeFromXMLString(res);
        StringBuffer xmlRet;
        toXML(xmlTree, xmlRet);
        cout << xmlRet << endl;
    }
    else {
        cerr << "Error Encountered, couldn't parse response type" << endl;
        return 1;
    }
    return 0;


}
EspService::EspService(const char* serviceName, const char* methodName, const char* reqString, const char* resType, const char* reqType,
const char* target, const char* username, const char* password):reqString(reqString), resType(resType), reqType(reqType), username(username), password(password), url(target)
{
}
