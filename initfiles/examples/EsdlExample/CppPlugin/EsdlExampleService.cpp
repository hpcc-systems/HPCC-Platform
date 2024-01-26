/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems.

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

#include "EsdlExampleService.hpp"
#include "jliball.hpp"
#include "jlog.hpp"
#include "jptree.hpp"

CppEchoPersonInfoResponse* EsdlExampleService::CppEchoPersonInfo(EsdlContext* context, CppEchoPersonInfoRequest* request)
{
    Owned<CppEchoPersonInfoResponse> resp = new CppEchoPersonInfoResponse();
    resp->m_count.setown(new Integer(0));
    if (request->m_Name)
    {
        resp->m_count.setown(new Integer(1));
        resp->m_Name.set(request->m_Name.get());
    }
    appendArray(resp->m_Addresses, request->m_Addresses);
    return resp.getClear();
}

EsdlExamplePingResponse* EsdlExampleService::Ping(EsdlContext* context, EsdlExamplePingRequest* request)
{
    Owned<EsdlExamplePingResponse> resp = new EsdlExamplePingResponse();
    //Fill in logic
    return resp.getClear();
}

extern "C" EsdlExampleServiceBase* createEsdlExampleServiceObj()
{
    return new EsdlExampleService();
}
