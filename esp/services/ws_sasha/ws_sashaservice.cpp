/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems.

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

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_sashaservice.hpp"
#include "jlib.hpp"
#include "TpWrapper.hpp"
#include "saruncmd.hpp"

constexpr const char* FEATURE_URL = "SashaAccess";

INode* CWSSashaEx::createSashaNode(const char* archiverType)
{
    SocketEndpoint ep;
    getSashaServiceEP(ep, archiverType, true);
    return createINode(ep);
}

void CWSSashaEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
}

bool CWSSashaEx::onGetVersion(IEspContext& context, IEspGetVersionRequest& req, IEspResultResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.GetVersion: Permission denied.");

        StringBuffer results;
        Owned<INode> node = createSashaNode(wuArchiverType);
        runSashaCommand(SCA_GETVERSION, node, results);
        resp.setResult(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}
