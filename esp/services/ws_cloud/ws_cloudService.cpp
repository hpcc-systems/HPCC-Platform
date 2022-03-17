/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#pragma warning (disable : 4786)

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_cloudService.hpp"
#include "exception_util.hpp"

void CWsCloudEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
    if(cfg == nullptr)
        throw makeStringException(-1, "Can't initialize CWsCloudEx. The cfg is NULL.");

    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/PODInfoCacheSeconds", process, service);
    unsigned podInfoCacheSeconds = cfg->getPropInt(xpath.str(), defaultPODInfoCacheForceBuildSecond);
    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/PODInfoCacheAutoRebuildSeconds", process, service);
    unsigned podInfoCacheAutoRebuildSeconds = cfg->getPropInt(xpath.str(), defaultPODInfoCacheAutoRebuildSecond);
    podInfoCacheReader.setown(new CPODInfoCacheReader("POD Reader", podInfoCacheAutoRebuildSeconds, podInfoCacheSeconds));
}

bool CWsCloudEx::onGetPODs(IEspContext& context, IEspGetPODsRequest& req, IEspGetPODsResponse& resp)
{
    try
    {
        Owned<CPODs> pods = (CPODs*) podInfoCacheReader->getCachedInfo();
        if (pods == nullptr)
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to get POD Info. Please try later.");

        const char* podInfo = pods->queryPODs();
        if (isEmptyString(podInfo))
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Unable to query POD Info. Please try later.");

        resp.setResult(podInfo);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

const char* CPODs::read()
{
    StringBuffer output, error;
    const char* command = "kubectl get pods --output=json";
    unsigned ret = runExternalCommand(output, error, command, nullptr);
    if (!error.isEmpty())
        OWARNLOG("runExternalCommand '%s', error: %s", command, error.str());

    if (!output.isEmpty())
    {
        timeCached.setNow();
        pods.swapWith(output);
    }
    return pods.str();
}
