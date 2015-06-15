/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jlib.hpp"
#include "DeleteHelper.ipp"

DeleteHelper::DeleteHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

DeleteHelper::~DeleteHelper()
{
}

bool DeleteHelper::doit(FILE * fp)
{
    const char* wuid = globals->queryProp("WUID");
    StringArray wuids;
    if(wuid && *wuid)
    {
        wuids.append(wuid);
    }
    else
    {
        const char* owner = globals->queryProp("OWNER");
        if(owner == NULL || *owner=='\0')
            return false;

        Owned<IClientWUQueryRequest> req = wuclient->createWUQueryRequest();
        req->setOwner(owner);

        Owned<IClientWUQueryResponse> resp = wuclient->WUQuery(req);
        if(!resp)
            return false;
        IArrayOf<IConstECLWorkunit>& wus = resp->getWorkunits();
        ForEachItemIn(idx, wus)
        {
            IConstECLWorkunit* wu = &wus.item(idx);
            if(wu == NULL || wu->getWuid() == NULL)
                continue;
            switch (wu->getStateID())
            {
            case WUStateAborted:
            case WUStateCompleted:
            case WUStateFailed:
                wuids.append(wu->getWuid());
                break;
            }
        }
    }

    Owned<IClientWUDeleteRequest> req = wuclient->createWUDeleteRequest();
    req->setWuids(wuids);
    
    Owned<IClientWUDeleteResponse> resp = wuclient->WUDelete(req);
    if(!resp)
        return false;
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL && excep->ordinality() > 0)
    {
        StringBuffer msg;
        excep->errorMessage(msg);
        printf("%s\n", msg.str());
        return false;
    }

    return true;
}

AbortHelper::AbortHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

bool AbortHelper::doit(FILE * fp)
{
    const char* wuid = globals->queryProp("WUID");
    StringArray wuids;
    if(wuid != NULL && *wuid != '\0')
    {
        Owned<IClientWUInfoRequest> inforeq = wuclient->createWUInfoRequest();
        inforeq->setWuid(wuid);
        Owned<IClientWUInfoResponse> inforesp = wuclient->WUInfo(inforeq);
        if(!inforesp)
        {
            printf("Workunit %s not found\n", wuid);
            return false;
        }

        int state = inforesp->getWorkunit().getStateID();
        switch (state)
        {
        case WUStateAborted:
        case WUStateCompleted:
        case WUStateFailed:
        case WUStateAborting:
        case WUStateWait:
            break;
        default:
            wuids.append(wuid);
        }
    }
    else
    {
        const char* owner = globals->queryProp("OWNER");
        if(owner == NULL || *owner=='\0')
            return false;

        Owned<IClientWUQueryRequest> req = wuclient->createWUQueryRequest();
        req->setOwner(owner);

        Owned<IClientWUQueryResponse> resp = wuclient->WUQuery(req);
        if(!resp)
            return false;
        IArrayOf<IConstECLWorkunit>& wus = resp->getWorkunits();
        ForEachItemIn(idx, wus)
        {
            IConstECLWorkunit* wu = &wus.item(idx);
            if(wu == NULL || wu->getWuid() == NULL)
                continue;
            switch (wu->getStateID())
            {
            case WUStateAborted:
            case WUStateCompleted:
            case WUStateFailed:
            case WUStateAborting:
            case WUStateWait:
                break;
            default:
                wuids.append(wu->getWuid());
            }
        }
    }
    
    Owned<IClientWUAbortRequest> req = wuclient->createWUAbortRequest();
    req->setWuids(wuids);
    Owned<IClientWUAbortResponse> resp = wuclient->WUAbort(req);
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL && excep->ordinality() > 0)
    {
        StringBuffer msg;
        excep->errorMessage(msg);
        printf("%s\n", msg.str());
        return false;
    }

    ForEachItemIn(x, wuids)
    {
        printf("Requested abort for %s\n", wuids.item(x));
    }
    
    return true;
}
