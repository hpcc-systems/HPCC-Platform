/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#include "jlib.hpp"
#include "DeleteHelper.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/eclplus/DeleteHelper.cpp $ $Id: DeleteHelper.cpp 62376 2011-02-04 21:59:58Z sort $");

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
        if(!inforesp || &inforesp->getWorkunit() == NULL)
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
