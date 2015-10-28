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
#include "jlib.hpp"
#include "DumpHelper.ipp"

DumpHelper::DumpHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

DumpHelper::~DumpHelper()
{
}

bool DumpHelper::doit(FILE * fp)
{
    SCMStringBuffer xml;

    // If we were given a workunit dump that one workunit, otherwise moan
    if (globals->hasProp("WUID"))
    {
        const char* wuid = globals->queryProp("WUID");
        const char *whichPath = globals->queryProp("prop");
        if (whichPath)
        {
            if (stricmp(whichPath, "ecl")==0)
            {                   
                Owned<IClientWUInfoRequest> inforeq = wuclient->createWUInfoRequest();
                inforeq->setWuid(wuid);
                Owned<IClientWUInfoResponse> inforesp = wuclient->WUInfo(inforeq);
                if(!inforesp)
                {
                    printf("Workunit %s not found\n", wuid);
                    return false;
                }

                IConstECLWorkunit* wu = &inforesp->getWorkunit();
                IConstECLQuery* query = &wu->getQuery();
                if(query)
                    xml.set(query->getText());
            }
            else
            {
                printf("Unrecognized parameter prop=%s", whichPath);
                return false;
            }
        }
        else
        {
            Owned<IClientWULogFileRequest> req = wuclient->createWUFileRequest();
            req->setWuid(wuid);
            req->setType("XML");
            Owned<IClientWULogFileResponse> resp = wuclient->WUFile(req);
            if(!resp)
            {
                printf("Workunit %s not found\n", wuid);
                return false;
            }
            const IMultiException* excep = &resp->getExceptions();
            if(excep != NULL && excep->ordinality())
            {
                unsigned i = 0;
                while (i < excep->ordinality())
                {
                    StringBuffer msg;
                    excep->item(i).errorMessage(msg);
                    unsigned code = excep->item(i).errorCode();
                    printf("<Error><code>%d</code><message>%s</message></Error>\n", code, msg.str());
                }
                return false;
            }

            const MemoryBuffer & xmlmem = resp->getThefile();
            StringBuffer xmlbuf;
            xmlbuf.append(xmlmem.length(), xmlmem.toByteArray());
            xml.set(xmlbuf.str());
        }
        // Print the results
        if (fp != NULL)
        {
            fprintf(fp, "%s", xml.str());
        }
        return true;
    }

    return false;
}


GraphHelper::GraphHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

GraphHelper::~GraphHelper()
{
}

bool GraphHelper::doit(FILE * fp)
{
    if (globals->hasProp("WUID"))
    {
        const char* wuid = globals->queryProp("WUID");
        Owned<IClientWUProcessGraphRequest> req = wuclient->createWUProcessGraphRequest();
        StringBuffer fname(wuid);
        req->setWuid(fname.str());

        StringBuffer graph;
        if(globals->hasProp("graph"))
            graph.append(globals->queryProp("graph"));

        req->setName(graph.str());
        Owned<IClientWUProcessGraphResponse> resp = wuclient->WUProcessGraph(req);
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer msg;
            excep->errorMessage(msg);
            printf("%s\n", msg.str());
            return false;
        }

            StringBuffer graphbuf;
            graphbuf.append(resp->getTheGraph());
            fprintf(fp, "%s", graphbuf.str());
            return true;
        
        return false;
    }
    else 
    {
        printf("Please specify the WUID\n");
        return false;
    }
}


